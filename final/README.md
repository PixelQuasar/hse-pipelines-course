# Spark + ClickHouse + S3 — финальный проект

Полностью контейнеризованный лейкхаус-пайплайн аналитики пользовательских сессий КонсультантПлюс. Раз в минуту парсер на Scala/Spark разбирает сырые логи из MinIO в типизированные события ClickHouse, Refreshable Materialized Views в CH считают целевые метрики, Grafana визуализирует, GitHub Actions деплоит push'ем в main.

```mermaid
flowchart TD
    USER((User))

    subgraph compute["Compute"]
        GEN["generator (Rust)<br/>~10k sessions/hour<br/>non-uniform arrivals<br/>backdate 0..7d"]
        SCH["scheduler (ofelia)<br/>parse @1m, cold-backfill @24h"]
        SPARK["spark-job (Scala)<br/>ParseJob (incremental)"]
        SCH -.->|docker exec spark-submit| SPARK
    end

    subgraph s3["MinIO (S3)"]
        SES[("sessions/<br/>raw cp1251 txt")]
        CHW[("ch-warehouse/<br/>CH MergeTree parts")]
        PEX[("parquet-exports/<br/>open parquet")]
    end

    subgraph ch["ClickHouse 25.3"]
        BRZ["bronze.events<br/>ReplacingMergeTree"]
        WM["bronze.processed_files<br/>watermark"]
        GOL["gold.*_daily<br/>Refreshable MV<br/>hot window = 10d"]
        EXP["exports.* MV<br/>S3 engine"]
    end

    subgraph ui["UIs (host ports)"]
        GRA["Grafana :8010"]
        PLY["CH Play :8011"]
        MCO["MinIO Console :8012"]
    end

    GEN -->|PUT| SES
    SPARK -->|list| SES
    SPARK -->|anti-join| WM
    SPARK -->|JDBC insert| BRZ
    SPARK -->|JDBC insert| WM
    BRZ -->|FROM .. FINAL<br/>WHERE date >= today()-10| GOL
    GOL --> EXP

    BRZ -.->|disk = S3| CHW
    GOL -.->|disk = S3| CHW
    EXP -->|parquet snapshot| PEX

    GRA -->|HTTP 8123| GOL
    PLY -->|HTTP 8123| BRZ
    USER --> GRA
    USER --> PLY
    USER --> MCO
    MCO -.->|browse| s3
```

## Содержание

1. [Стек и ключевые решения](#стек-и-ключевые-решения)
2. [Схемы таблиц](#схемы-таблиц)
3. [Spark ParseJob](#spark-parsejob)
4. [ClickHouse: hot window + cold backfill](#clickhouse-hot-window--cold-backfill)
5. [Генератор данных](#генератор-данных)
6. [Дашборд Grafana](#дашборд-grafana)
7. [Быстрый старт](#быстрый-старт)
8. [Целевые метрики и проверка](#целевые-метрики-и-проверка)
9. [Известные ограничения](#известные-ограничения)
10. [Остановка / reset](#остановка--reset)

---

## Стек и ключевые решения

| Сервис | Зачем |
|---|---|
| **MinIO (S3)** | Сырые логи + физический disk для CH MergeTree + parquet snapshots |
| **ClickHouse 25.3** | Универсальный storage + query engine. Bronze (raw events), gold (aggregates через Refreshable MV), exports (open parquet) |
| **Spark 3.5 / Scala 2.12** | Только парсер логов в типизированные `RawEvent`. Никакой агрегации — её делает CH MV |
| **Rust generator** | Синтетическая нагрузка для демо «живого» пайплайна |
| **ofelia scheduler** | Cron: `parse @every 1m`, `cold-backfill @every 24h` через `docker exec` |
| **Grafana** | Дашборд с 11 панелями: метрики + распределения + heatmap'ы |

### Почему ClickHouse, а не Iceberg/Nessie

Изначальный план был open-format лейкхаус (Iceberg + Nessie catalog + Trino + AggregateJob). Сменили на ClickHouse-centric архитектуру:

| Аспект | Iceberg-вариант | Наш CH-вариант |
|---|---|---|
| Сервисов в compose | 6 (+Nessie, +Trino) | 4 |
| Время до агрегата | минуты (batch job) | секунды (Refreshable MV) |
| Latency запроса | секунды (Trino) | миллисекунды (CH) |
| Vendor lock | нет | **есть** на bronze/gold |

Компромисс с lock'ом — через `exports.*_parquet` MV: раз в час дублирует bronze/gold в открытый parquet на `s3://parquet-exports/`. Если CH помрёт — данные читаемы любым движком.

### Идемпотентность и инкрементальность

- **ParseJob** — incremental через watermark `bronze.processed_files`. Каждый запуск читает только новые S3-файлы.
- **CH Refreshable MV** — hot window 10 дней. Каждую минуту пересчитывает только последние 10 дней, не всё bronze.
- **Cold partitions** (старше 10 дней) — раз заинициализированы через cold-backfill (CI-deploy step + daily scheduler), дальше остаются «frozen».
- **ReplacingMergeTree** на всех таблицах — дедуп safety net поверх watermark'а.

---

## Схемы таблиц

DDL: `clickhouse/init/01_schemas.sql`. В рантайме — `SHOW CREATE TABLE bronze.events FORMAT TSVRaw`.

### `bronze.events` — типизированные события

Универсальная wide-таблица для всех типов (SESSION_*, QS, CARD_SEARCH, DOC_OPEN, MALFORMED). Дизайн «одна таблица для всех» нужен чтобы сохранить порядок событий внутри сессии и связи (DOC_OPEN ↔ его поиск).

| Поле | Тип | Зачем |
|---|---|---|
| `session_id` | `String` | Имя файла из S3 (`0`-`9999` seed, `synthetic-<uuid>` генератор). **Часть PK** |
| `event_seq` | `Int32` | Порядковый номер внутри сессии, монотонно с 0. **Часть PK**, дедуп при re-runs |
| `event_time` | `Nullable(DateTime)` | Время события из исходной строки лога |
| `event_date` | `Nullable(Date)` | Производное; **partition key**, помесячная пардишн-пруна |
| `event_type` | `LowCardinality(String)` | Discriminator: 6 значений, dict-encoded → 1 байт/строку |
| `search_id` | `Nullable(String)` | Идентификатор поиска, связывает DOC_OPEN с породившим QS/CARD_SEARCH |
| `search_kind` | `LowCardinality(Nullable(String))` | `'QS'` / `'CARD'`. Для DOC_OPEN резолвится через словарь по search_id |
| `query_text` | `Nullable(String)` | Текст запроса (из `{...}` в QS) |
| `card_params_json` | `String DEFAULT '[]'` | JSON-массив `[{param_id, value}]` — Spark JDBC не умеет CH Array, поэтому JSON |
| `result_doc_ids_json` | `String DEFAULT '[]'` | JSON-массив документов из поиска. Источник Metric 1 (`arrayJoin(JSONExtractArrayRaw(...))`) |
| `doc_id` | `Nullable(String)` | Идентификатор открытого документа (DOC_OPEN) |
| `parse_error` | `Nullable(String)` | Текст ошибки для MALFORMED |
| `raw_line` | `Nullable(String)` | Исходная строка для MALFORMED, чтобы можно было руками разобраться |
| `ingested_at` | `DateTime DEFAULT now()` | **Version-column** ReplacingMergeTree. Используется `FROM ... FINAL` для дедупа |

**Engine**: `ReplacingMergeTree(ingested_at)`, **Order**: `(session_id, event_seq)`, **Partition**: `toYYYYMM(event_date)`, **Storage**: `s3_main` (физически parquet-like парты в `s3://ch-warehouse/`).

### `bronze.processed_files` — watermark

| Поле | Тип | Зачем |
|---|---|---|
| `path` | `String` | Полный S3-путь, PK |
| `processed_at` | `DateTime DEFAULT now()` | Когда обработан. Version-column |
| `events_count` | `UInt32 DEFAULT 0` | Сколько событий выпарсено (на будущее, сейчас всегда 0) |

ParseJob делает `SELECT DISTINCT path` для anti-join'а с листингом S3.

### `gold.card_search_doc_hits_daily` — Metric 1

| Поле | Тип |
|---|---|
| `date` | `Date` |
| `doc_id` | `String` |
| `hits` | `UInt64` |
| `computed_at` | `DateTime` |

**Engine**: `ReplacingMergeTree(computed_at)`, **Order**: `(date, doc_id)`, **Partition**: `toYYYYMM(date)`.

Сколько раз каждый документ появился в результатах CARD_SEARCH'а, разрез по дню.

### `gold.qs_doc_opens_daily` — Metric 2

Симметричная таблица для DOC_OPEN, отфильтрованных по `search_kind='QS'`.

### `exports.*_parquet` — open snapshots

Три таблицы на CH `S3` engine (не MergeTree) с partition by `toYYYYMM(date)`. Запись через MV каждый час. Парты лежат в `s3://parquet-exports/` как обычные parquet — читаются любым движком (DuckDB, pandas, Spark).

---

## Spark ParseJob

`spark-jobs/src/main/scala/ru/consultant/lakehouse/jobs/ParseJob.scala`. Запускается ofelia каждую минуту через `docker exec spark-job spark-submit ...`. Один прогон:

1. **Read watermark**: `SELECT DISTINCT path FROM bronze.processed_files` через JDBC, бродкаст set'а путей по executor'ам.
2. **List S3**: `binaryFiles("s3a://sessions/")` — RDD из `(path, bytes)`.
3. **Anti-join**: `.filter { case (p, _) => !processedBC.value.contains(p) }`, кэш RDD.
4. **Parse**: для каждого нового файла — декод cp1251 → `SessionParser.parse(sessionId, content)` → `Seq[RawEvent]`. Парсер чистый Scala, без Spark-зависимостей.
5. **Write bronze**: DataFrame → JDBC append в `bronze.events`. Дубли (если race) дедупит `ReplacingMergeTree(ingested_at)`.
6. **Advance watermark**: пути обработанных файлов → JDBC append в `bronze.processed_files`.

`flock -n /tmp/parse.lock` в cron-команде предотвращает overlap, если предыдущий запуск ещё не закончился.

### `SessionParser` — конечный автомат

`parser/SessionParser.scala`. 3 состояния:
- **Neutral** — между блоками
- **AwaitingQsResults(ts, query)** — увидели `QS`, ждём строку результатов
- **InCardSearch(ts, params, awaitingResults)** — собираем параметры CARD_SEARCH'а; флаг `awaitingResults` отделяет «до CARD_END» от «после CARD_END»

Сборка многострочных событий (`QS` + результаты, `CARD_SEARCH_START` + `$params` + `CARD_END` + результаты) идёт через накопление в стейте. Нарушения протокола (например, `$param` после `CARD_END`) → `RawEvent.malformed`.

---

## ClickHouse: hot window + cold backfill

`gold.*_daily_mv` — Refreshable MV с `REFRESH EVERY 1 MINUTE` и `WHERE event_date >= today() - 10`. На каждой итерации:

1. `SELECT event_date, doc_id, count() FROM bronze.events FINAL WHERE ... AND event_date >= today() - 10`
2. `INSERT INTO gold.*_daily` — новые строки с `computed_at = now()`
3. `ReplacingMergeTree(computed_at)` дедуплицирует по `(date, doc_id)`, оставляя последнюю версию

Cold partitions (`event_date < today() - 10`) MV **никогда не трогает** — экономия CPU. Историческое наполнение делается отдельно:

- **CI workflow `cold-backfill one-shot`** — выполняется при каждом push'е после `compose up + sleep 120s`
- **Scheduler `cold-backfill @every 24h`** — daily safety net на случай, если когда-нибудь появятся late-arriving cold events

Hot window = 10 дней >= backdating range генератора (7 дней) + 3 дня safety margin.

**Важно для запросов** к `gold.*`: всегда использовать `FROM ... FINAL`, иначе ReplacingMergeTree вернёт все версии до фоновой склейки парт'ов → счётчики раздуты.

---

## Генератор данных

`generator/src/`, ~250 LOC Rust. Три модуля: `main.rs` (Poisson scheduler), `dist.rs` (snapshot распределений), `session.rs` (рендер сессии).

### Snapshot реальных распределений

При первом старте читает 500 случайных файлов из S3, выделяет:
- Распределение длин сессий
- Частоты типов событий (QS/CARD/DOC_OPEN)
- Gaps между событиями
- Top-5000 doc_ids
- 200 примеров queries

Кэширует в `/var/cache/generator/distributions.json` для рестартов.

### Non-homogeneous Poisson по часу

Каждый час `plan_arrivals(10_000, 3600.0)`:
1. Density `f(u) = 1 + 0.5·sin(2π·u) + 0.3·sin(6π·u)` — две синусоиды
2. Acceptance-rejection sampling против `f_max = 1.8`, ~55% accept rate
3. Sorted 10k offset-ов в `[0, 3600)`

Цикл: sleep до каждого target-времени → `tokio::spawn` отправляет S3 PUT параллельно. Peak-to-trough ≈ 2× — реалистичный «дышащий» поток с волнами.

### Backdating

`SESSION_START = Utc::now() - rand(0..7д)`. Прибытие файла в S3 — live (по Poisson-расписанию), но **внутреннее время** — случайная точка в последних 7 сутках. Это наполняет дашборд за «последнюю неделю» живой плотностью данных вместо тонкой сегодняшней полоски.

---

## Дашборд Grafana

`grafana/dashboards/lakehouse.json`. Provisioning заливает datasource и dashboard автоматически при старте контейнера.

### Метрики из task.md

| Панель | Что показывает |
|---|---|
| **Метрика 1: ACC_45616 в результатах карточного поиска** | `sum(hits)` из `gold.card_search_doc_hits_daily FINAL WHERE doc_id='ACC_45616'`. Эталон 479 (на чистой истории, без синтетики) |
| **Метрика 2: топ-20 документов по открытиям через QS** | Table из `gold.qs_doc_opens_daily FINAL ORDER BY opens DESC` |
| **Hits ACC_45616 timeseries** | Метрика 1 в разрезе дней (`event_date`) |
| **Top-3 doc opens timeseries** | Топ-3 doc'а по сумме opens, разложенные по дням |

### Pipeline observability

| Панель | Что показывает |
|---|---|
| **Всего событий в bronze** | `count() FROM bronze.events FINAL` |
| **Уникальных сессий** | `countDistinct(session_id) FROM bronze.events` |
| **Распределение событий по типам** | Piechart по `event_type` |

### Data exploration

| Панель | Что показывает |
|---|---|
| **Топ-20 QS-запросов** | Текст запроса + частота |
| **Распределение событий на сессию** | Histogram bucket-ов длин сессий |
| **Длительность сессии** | Histogram bucket-ов «< 1 мин» / «5-15 мин» / ... |
| **Длина сессии за 24h** | avg/p50/p90/p99 событий на сессию |
| **Поведенческий профиль сессий** | Donut: «QS + CARD / только QS / только CARD / только просмотр» |
| **Открытий документов на сессию** | Histogram bounce rate vs engaged sessions |
| **Активность: час × день недели** | 7×24 grid с colored cells, ивенты по `toHour × toDayOfWeek` |
| **Новые сессии в минуту (live — Poisson)** | Timeseries последнего часа — видны волны Poisson-модуляции |

### Spark observability

| Панель | Что показывает |
|---|---|
| **Watermark: всего обработано файлов** | Растущий counter `bronze.processed_files` |
| **ParseJob throughput за час** | Дельта watermark за последний час (~10k/час совпадает с темпом генератора) |
| **Время с последнего ParseJob** | `now() - max(processed_at)` с порогами health |
| **Parquet snapshot bronze (last partition date)** | Дата последней записанной partition в `s3://parquet-exports/bronze/` |
| **Parquet файлов в S3: bronze / card_hits / qs_opens** | `count(DISTINCT _file) FROM s3(minio_s3, ...)` — кол-во parquet-партов |

### UI URLs

**Prod** (demo инстанс на `quasarity.com`):

| URL | Что | Креды |
|---|---|---|
| <http://demo-1.quasarity.com> | Grafana | anonymous Admin (без логина) |
| <http://demo-2.quasarity.com/play> | ClickHouse Play | `quasarity` / `mini44991231222` (read-only пользователь) |
| <http://demo-3.quasarity.com> | MinIO Console — обзор бакетов | `quasarity` / `mini44991231222` |

**Локально** (`docker-compose up` на своей машине):

| URL | Что |
|---|---|
| <http://localhost:8010> | Grafana |
| <http://localhost:8011/play> | ClickHouse Play |
| <http://localhost:8012> | MinIO Console |

Креды для локального запуска берутся из `.env` (по умолчанию `admin` / `admin12345`).

---

## Быстрый старт

```bash
cd final
docker-compose up -d --build
# первая сборка ~5-7 минут: maven build spark-jobs, rust build generator
# CH стартует через ~30 сек и применяет DDL из ./clickhouse/init/
```

После `up`:
- `mc-init` создаёт бакеты `sessions/`, `warehouse/`, `ch-warehouse/`, `parquet-exports/` и заливает 10000 файлов из `./data/` в `s3://sessions/`
- `clickhouse` применяет DDL: `bronze.events`, `bronze.processed_files`, `gold.*`, `exports.*`, 5 Refreshable MVs
- `generator` снимает распределения с 500 файлов, начинает Poisson-поток ~10k сессий/час с backdating'ом
- `spark-job` стоит idle JVM, ofelia дёргает ParseJob по cron каждую минуту

Через ~2 минуты bronze наполнен ~135k событий из 10k seed-файлов, MV пересчитала hot-партиции. Чтобы исторические дате попали в `gold.*` сразу (а не через сутки от scheduler'а):

```bash
CH="docker compose exec -T clickhouse clickhouse-client --user <user> --password <pw>"

$CH --query "INSERT INTO gold.card_search_doc_hits_daily SELECT event_date AS date, JSONExtractString(arrayJoin(JSONExtractArrayRaw(result_doc_ids_json))) AS doc_id, count() AS hits, now() AS computed_at FROM bronze.events FINAL WHERE event_type = 'CARD_SEARCH' AND event_date IS NOT NULL AND event_date < today() - 10 GROUP BY date, doc_id"

$CH --query "INSERT INTO gold.qs_doc_opens_daily SELECT event_date AS open_date, doc_id, count() AS opens, now() AS computed_at FROM bronze.events FINAL WHERE event_type = 'DOC_OPEN' AND search_kind = 'QS' AND doc_id IS NOT NULL AND event_date IS NOT NULL AND event_date < today() - 10 GROUP BY event_date, doc_id"
```

В CI/CD workflow этот step есть автоматически после деплоя.

---

## Целевые метрики и проверка

**Metric 1** — количество карточных поисков, вернувших `ACC_45616`:

```sql
SELECT sum(hits) AS total
FROM gold.card_search_doc_hits_daily FINAL
WHERE doc_id = 'ACC_45616'
  AND date <= '2026-05-28'    -- отсечь синтетику, оставить только историческое
```

Эталон: **479** на исходных 10000 seed-файлах.

**Metric 2** — открытия документов через QS, по дням:

```sql
SELECT open_date, doc_id, opens
FROM gold.qs_doc_opens_daily FINAL
WHERE open_date <= '2026-05-28'
ORDER BY opens DESC
LIMIT 50
```

Полный экспорт в CSV:

```bash
docker compose exec clickhouse clickhouse-client --user <user> --password <pw> --query "
  SELECT open_date, doc_id, opens FROM gold.qs_doc_opens_daily FINAL 
  WHERE open_date <= '2026-05-28' ORDER BY open_date, opens DESC FORMAT CSV
" > metric2.csv
```

---

## Где данные физически

| Слой | Формат | Физически |
|---|---|---|
| Сырые сессии | text (cp1251) | `s3://sessions/{0..9999, synthetic-*}` |
| **bronze.events** | CH MergeTree | `s3://ch-warehouse/...` (CH-native binary) |
| **bronze.processed_files** | CH MergeTree | `s3://ch-warehouse/...` |
| **gold.*** | CH MergeTree | `s3://ch-warehouse/...` |
| Parquet snapshots | open parquet | `s3://parquet-exports/{bronze,gold}/` (refresh @1h) |
| CH metadata | CH-internal | named volume `clickhouse-data` |

**Vendor lock** на bronze/gold — осознанный. Если CH помрёт: данные в `s3://ch-warehouse/` сохраняются, но без CH нечитаемы. Recovery — поднять CH на том же volume `clickhouse-data`. Открытый escape — `s3://parquet-exports/`.

---

## ClickHouse RBAC quirks

В compose есть **два user'а** ClickHouse — потому что один не может всё:

| User | Создаётся через | Может |
|---|---|---|
| `quasarity` | XML (entrypoint из `CLICKHOUSE_USER` env) | ALL grants + ACCESS_MANAGEMENT, но **не** USE NAMED COLLECTION (XML-users read-only для SQL grant'ов) |
| `admin_sql` | SQL (`config.d/startup_scripts.xml`) | ALL + NAMED COLLECTION ADMIN (SQL-storage, grant работает) |

`admin_sql` нужен для:
1. **CREATE TABLE на S3 engine с named_collection** — `exports.*_parquet` таблицы
2. **Grafana SELECT через `s3()` table function** — чтение parquet-файлов из MinIO

Креды `admin_sql` хардкодом в `clickhouse/config.d/startup_scripts.xml` (это не secret, юзер ходит только внутри docker network).

Named collection `minio_s3` определена в `clickhouse/config.d/named_collections.xml` с `<from_env="MINIO_ROOT_USER">` — кред подсасывается из env контейнера, не хардкодится в DDL.

## Известные ограничения

Честный список, что не сделано:

- **Тестов нет** — парсер чистый Scala, был бы хороший candidate для scalatest. README не assertit `479` через CI.
- **Дизайн-спека и план в `docs/`** описывают старый Iceberg-вариант — устарели, помечено в preamble.
- **Timezone парсера** — `ZoneOffset.UTC`. Исторические логи КонсультантПлюс почти точно МСК → `event_time` смещён на 3 часа. На метрики (через `event_date`) не влияет.
- **Malformed events** парсятся в bronze (`event_type='MALFORMED'`), но ни одна Grafana панель их не показывает. Если парсер сломается — никто не узнает.
- **`spark.sql.codegen.wholeStage=false`** — на aarch64 (M-чипы через colima) Hotspot падает SIGSEGV при codegen-агрегациях. Отключение лечит.
- **CH JDBC driver `0.4.6-all`** — последняя версия с реально self-contained `-all` jar. 0.6.x разбил classifier → `NoClassDefFoundError`. См. `spark-jobs/Dockerfile`.
- **Spark JDBC не умеет CH Array** — workaround через JSON-сериализацию (`card_params_json`, `result_doc_ids_json`), парсинг обратно в MV через `JSONExtractArrayRaw`.
- **CH listen_host** — по умолчанию `127.0.0.1`, другие контейнеры не подключаются. `clickhouse/config.d/listen.xml` ставит `0.0.0.0`.
- **Partitioned S3 engine write-only** — `SELECT FROM exports.*_parquet` падает с `NOT_IMPLEMENTED`. Поэтому panel'ы для подсчёта parquet-файлов используют `FROM s3(minio_s3, ...)` table function вместо engine таблицы.
- **Bucket `warehouse/`** создаётся `mc-init`, но не используется — leftover из старого Iceberg-плана. Безвредно.

---

## Остановка / reset

```bash
docker compose down            # остановить, оставить volumes
docker compose down -v         # + удалить volumes (полный reset)
```

**Когда нужен volume reset**: если меняли `01_schemas.sql` или dashboard provisioning, а у CH/Grafana уже есть существующий volume — init-script не перезапустится, схемы не обновятся. Wipe volumes → следующий `up` применит свежие конфиги.

---

## Структура проекта

```
final/
├── docker-compose.yml
├── .env                       # MinIO/CH/AWS creds — single source of truth
├── data/                      # 10k seed файлов (сдвинутые таймстемпы до 2026-05-28)
├── docs/superpowers/{specs,plans}/   # дизайн + план (УСТАРЕЛИ, описывают Iceberg)
├── clickhouse/
│   ├── config.d/
│   │   ├── storage.xml                # S3 disk через from_env (для CH MergeTree)
│   │   ├── listen.xml                 # listen_host = 0.0.0.0
│   │   ├── named_collections.xml      # minio_s3 named collection (from_env)
│   │   └── startup_scripts.xml        # creates admin_sql user + grants NAMED COLLECTION
│   ├── users.d/                       # mount RW (entrypoint пишет default-user.xml)
│   └── init/01_schemas.sql            # bronze + processed_files + gold + exports + MVs
├── scheduler/config.ini       # ofelia: parse @1m, cold-backfill @24h
├── grafana/
│   ├── provisioning/{datasources,dashboards}/  # provisioning yaml
│   └── dashboards/lakehouse.json               # 22 панелей
├── generator/                 # Rust generator (Cargo.toml + src/{main,dist,session}.rs)
└── spark-jobs/                # Scala application
    ├── pom.xml
    ├── Dockerfile             # multi-stage: maven build → apache/spark + CH JDBC
    ├── conf/spark-defaults.conf
    └── src/main/scala/ru/consultant/lakehouse/
        ├── model/             # RawEvent, CardParam, EventType
        ├── parser/            # TimeParser, EventLineParser, SessionParser
        ├── config/            # AppConfig (Typesafe Config)
        └── jobs/              # SparkApp, ParseJob
```

CI/CD: `.github/workflows/ci-cd.yml` — 4 джоба (Scala build, Rust build, compose validate, deploy). Push в main → SSH-деплой на хост с одношотовым cold-backfill после старта.
