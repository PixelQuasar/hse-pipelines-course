# Spark Lakehouse Pipeline для аналитики сессий КонсультантПлюс

**Дата:** 2026-05-23, ревизия 2026-05-24
**Статус:** реализовано

> **Эволюция архитектуры (2026-05-24).** Исходный дизайн (Iceberg + Nessie + Spark
> ParseJob + Spark AggregateJob) был реализован полностью, метрики совпали
> с эталонами. После анализа целесообразности lakehouse для нашего объёма
> (10k файлов / мегабайты данных, 1 потребитель) проект мигрировал на более
> прагматичную архитектуру **ClickHouse + S3 + Spark parser-only**:
>
> - `bronze.events` и `gold.*` хранятся как **CH MergeTree на S3 disk** (физически в `s3://ch-warehouse/`, формат CH-native)
> - **AggregateJob выкинут** — заменён на CH Refreshable Materialized Views (real-time агрегации в storage layer)
> - **Spark остаётся** только как parser (raw text → JDBC INSERT в CH bronze)
> - **Iceberg + Nessie выкинуты** — Iceberg overhead не оправдан, его фичи (multi-engine, time travel, schema evolution) у нас не использовались
> - **Open-format escape hatch:** ежечасный CH MV дампит bronze/gold как parquet в `s3://parquet-exports/` (читается любым движком, не CH)
> - Vendor lock на bronze/gold принят осознанно (derived data, всегда восстанавливаемо из `s3://sessions/`)
>
> Целевые метрики (479 для `ACC_45616`, 75516 строк для daily QS opens) совпадают с Iceberg-версией с точностью до строки. Подробнее в README.md (раздел "Эволюция архитектуры" в нижней части).
>
> Документ ниже — оригинальный Iceberg-дизайн, оставлен для истории решений.

---

## 1. Контекст и задача

Финальный проект курса по pipeline'ам. Входные данные — 10 000 файлов в `final/data/`, каждый
файл представляет одну пользовательскую сессию системы КонсультантПлюс. Внутри сессии —
текстовый лог событий: `SESSION_START/END`, `QS` (быстрый поиск), `CARD_SEARCH_*`
(карточный поиск), `DOC_OPEN`.

Задание требует посчитать две метрики:

1. Количество раз, когда в **карточке** производили поиск документа с идентификатором
   `ACC_45616` (т.е. сколько карточных поисков вернули этот документ в списке найденных).
2. Количество **открытий каждого документа**, найденного через **быстрый поиск (QS)**,
   за каждый день.

Технические требования задания: Apache Spark, Scala, Maven. Оценивается корректность
чисел И качество кода.

## 2. Цели проекта (за рамками минимума)

Кроме самой задачи, проект демонстрирует:

- **Lakehouse-архитектуру** на полностью локальном стеке (MinIO + Iceberg + Nessie).
- **Многоступенчатый pipeline** с разделением raw → bronze → gold.
- **«Живую» систему**: генератор подсыпает новые сессии, метрики обновляются
  периодически (без Structured Streaming — через batch с hot-window-cutoff).
- **Воспроизводимый запуск через Docker Compose** — без установки Spark на хост.

Что **не** в scope первой итерации (отложено):

- Дашборд (Trino + Superset). Архитектурно поддерживается, но добавляется отдельной фазой.
- Unit/integration тесты Scala-кода. Структура проекта тестам не противоречит, но
  тесты добавим следующей итерацией.
- Production-grade observability (метрики джоб, alerting).

## 3. Архитектурное решение (обзор)

### 3.1 Топология сервисов

```
docker-compose (lakehouse-net):

┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│   ┌──────────┐                                                           │
│   │ generator│── PUT s3://sessions/<uuid>.txt ──────────────┐            │
│   │ (Python) │                                              │            │
│   └──────────┘                                              ▼            │
│                                              ┌──────────────────────┐    │
│   ./data ──── mc-init ──── PUT ─────────────►│  minio               │    │
│   (10k файлов)                                │  s3://sessions/      │    │
│                                              │  s3://warehouse/     │    │
│                                              │     bronze/  gold/   │    │
│                                              └──┬───────────────────┘    │
│                                                 │ S3 API                 │
│  ┌────────────┐                                 │                        │
│  │ scheduler  │── docker exec ──────┐           ▼                        │
│  │ (ofelia)   │  every 30s          │   ┌────────────┐  REST  ┌──────┐   │
│  └────────────┘                     └──►│ spark-job  │◄──────►│nessie│   │
│                                          │ (idle JVM, │        └──────┘  │
│                                          │  scala jar)│                   │
│                                          └────────────┘                   │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

Порты с хоста:
   9000  — MinIO S3 API
   9001  — MinIO Web UI
   19120 — Nessie REST/UI
```

### 3.2 Слои данных

| Слой | Содержимое | Формат | Где живёт |
|------|------------|--------|-----------|
| **raw** | Сырые текстовые файлы сессий (10k из задания + подсыпает генератор) | text | `s3://sessions/` |
| **bronze** | Распарсенные типизированные события | Iceberg parquet | `s3://warehouse/bronze/events/` |
| **gold**   | Аналитические метрики per-day | Iceberg parquet | `s3://warehouse/gold/{card_search_doc_hits_daily, qs_doc_opens_daily}/` |

Каталог Iceberg-таблиц — Nessie REST.

### 3.3 Pipeline-фазы

```
Phase 1 (Bootstrap, выполняется вручную один раз):
   ParseJob (читает ВСЕ файлы из s3://sessions/*)         → bronze.events
   AggregateJob --mode=full (без cutoff, по всей bronze)  → gold.*

Phase 2 (Continuous, после bootstrap):
   scheduler @every 30s:
     ParseJob       (incremental, только новые файлы)     → bronze.events (append)
     AggregateJob   (--mode=incremental, cutoff=3 days)   → gold.* (MERGE на hot window)
   generator @every N sec:
     → пишет новую синтетическую сессию в s3://sessions/synthetic/
```

## 4. Ключевые архитектурные решения и их обоснование

### 4.1 Hot-window-recompute вместо Structured Streaming

AggregateJob в обычном режиме пересчитывает **только последние 3 дня** (cutoff), не весь
bronze. Это:

- Даёт ту же семантику, что watermark в стриминге (события старше cutoff не учитываются).
- Идемпотентно «бесплатно»: повторный запуск с тем же cutoff даёт тот же результат →
  MERGE-overwrite затронутых ключей не вызывает дублей при сбое.
- Не требует Spark Structured Streaming, checkpoint'ов, state store, long-running
  процессов.
- Стоимость постоянна: O(3 дня данных) с partition pruning по `event_date`.

Альтернативы (Structured Streaming, recompute all) рассмотрены и отброшены: streaming
вводит сложность тестирования и checkpoint management без выигрыша на нашем объёме;
recompute all не масштабируется при долгой работе.

**Допущение операционного режима:** AggregateJob запускается не реже, чем раз в
`hot_window_days` (по умолчанию 3 дня). Если AggregateJob простаивает дольше cutoff,
события за дни простоя в интервале `(today − days_offline, today − hot_window_days]`
**не попадут в gold** — они окажутся вне фильтра на следующем запуске. Для дипломного
проекта это приемлемо: scheduler в compose работает непрерывно. Решение этого edge
case (adaptive cutoff = `min(today − 3d, MAX(date) FROM gold.*)`) обсуждалось и
отложено как out-of-scope первой итерации.

### 4.2 Bootstrap-фаза для исторических данных

10k файлов из `final/data/` имеют `event_time` 2020 года. При hot-window-cutoff = 3 дня
от `now()` они не попадут в incremental aggregate. Поэтому:

- **Первый запуск AggregateJob** делается с флагом `--mode=full` — пересчёт по всем bronze.
  Это **единоразово** заполняет gold для всех исторических дат.
- **Последующие запуски** — `--mode=incremental` (по cron'у), только hot window.

### 4.3 Все сервисы в Docker

Задание предписывает «установить Spark локально», но мы сознательно идём против буквы
этого требования ради воспроизводимости. Spark запускается в контейнере `spark-job`,
сборка jar — multi-stage Dockerfile (`maven` build → `bitnami/spark` runtime).
Проверяющий не должен ставить Spark/Scala/Maven на свою машину — достаточно
`docker compose up`.

### 4.4 Iceberg ради pipeline-зрелости

Iceberg даёт ACID writes, snapshot-based incremental reads, и единый каталог. Альтернативы
(plain parquet + Postgres для gold; Hive Metastore) рассмотрены и отброшены: Iceberg-стек
лучше демонстрирует современный lakehouse-паттерн.

### 4.5 Парсер — отдельный «pure Scala» пакет

Парсер не импортит `org.apache.spark.*` — это `SessionParser.parse(sessionId, content): Seq[RawEvent]`.
Это даёт:

- Быстрые unit-тесты без Spark.
- Чёткую границу: если завтра парсер понадобится вне Spark — переезжает без переписывания.
- Архитектурно соответствует «1 сервис парсит, 1 сервис аналитирует», только оба сервиса
  упакованы в один JVM-процесс.

## 5. Схемы данных

### 5.1 bronze.events (Iceberg)

```
session_id        STRING       идентификатор файла-сессии (имя файла, "0", "1403", ...)
event_seq         INT          порядковый номер события в сессии (для дедупа)
event_time        TIMESTAMP    распарсенное время; null если не удалось распарсить
event_date        DATE         partition column = date(event_time)
event_type        STRING       SESSION_START | SESSION_END | QS | CARD_SEARCH | DOC_OPEN | MALFORMED
search_id         STRING       идентификатор поиска (QS, CARD_SEARCH, DOC_OPEN); null для SESSION_*
search_kind       STRING       QS | CARD | null. Заполняется парсером на основе сессии-в-памяти:
                                  - у QS-события: "QS"
                                  - у CARD_SEARCH: "CARD"
                                  - у DOC_OPEN: проставляется из ранее встреченного в той же
                                    сессии поиска с тем же search_id (если такого нет — null)
query_text        STRING       только для QS (декодировано из cp1251)
card_params       ARRAY<STRUCT<param_id:STRING, value:STRING>>   только для CARD_SEARCH
result_doc_ids    ARRAY<STRING>   для QS и CARD_SEARCH (найденные документы)
doc_id            STRING       только для DOC_OPEN
parse_error       STRING       null если ок; иначе сообщение
raw_line          STRING       сохраняем только если parse_error

partitioned by (event_date)
sort by (session_id, event_seq)
natural key для дедупа: (session_id, event_seq)
```

### 5.2 bronze.processed_files (Iceberg, tracker для incremental ParseJob)

```
filename       STRING     S3 path сырого файла
processed_at   TIMESTAMP
```

### 5.3 gold.card_search_doc_hits_daily (Iceberg)

```
date         DATE
doc_id       STRING
hits         BIGINT     количество карточных поисков за этот день, вернувших этот doc_id
computed_at  TIMESTAMP

key: (date, doc_id)
```

Для метрики 1 из задания: `SELECT SUM(hits) FROM ... WHERE doc_id = 'ACC_45616'`.

### 5.4 gold.qs_doc_opens_daily (Iceberg)

```
open_date    DATE
doc_id       STRING
opens        BIGINT     сколько раз doc_id открыт через QS в этот день
computed_at  TIMESTAMP

key: (open_date, doc_id)
```

## 6. Алгоритмы джоб (high-level)

### 6.1 ParseJob

```
1. Прочитать список файлов в s3://sessions/
2. Anti-join с bronze.processed_files → new_files
3. Для каждого new_file:
     a. binaryFiles → bytes → new String(bytes, "Cp1251")
     b. SessionParser.parse(sessionId, content) → Seq[RawEvent]
4. RDD[RawEvent] → DataFrame → dropDuplicates(session_id, event_seq)
5. Append в bronze.events + insert в bronze.processed_files
   (в одной Iceberg-сессии Spark, но это два разных INSERT — детальная атомарность
    рассматривается на уровне implementation plan)
```

### 6.2 AggregateJob

```
Аргументы CLI:
  --mode=full|incremental  (default: incremental)
  --cutoff-days=N          (default: 3)

bronze = spark.read.format("iceberg").load("warehouse.bronze.events")

cutoff = if (mode == full) DATE("1970-01-01") else current_date - cutoff_days

bronzeWindow = bronze.filter($"event_date" >= cutoff)

// search_kind у DOC_OPEN уже заполнен парсером (см. 5.1). Здесь join не нужен.

// Метрика 1
hitsDelta = bronzeWindow
  .filter(event_type === "CARD_SEARCH")
  .withColumn("doc_id", explode(result_doc_ids))
  .groupBy(event_date as "date", doc_id)
  .agg(count(*) as "hits")
MERGE INTO gold.card_search_doc_hits_daily USING hitsDelta ...
   WHEN MATCHED THEN UPDATE SET hits = source.hits  (overwrite)
   WHEN NOT MATCHED THEN INSERT ...

// Метрика 2
opensDelta = bronzeWindow
  .filter(event_type === "DOC_OPEN" && search_kind === "QS")
  .groupBy(event_date as "open_date", doc_id)
  .agg(count(*) as "opens")
MERGE INTO gold.qs_doc_opens_daily USING opensDelta ...
   (analogously: overwrite hits для затронутых (open_date, doc_id))
```

## 7. Структура проекта

```
final/
├── docker-compose.yml
├── docs/superpowers/specs/2026-05-23-spark-lakehouse-design.md   ← этот документ
├── data/                              # 10k исходных файлов (mount в mc-init)
├── generator/                         # Rust long-running сервис
│   ├── Dockerfile                     # multi-stage: rust build → slim runtime
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── scheduler/
│   └── config.ini                     # ofelia cron-jobs
└── spark-jobs/                        # Scala-приложение
    ├── pom.xml
    ├── Dockerfile                     # multi-stage build
    ├── conf/spark-defaults.conf       # Spark + Iceberg + Nessie + S3 confs
    └── src/main/
        ├── scala/ru/consultant/lakehouse/
        │   ├── model/         # RawEvent, CardParam, EventType  — NO spark.* imports
        │   ├── parser/        # TimeParser, EventLineParser, SessionParser — NO spark.*
        │   ├── io/            # IcebergSchemas, BronzeWriter, GoldWriter
        │   ├── config/        # AppConfig
        │   └── jobs/          # SparkApp, ParseJob, AggregateJob
        └── resources/
            ├── log4j2.properties
            └── reference.conf
```

### 7.1 Maven версии (фиксируем)

```
scala.binary.version = 2.12
scala.version        = 2.12.20
spark.version        = 3.5.5
iceberg.version      = 1.7.0
nessie.version       = 0.99.0
hadoop.version       = 3.3.4   (через iceberg-aws-bundle)
java target          = 17
```

### 7.2 Maven dependencies (кратко)

- `org.apache.spark:spark-{core,sql}_2.12` — `provided` (даёт bitnami/spark runtime)
- `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12`
- `org.apache.iceberg:iceberg-aws-bundle`
- `org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12`
- `com.typesafe:config` (для reference.conf)

Plugins: `scala-maven-plugin`, `maven-shade-plugin`.

## 8. Docker Compose стек (без дашборда)

Финальный набор сервисов:

| Сервис | Образ | Назначение |
|--------|-------|-----------|
| `minio` | `minio/minio:latest` | S3-совместимое объектное хранилище |
| `mc-init` | `minio/mc:latest` | One-shot: создаёт бакеты `sessions/`, `warehouse/`, грузит 10k файлов из `./data/` в `s3://sessions/` |
| `nessie` | `ghcr.io/projectnessie/nessie:0.99.0` | REST-каталог Iceberg, метаданные в RocksDB на volume |
| `spark-job` | `cs-lakehouse/spark-job:latest` (local build) | Long-running idle JVM-контейнер с собранным jar; `ofelia` делает `docker exec spark-submit ...` |
| `generator` | `cs-lakehouse/generator:latest` (local build, Rust binary) | Long-running сервис, подсыпает новые сессии в `s3://sessions/` |
| `scheduler` | `mcuadros/ofelia:latest` | Cron: каждые 30 сек запускает ParseJob и AggregateJob в `spark-job` |

Тома: `minio-data`, `nessie-data`.

## 9. Generator (краткий дизайн)

Long-running сервис на Rust (отдельный бинарь). Выбор Rust — производительность + статически
слинкованный бинарь, что даёт минимальный runtime-образ.

### 9.1 Алгоритм

1. При первом старте — снять распределения с `s3://sessions/` (на этот момент там
   только 10k файлов из задания, загруженные mc-init) и **закешировать** в локальный
   файл (`/var/cache/generator/distributions.json` на volume):
   - длина сессии (число событий)
   - доля типов событий (QS share, CARD share, DOC_OPEN share)
   - распределение межсобытийных интервалов (секунды)
   - частота doc_id и search_id
   Кеширование важно: иначе на рестартах генератор начнёт «учиться» на собственных
   синтетических сессиях (circular feedback).
2. В loop с интервалом `INTERVAL_SECONDS`:
   - Сэмплить структуру сессии из распределений (rand::distributions + WeightedIndex)
   - Поставить `session_start_time = now()`, остальные события — относительные
     интервалы от старта (важно для cutoff: события должны попасть в hot window)
   - Сериализовать в текстовый формат (как в `final/data/`)
   - Кодировать в **cp1251** через `encoding_rs` (для совместимости с парсером ParseJob)
   - PUT в `s3://sessions/<uuid>.txt` через `aws-sdk-s3` с custom endpoint (MinIO)

### 9.2 Ключевые crates

- `tokio` — async runtime
- `aws-sdk-s3` + `aws-config` — клиент к MinIO (через endpoint override)
- `rand` + `rand_distr` — sampling из распределений (WeightedIndex для категориальных)
- `encoding_rs` — кодирование utf-8 → cp1251
- `uuid` — имена файлов
- `serde` + `serde_json` — сериализация closures distributions в кеш-файл
- `chrono` — работа со временем
- `anyhow` / `thiserror` — error handling

### 9.3 Dockerfile (multi-stage)

```dockerfile
FROM rust:1-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/generator /usr/local/bin/generator
ENTRYPOINT ["/usr/local/bin/generator"]
```

Финальный образ — ~20-30 MB, против ~150 MB для типового Python-сервиса.

## 10. Открытые вопросы / отложенные решения

Эти решения зафиксированы как «default» и могут быть переоценены в плане имплементации:

1. **CLI parsing для AggregateJob:** ручной (`args.find(_.startsWith("--mode=")...`) — без зависимостей.
2. **AWS-bundle:** взят `iceberg-aws-bundle` вместо отдельных `hadoop-aws` (бесконфликтное API S3FileIO).
3. **Java версия:** 17 (соответствует bitnami/spark:3.5).
4. **Pre-warm depcache в Dockerfile:** оставить (`dependency:go-offline`) — ускоряет повторные сборки.
5. **Часовой пояс для `dd.MM.yyyy_HH:mm:ss`:** принимаем UTC. Если позже выяснится МСК — переменная.
6. **Атомарность ParseJob (bronze.events + processed_files):** в первой итерации — два последовательных
   INSERT в одной Spark-сессии. Если ParseJob упадёт между ними — на следующем запуске тот же файл
   парсится повторно, и `dropDuplicates` спасает по `(session_id, event_seq)`. Это **at-least-once
   ingestion + idempotent dedup** — приемлемо для нашей задачи. Multi-table Iceberg transaction
   рассмотрим в будущей итерации.
7. **Trino + Superset для дашборда:** не в scope первой итерации. Архитектурно добавляются как
   два сервиса в тот же compose, читают gold через Iceberg connector. Никаких изменений в Scala-коде
   не требуется.

## 11. Что считаем «готовым» (acceptance)

- `docker compose up -d` поднимает minio, nessie, generator, scheduler, idle spark-job.
- `mc-init` корректно отрабатывает: бакеты созданы, 10k файлов из `./data/` в `s3://sessions/`.
- Один раз вручную выполнен bootstrap: `ParseJob` + `AggregateJob --mode=full`.
- В Iceberg каталоге через Nessie UI видны `warehouse.bronze.events`, `warehouse.bronze.processed_files`,
  `warehouse.gold.card_search_doc_hits_daily`, `warehouse.gold.qs_doc_opens_daily`.
- `SELECT SUM(hits) FROM warehouse.gold.card_search_doc_hits_daily WHERE doc_id='ACC_45616'`
  возвращает правильное число (метрика 1 из задания).
- `SELECT * FROM warehouse.gold.qs_doc_opens_daily ORDER BY open_date, opens DESC` показывает
  по-дневные открытия документов через быстрый поиск (метрика 2).
- Через несколько минут после старта (когда генератор подсыпает новые файлы и cron-jobs
  отработали) в `bronze.events` появляются новые сессии с `event_date` ≈ today,
  gold-метрики автоматически обновлены по hot-window.
