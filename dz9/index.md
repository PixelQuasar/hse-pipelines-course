# ДЗ9:  Trino vs StarRocks vs ClickHouse

За основу я взял очень обширную статью [ClickHouse vs StarRocks vs Presto vs Trino vs Apache Spark™ — Comparing Analytics Engines](https://www.onehouse.ai/blog/apache-spark-vs-clickhouse-vs-presto-vs-starrocks-vs-trino-comparing-analytics-engines)

Прямых значений временных бенчмарков в ней к сожалению нет, поэтому в данном отчете опираться мы будем на абстрактную оценку различных характеристик движков.

---

## Оценки

### Архитектура движка

В статье оцениваются четыре характеристики: vectorized columnar processing, shuffle speed and efficiency, caching, query plan optimizers.

| Критерий | Trino | StarRocks | ClickHouse |
|---|---|---|---|
| Vectorized columnar processing | C | A | A |
| Shuffle speed and efficiency | A | A | A |
| Caching of source/intermediate data | B | A | A |
| Query plan optimizers | A | A | A |

### Масштабирование

В статье оцениваются elastic scaling, data parallelism, load balancing, high availability.

| Критерий | Trino | StarRocks | ClickHouse |
|---|---|---|---|
| Elastic scaling | A | B | B |
| Data parallelism | A | A | A |
| Load balancing | A | B | B |
| High availability | B | A | A |

### Конкурентность

В статье оцениваются concurrent reads, concurrent writes, workload priority management.

| Критерий | Trino | StarRocks | ClickHouse |
|---|---|---|---|
| Concurrent reads | A | A | A |
| Concurrent writes | B | A | A |
| Workload priority management | A | B | A |

### Поддерживаемые форматы данных

В статье оцениваются file format support, table format support, cloud storage support.

| Критерий | Trino | StarRocks | ClickHouse |
|---|---|---|---|
| File format support | B | A | A |
| Table format support | B | B | C |
| Cloud storage support | A | A | A |

### QL

В статье оцениваются SQL support, Python support, additional language support, non-analytics connectors.

| Критерий | Trino | StarRocks | ClickHouse |
|---|---|---|---|
| SQL support | A | A | A |
| Python support | B | B | B |
| Additional language support | A | B | A |
| Non-analytics connectors | C | B | B |

### Экосистема

В статье оцениваются catalog support, cloud vendor support, self-hosted deployments.

| Критерий | Trino | StarRocks | ClickHouse |
|---|---|---|---|
| Catalog support | B | A | B |
| Cloud vendor support | B | C | C |
| Self-hosted deployments | A | A | B |

---

## Сводка оценок

- A = 3 балла
- B = 2 балла
- C = 1 балл

### Сумма баллов по разделам

| Раздел | Trino | StarRocks | ClickHouse |
|---|---:|---:|---:|
| Engine Design | 9 | 12 | 12 |
| Scalability | 11 | 10 | 10 |
| Concurrency | 8 | 8 | 9 |
| Storage Support | 7 | 8 | 7 |
| Query Language | 9 | 8 | 9 |
| Ecosystem | 8 | 8 | 7 |
| **Итого** | **52** | **54** | **54** |

### Количество оценок A / B / C

| Движок | A | B | C |
|---|---:|---:|---:|
| Trino | 12 | 8 | 2 |
| StarRocks | 12 | 6 | 0 |
| ClickHouse | 12 | 5 | 2 |

---


По архитектурным признакам производительности лидируют [`StarRocks`](dz9/index.md) и [`ClickHouse`](dz9/index.md).
Это видно по блоку [`Engine Design`](dz9/index.md): 12 против 9 у [`Trino`](dz9/index.md).

По удобству масштабирования лидирует [`Trino`](dz9/index.md).
Это видно по блоку [`Scalability`](dz9/index.md): 11 против 10 у конкурентов.

По конкурентной аналитической нагрузке немного впереди [`ClickHouse`](dz9/index.md).
Это видно по блоку [`Concurrency`](dz9/index.md): 9 против 8.

По суммарной оценке статья фактически ставит [`StarRocks`](dz9/index.md) и [`ClickHouse`](dz9/index.md) на один уровень, а [`Trino`](dz9/index.md) — совсем немного ниже

## Ключевые различия

[`Trino`](dz9/index.md) — это в первую очередь federated SQL engine: он сильнее там, где данные уже лежат во внешних системах и нужен единый SQL-слой поверх них. [`StarRocks`](dz9/index.md) и [`ClickHouse`](dz9/index.md) ближе к специализированным движкам, где требуется низкая задержка и высокая скорость аналитических запросов.

[`StarRocks`](dz9/index.md) выглядит как более сбалансированный вариант между lakehouse-интеграцией и быстрым MPP OLAP. [`ClickHouse`](dz9/index.md) обычно сильнее всего в сценариях с тяжёлыми агрегациями и событийной аналитикой, но слабее по поддержке открытых table formats.

## Анализ рисков внедрения

Для [`Trino`](dz9/index.md) главный риск — ожидать от него производительности специализированной аналитической БД: он удобен и гибок, но не всегда лучший по latency. Для [`StarRocks`](dz9/index.md) риск в более сложной эксплуатации и меньшей зрелости экосистемы по сравнению с более массовыми решениями. Для [`ClickHouse`](dz9/index.md) основной риск — сложнее спроектировать физическую модель хранения так, чтобы действительно получить максимум производительности.

## Итоговое решение

Если нужен универсальный SQL-слой поверх data lake и внешних источников, то наиболее логичный выбор — [`Trino`](dz9/index.md). Если приоритет — быстрые BI-запросы и serving аналитики, то сильнее выглядят [`StarRocks`](dz9/index.md) и [`ClickHouse`](dz9/index.md).
