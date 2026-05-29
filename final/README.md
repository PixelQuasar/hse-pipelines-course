## Архитектура

```mermaid
flowchart TD
    GEN[Generator] --> S3[(MinIO S3)]
    S3 --> SPARK[Spark ParseJob]
    SPARK --> CH[(ClickHouse<br/>bronze)]
    CH --> CH2[(ClickHouse<br/>gold)]
    CH2 --> GRAF[Grafana]
    GRAF --> USER((User))
    CH -->|parquet dump| S3
    CH2 -->|parquet dump| S3
```

## Процессы

### Джоба 1 (spark): парсинг сырых логов и формирование bronse

### Джоба 2 (СH): Агрегация gold

### Джоба 3 (СH): Бэкфилл паркетов

##  Cхема

### gold

### bronse

### Рантайм данных: окно расчетов и вотермарки

## Соответствие ТЗ

### Метрика 1

### Метрика 2
