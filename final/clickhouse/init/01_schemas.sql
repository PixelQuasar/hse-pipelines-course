-- =====================================================================
-- Lakehouse schema for ClickHouse
-- Bronze: typed events from Spark (via JDBC)
-- Gold:   pre-aggregated metrics, maintained by CH Refreshable MVs
-- All MergeTree data physically resides in s3://ch-warehouse/ (S3 disk)
-- =====================================================================

CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS gold;
CREATE DATABASE IF NOT EXISTS exports;

-- ===== bronze =====
-- ReplacingMergeTree on (session_id, event_seq) deduplicates rows
-- if ParseJob is re-run (eventual dedup via background merge).
CREATE TABLE IF NOT EXISTS bronze.events
(
  session_id     String,
  event_seq      Int32,
  event_time     Nullable(DateTime),
  event_date     Nullable(Date),
  event_type     LowCardinality(String),
  search_id      Nullable(String),
  search_kind    LowCardinality(Nullable(String)),
  query_text     Nullable(String),
  card_params_json   String DEFAULT '[]',   -- serialized JSON of [{param_id, value}, ...]
  result_doc_ids_json String DEFAULT '[]',  -- serialized JSON of ["doc1", "doc2", ...]
  doc_id         Nullable(String),
  parse_error    Nullable(String),
  raw_line       Nullable(String),
  ingested_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(coalesce(event_date, toDate('1970-01-01')))
ORDER BY (session_id, event_seq)
SETTINGS storage_policy = 's3_main', index_granularity = 8192;

-- ===== bronze.processed_files (watermark) =====
-- ParseJob reads this table to skip already-ingested files.
-- ReplacingMergeTree by path = a single source of truth even if
-- a race inserts the same path twice.
CREATE TABLE IF NOT EXISTS bronze.processed_files
(
  path          String,
  processed_at  DateTime DEFAULT now(),
  events_count  UInt32   DEFAULT 0
)
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(processed_at)
ORDER BY path
SETTINGS storage_policy = 's3_main';

-- ===== gold tables (targets for Refreshable MVs) =====

CREATE TABLE IF NOT EXISTS gold.card_search_doc_hits_daily
(
  date         Date,
  doc_id       String,
  hits         UInt64,
  computed_at  DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, doc_id)
SETTINGS storage_policy = 's3_main';

CREATE TABLE IF NOT EXISTS gold.qs_doc_opens_daily
(
  open_date    Date,
  doc_id       String,
  opens        UInt64,
  computed_at  DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(open_date)
ORDER BY (open_date, doc_id)
SETTINGS storage_policy = 's3_main';

-- ===== Refreshable Materialized Views =====
-- Recompute every 1 minute. Full recompute is cheap on our data volume.
-- ReplacingMergeTree dedups by computed_at, so each refresh effectively
-- overwrites the previous run for affected keys.

-- Hot window = 10 days. Generator backdates SESSION_START up to 7 days,
-- so 10d gives a safe margin for late ingest + day-boundary edge cases.
-- Older partitions stay "frozen" in gold (never re-inserted), values held
-- by ReplacingMergeTree from the last in-window computation.
--
-- Bootstrap of historical (cold) data happens via a one-shot INSERT below.

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.card_search_doc_hits_daily_mv
REFRESH EVERY 1 MINUTE APPEND
TO gold.card_search_doc_hits_daily AS
SELECT
  event_date AS date,
  JSONExtractString(arrayJoin(JSONExtractArrayRaw(result_doc_ids_json))) AS doc_id,
  count() AS hits,
  now() AS computed_at
FROM bronze.events FINAL
WHERE event_type = 'CARD_SEARCH'
  AND event_date IS NOT NULL
  AND event_date >= today() - 10
GROUP BY date, doc_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.qs_doc_opens_daily_mv
REFRESH EVERY 1 MINUTE APPEND
TO gold.qs_doc_opens_daily AS
SELECT
  event_date AS open_date,
  doc_id,
  count() AS opens,
  now() AS computed_at
FROM bronze.events FINAL
WHERE event_type = 'DOC_OPEN'
  AND search_kind = 'QS'
  AND doc_id IS NOT NULL
  AND event_date IS NOT NULL
  AND event_date >= today() - 10
GROUP BY event_date, doc_id;

-- Cold partitions (event_date < today() - 10) are backfilled by a daily
-- scheduler job (see scheduler/config.ini → cold-backfill) — not here,
-- because at schema-init time bronze.events is still empty.

-- =====================================================================
-- Parquet exports: scheduled CH-side dump of CH-native tables into open
-- parquet on S3. Lets other engines (Spark/DuckDB/pandas) read the data
-- without ClickHouse. Refreshes hourly; per-partition files are overwritten.
-- =====================================================================

CREATE TABLE IF NOT EXISTS exports.bronze_events_parquet
(
  session_id       String,
  event_seq        Int32,
  event_time       Nullable(DateTime),
  event_date       Nullable(Date),
  event_type       String,
  search_id        Nullable(String),
  search_kind      Nullable(String),
  query_text       Nullable(String),
  card_params_json    String,
  result_doc_ids_json String,
  doc_id           Nullable(String),
  parse_error      Nullable(String),
  raw_line         Nullable(String)
)
ENGINE = S3(minio_s3,
  url = 'http://minio:9000/parquet-exports/bronze/events-{_partition_id}.parquet',
  format = 'Parquet'
)
PARTITION BY toYYYYMMDD(coalesce(event_date, toDate('1970-01-01')));

CREATE MATERIALIZED VIEW IF NOT EXISTS exports.bronze_events_parquet_mv
REFRESH EVERY 1 HOUR
TO exports.bronze_events_parquet AS
SELECT
  session_id, event_seq, event_time, event_date, event_type,
  search_id, search_kind, query_text, card_params_json,
  result_doc_ids_json, doc_id, parse_error, raw_line
FROM bronze.events
WHERE event_date IS NOT NULL;

CREATE TABLE IF NOT EXISTS exports.card_search_doc_hits_daily_parquet
(
  date         Date,
  doc_id       String,
  hits         UInt64,
  computed_at  DateTime
)
ENGINE = S3(minio_s3,
  url = 'http://minio:9000/parquet-exports/gold/card-hits-{_partition_id}.parquet',
  format = 'Parquet'
)
PARTITION BY toYYYYMM(date);

CREATE MATERIALIZED VIEW IF NOT EXISTS exports.card_search_doc_hits_daily_parquet_mv
REFRESH EVERY 1 HOUR
TO exports.card_search_doc_hits_daily_parquet AS
SELECT date, doc_id, hits, computed_at FROM gold.card_search_doc_hits_daily;

CREATE TABLE IF NOT EXISTS exports.qs_doc_opens_daily_parquet
(
  open_date    Date,
  doc_id       String,
  opens        UInt64,
  computed_at  DateTime
)
ENGINE = S3(minio_s3,
  url = 'http://minio:9000/parquet-exports/gold/qs-opens-{_partition_id}.parquet',
  format = 'Parquet'
)
PARTITION BY toYYYYMM(open_date);

CREATE MATERIALIZED VIEW IF NOT EXISTS exports.qs_doc_opens_daily_parquet_mv
REFRESH EVERY 1 HOUR
TO exports.qs_doc_opens_daily_parquet AS
SELECT open_date, doc_id, opens, computed_at FROM gold.qs_doc_opens_daily;
