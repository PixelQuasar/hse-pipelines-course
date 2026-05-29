CREATE DATABASE IF NOT EXISTS exports;

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
