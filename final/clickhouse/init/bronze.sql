CREATE DATABASE IF NOT EXISTS bronze;

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
  card_params_json   String DEFAULT '[]', 
  result_doc_ids_json String DEFAULT '[]', 
  doc_id         Nullable(String),
  parse_error    Nullable(String),
  raw_line       Nullable(String),
  ingested_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(coalesce(event_date, toDate('1970-01-01')))
ORDER BY (session_id, event_seq)
SETTINGS storage_policy = 's3_main', index_granularity = 8192;

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
