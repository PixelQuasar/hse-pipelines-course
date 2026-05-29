CREATE DATABASE IF NOT EXISTS gold;

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
