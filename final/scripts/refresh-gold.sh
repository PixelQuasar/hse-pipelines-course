set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

set -a
. ./.env
set +a

docker compose exec -T clickhouse clickhouse-client \
  --user "$MINIO_ROOT_USER" \
  --password "$MINIO_ROOT_PASSWORD" \
  --multiquery <<'SQL'
SYSTEM REFRESH VIEW gold.card_search_doc_hits_daily_mv;
SYSTEM REFRESH VIEW gold.qs_doc_opens_daily_mv;

INSERT INTO gold.card_search_doc_hits_daily
SELECT
  event_date AS date,
  JSONExtractString(arrayJoin(JSONExtractArrayRaw(result_doc_ids_json))) AS doc_id,
  count() AS hits,
  now() AS computed_at
FROM bronze.events FINAL
WHERE event_type = 'CARD_SEARCH'
  AND event_date IS NOT NULL
  AND event_date < today() - 10
GROUP BY date, doc_id;

INSERT INTO gold.qs_doc_opens_daily
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
  AND event_date < today() - 10
GROUP BY event_date, doc_id;
SQL
