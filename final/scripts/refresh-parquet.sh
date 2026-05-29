set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

set -a
. ./.env
set +a

docker compose exec -T clickhouse clickhouse-client \
  --user "$MINIO_ROOT_USER" \
  --password "$MINIO_ROOT_PASSWORD" \
  --multiquery <<'SQL'
SYSTEM REFRESH VIEW exports.bronze_events_parquet_mv;
SYSTEM REFRESH VIEW exports.card_search_doc_hits_daily_parquet_mv;
SYSTEM REFRESH VIEW exports.qs_doc_opens_daily_parquet_mv;
SQL
