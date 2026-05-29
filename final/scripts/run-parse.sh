set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

docker compose exec -T spark-job spark-submit \
  --driver-memory 3g \
  --class ru.consultant.lakehouse.jobs.ParseJob \
  /opt/spark/jars/spark-jobs.jar
