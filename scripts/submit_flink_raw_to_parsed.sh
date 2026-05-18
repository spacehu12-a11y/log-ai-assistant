#!/usr/bin/env bash
set -euo pipefail

JOBMANAGER_CONTAINER="${JOBMANAGER_CONTAINER:-flink-jobmanager}"
PROJECT_DIR_IN_CONTAINER="${PROJECT_DIR_IN_CONTAINER:-/opt/log-ai-assistant}"
KAFKA_CONNECTOR_JAR="${KAFKA_CONNECTOR_JAR:-/opt/flink/usrlib/flink-sql-connector-kafka-3.1.0-1.18.jar}"

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-kafka:9092}"
RAW_TOPIC="${RAW_TOPIC:-raw_logs}"
PARSED_TOPIC="${PARSED_TOPIC:-parsed_logs}"
GROUP_ID="${GROUP_ID:-flink-raw-to-parsed}"
STARTING_OFFSETS="${STARTING_OFFSETS:-latest}"
PARALLELISM="${PARALLELISM:-1}"
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-10000}"

echo "=== Submit Docker Flink job: raw_logs -> parsed_logs ==="
echo "JobManager container : ${JOBMANAGER_CONTAINER}"
echo "Project dir          : ${PROJECT_DIR_IN_CONTAINER}"
echo "Kafka connector jar  : ${KAFKA_CONNECTOR_JAR}"
echo "Bootstrap servers    : ${BOOTSTRAP_SERVERS}"
echo "Raw topic            : ${RAW_TOPIC}"
echo "Parsed topic         : ${PARSED_TOPIC}"
echo "Group id             : ${GROUP_ID}"
echo "Starting offsets     : ${STARTING_OFFSETS}"
echo "Parallelism          : ${PARALLELISM}"
echo

docker exec "${JOBMANAGER_CONTAINER}" test -f "${KAFKA_CONNECTOR_JAR}"

docker exec "${JOBMANAGER_CONTAINER}" flink run -d \
  -C "file://${KAFKA_CONNECTOR_JAR}" \
  -py "${PROJECT_DIR_IN_CONTAINER}/flink_jobs/raw_to_parsed.py" \
  --bootstrap-servers "${BOOTSTRAP_SERVERS}" \
  --raw-topic "${RAW_TOPIC}" \
  --parsed-topic "${PARSED_TOPIC}" \
  --group-id "${GROUP_ID}" \
  --starting-offsets "${STARTING_OFFSETS}" \
  --parallelism "${PARALLELISM}" \
  --checkpoint-interval-ms "${CHECKPOINT_INTERVAL_MS}"

echo
echo "=== Submitted. Check Flink UI: http://localhost:8081 ==="