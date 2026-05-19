#!/usr/bin/env bash
set -euo pipefail

echo "========== Docker services =========="
docker compose ps

echo
echo "========== Flink jobs =========="
docker exec flink-jobmanager flink list -a || true

echo
echo "========== Kafka topics =========="
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

echo
echo "========== Sample raw_logs =========="
timeout 10 docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic raw_logs \
  --max-messages 2 || true

echo
echo "========== Sample parsed_logs =========="
timeout 10 docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic parsed_logs \
  --max-messages 2 || true

echo
echo "========== Elasticsearch indices =========="
curl -s "http://localhost:9200/_cat/indices?v" || true

echo
echo "========== Elasticsearch count now =========="
curl -s "http://localhost:9200/_cat/count?v" || true

echo
echo "========== Wait 10 seconds =========="
sleep 10

echo
echo "========== Elasticsearch count after 10 seconds =========="
curl -s "http://localhost:9200/_cat/count?v" || true

echo
echo "========== Sample alert_events =========="
timeout 10 docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic alert_events \
  --from-beginning \
  --max-messages 3 || true

echo
echo "========== Verify finished =========="