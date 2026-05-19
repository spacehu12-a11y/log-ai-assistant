from __future__ import annotations

import json
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from src.config import settings
from src.schemas import AlertEvent, NormalizedLog
from src.storage.elastic_client import ElasticStorage
from src.ueba import UebaScorer


class KafkaToElasticConsumer:
    def __init__(
        self,
        consumer_timeout_ms: int = 5000,
        group_id: str = "log-ai-consume-to-es",
    ):
        self.storage = ElasticStorage()
        self.ueba_scorer = UebaScorer(self.storage)

        self.consumer = KafkaConsumer(
            settings.kafka_parsed_topic,
            settings.kafka_alert_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=consumer_timeout_ms,
        )

        self.alert_producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            linger_ms=10,
        )

    def run(self, max_messages: int | None = None) -> int:
        self.storage.ensure_indices()

        consumed = 0

        for message in self.consumer:
            topic = message.topic
            payload = message.value

            if topic == settings.kafka_parsed_topic:
                consumed += self._handle_parsed(payload)

            elif topic == settings.kafka_alert_topic:
                consumed += self._handle_alert(payload)

            if max_messages is not None and consumed >= max_messages:
                break

        self.alert_producer.flush()
        self.alert_producer.close()
        self.consumer.close()

        return consumed

    def _handle_parsed(self, payload: dict) -> int:
        log = NormalizedLog.model_validate(payload)
        doc = log.model_dump(mode="json")

        self.storage.index_document(
            settings.elasticsearch_log_index,
            doc,
            doc_id=log.event_id,
        )

        generated = self.ueba_scorer.evaluate_log(log)

        for alert in generated:
            alert_doc = alert.model_dump(mode="json")

            self.storage.index_document(
                settings.elasticsearch_alert_index,
                alert_doc,
                doc_id=alert.alert_id,
            )

            self.alert_producer.send(settings.kafka_alert_topic, alert_doc)

        return 1 + len(generated)

    def _handle_alert(self, payload: dict) -> int:
        if "detect_time" not in payload:
            payload["detect_time"] = datetime.now(timezone.utc).isoformat()

        alert = AlertEvent.model_validate(payload)

        self.storage.index_document(
            settings.elasticsearch_alert_index,
            alert.model_dump(mode="json"),
            doc_id=alert.alert_id,
        )

        return 1