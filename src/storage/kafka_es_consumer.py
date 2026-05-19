from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer, KafkaProducer

from src.config import settings
from src.schemas import AlertEvent, NormalizedLog
from src.storage.elastic_client import ElasticStorage
from src.ueba import UebaScorer


logger = logging.getLogger(__name__)

# Keep a single schema version for all messages handled by this consumer.
# This helps upstream/downstream compatibility and later evolution.
SCHEMA_VERSION = "1.0"


def _identity(value: Any) -> Any:
    """Return Kafka key/value as raw payload without library-side conversion."""
    return value


class KafkaToElasticConsumer:
    def __init__(
        self,
        consumer_timeout_ms: int = 5000,
        group_id: str = "log-ai-consume-to-es",
    ):
        self._consumer_timeout_ms = consumer_timeout_ms
        self._group_id = group_id

        self.storage = ElasticStorage()
        self.ueba_scorer = UebaScorer(self.storage)

        self.consumer = self._create_consumer()
        self.alert_producer = self._create_producer()

    def _create_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            settings.kafka_parsed_topic,
            settings.kafka_alert_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=self._group_id,
            key_deserializer=_identity,
            value_deserializer=_identity,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=self._consumer_timeout_ms,
        )

    def _create_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            linger_ms=10,
        )

    def run(self, max_messages: int | None = None) -> int:
        self.storage.ensure_indices()

        total_consumed = 0
        restart_delay = 5

        while True:
            try:
                total_consumed += self._run_once(max_messages)
                break
            except Exception:
                logger.exception(
                    "Kafka consumer loop crashed; restarting in %s seconds",
                    restart_delay,
                )
                self._close_resources()
                time.sleep(restart_delay)
                self._reinit_consumer()

        return total_consumed

    def _run_once(self, max_messages: int | None) -> int:
        consumed = 0

        try:
            for message in self.consumer:
                try:
                    topic = getattr(message, "topic", None)
                    payload = self._decode_payload(getattr(message, "value", None))

                    if not payload:
                        logger.warning(
                            "Skip empty/invalid Kafka payload topic=%s partition=%s offset=%s",
                            topic,
                            getattr(message, "partition", "unknown"),
                            getattr(message, "offset", "unknown"),
                        )
                        continue

                    if topic == settings.kafka_parsed_topic:
                        consumed += self._handle_parsed(payload)

                    elif topic == settings.kafka_alert_topic:
                        consumed += self._handle_alert(payload)

                    else:
                        logger.warning(
                            "Skip message from unexpected topic=%s partition=%s offset=%s",
                            topic,
                            getattr(message, "partition", "unknown"),
                            getattr(message, "offset", "unknown"),
                        )

                    if max_messages is not None and consumed >= max_messages:
                        break

                except Exception:
                    logger.exception(
                        "Failed to process Kafka message topic=%s partition=%s offset=%s",
                        getattr(message, "topic", "unknown"),
                        getattr(message, "partition", "unknown"),
                        getattr(message, "offset", "unknown"),
                    )
                    continue

        finally:
            self._close_resources()

        return consumed

    def _close_resources(self) -> None:
        try:
            self.alert_producer.flush()
        except Exception:
            logger.exception("Failed to flush alert producer")

        try:
            self.alert_producer.close()
        except Exception:
            logger.exception("Failed to close alert producer")

        try:
            self.consumer.close()
        except Exception:
            logger.exception("Failed to close Kafka consumer")

    def _reinit_consumer(self) -> None:
        self.consumer = self._create_consumer()
        self.alert_producer = self._create_producer()

    @staticmethod
    def _decode_payload(raw_value: Any) -> dict[str, Any]:
        """
        Decode Kafka raw value into a dictionary.

        Supported inputs:
        - bytes / bytearray / memoryview
        - str
        - dict
        """
        if raw_value is None:
            return {}

        if isinstance(raw_value, dict):
            payload = raw_value
        elif isinstance(raw_value, (bytes, bytearray, memoryview)):
            raw_bytes = bytes(raw_value)
            if not raw_bytes:
                return {}
            raw_text = raw_bytes.decode("utf-8", errors="replace").strip()
            if not raw_text:
                return {}
            try:
                parsed = json.loads(raw_text)
            except Exception:
                logger.exception("Failed to decode Kafka JSON payload: %r", raw_text[:500])
                return {}
            if isinstance(parsed, dict):
                payload = parsed
            else:
                payload = {"_payload": parsed}
        elif isinstance(raw_value, str):
            raw_text = raw_value.strip()
            if not raw_text:
                return {}
            try:
                parsed = json.loads(raw_text)
            except Exception:
                logger.exception("Failed to decode Kafka JSON payload: %r", raw_text[:500])
                return {}
            if isinstance(parsed, dict):
                payload = parsed
            else:
                payload = {"_payload": parsed}
        else:
            raw_text = str(raw_value).strip()
            if not raw_text:
                return {}
            try:
                parsed = json.loads(raw_text)
            except Exception:
                logger.exception("Failed to decode Kafka JSON payload: %r", raw_text[:500])
                return {}
            if isinstance(parsed, dict):
                payload = parsed
            else:
                payload = {"_payload": parsed}

        # Normalization for schema evolution:
        # ensure every payload carries schema_version if it is missing.
        if isinstance(payload, dict) and not payload.get("schema_version"):
            payload = dict(payload)
            payload["schema_version"] = SCHEMA_VERSION

        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _attach_schema_version(doc: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(doc)
        enriched.setdefault("schema_version", SCHEMA_VERSION)
        return enriched

    def _handle_parsed(self, payload: dict[str, Any]) -> int:
        try:
            log = NormalizedLog.model_validate(payload)
        except Exception:
            logger.exception("Invalid parsed log payload: %s", payload)
            return 0

        doc = self._attach_schema_version(log.model_dump(mode="json"))

        try:
            self.storage.index_document(
                settings.elasticsearch_log_index,
                doc,
                doc_id=log.event_id,
            )
        except Exception:
            logger.exception("Failed to index parsed log into Elasticsearch: event_id=%s", log.event_id)
            return 0

        generated = self.ueba_scorer.evaluate_log(log)

        for alert in generated:
            alert_doc = self._attach_schema_version(alert.model_dump(mode="json"))

            try:
                self.storage.index_document(
                    settings.elasticsearch_alert_index,
                    alert_doc,
                    doc_id=alert.alert_id,
                )
            except Exception:
                logger.exception("Failed to index UEBA alert into Elasticsearch: alert_id=%s", alert.alert_id)
                continue

            try:
                self.alert_producer.send(settings.kafka_alert_topic, alert_doc)
            except Exception:
                logger.exception("Failed to publish alert to Kafka topic=%s", settings.kafka_alert_topic)

        return 1 + len(generated)

    def _handle_alert(self, payload: dict[str, Any]) -> int:
        if "detect_time" not in payload:
            payload["detect_time"] = datetime.now(timezone.utc).isoformat()

        try:
            alert = AlertEvent.model_validate(payload)
        except Exception:
            logger.exception("Invalid alert payload: %s", payload)
            return 0

        alert_doc = self._attach_schema_version(alert.model_dump(mode="json"))

        try:
            self.storage.index_document(
                settings.elasticsearch_alert_index,
                alert_doc,
                doc_id=alert.alert_id,
            )
        except Exception:
            logger.exception("Failed to index alert into Elasticsearch: alert_id=%s", alert.alert_id)
            return 0

        return 1


