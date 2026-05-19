from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from elasticsearch import Elasticsearch, helpers

from src.config import settings


class ElasticStorage:
    def __init__(self, url: str | None = None):
        self.client = Elasticsearch(url or settings.elasticsearch_url, request_timeout=30)

    def health(self) -> bool:
        try:
            return bool(self.client.ping())
        except Exception:
            return False

    def ensure_indices(self) -> None:
        for name, body in index_templates().items():
            if not self.client.indices.exists(index=name):
                self.client.indices.create(index=name, body=body)

    def index_document(self, index: str, document: dict[str, Any], doc_id: str | None = None) -> None:
        self.client.index(index=index, id=doc_id, document=document, refresh=False)

    def bulk_index(self, index: str, documents: list[dict[str, Any]], id_field: str | None = None) -> None:
        if not documents:
            return
        actions = []
        for doc in documents:
            action = {"_op_type": "index", "_index": index, "_source": doc}
            if id_field and doc.get(id_field):
                action["_id"] = doc[id_field]
            actions.append(action)
        helpers.bulk(self.client, actions, refresh=False)

    def update_document(self, index: str, doc_id: str, partial: dict[str, Any]) -> None:
        self.client.update(index=index, id=doc_id, doc=partial, refresh=False)

    def count(self, index: str, query: dict[str, Any] | None = None) -> int:
        payload = {"query": query or {"match_all": {}}}
        return int(self.client.count(index=index, body=payload)["count"])

    def search(
        self,
        index: str,
        query: dict[str, Any] | None = None,
        size: int = 100,
        sort: list[dict[str, str]] | None = None,
    ) -> list[dict[str, Any]]:
        body = {"query": query or {"match_all": {}}, "size": size}
        if sort:
            body["sort"] = sort
        resp = self.client.search(index=index, body=body)
        return [hit["_source"] | {"_id": hit["_id"]} for hit in resp["hits"]["hits"]]

    def aggregate(self, index: str, body: dict[str, Any]) -> dict[str, Any]:
        resp = self.client.search(index=index, body=body)
        return dict(resp)

    def fetch_recent_logs(self, hours: int = 24, size: int = 5000) -> list[dict[str, Any]]:
        return self.fetch_recent_logs_by_field(hours=hours, size=size, time_field="event_time")

    def fetch_recent_logs_by_field(
        self,
        hours: int = 24,
        size: int = 5000,
        time_field: str = "event_time",
    ) -> list[dict[str, Any]]:
        end = datetime.utcnow()
        start = end - timedelta(hours=hours)
        query = {
            "range": {
                time_field: {
                    "gte": start.isoformat(),
                    "lte": end.isoformat(),
                }
            }
        }
        return self.search(
            index=settings.elasticsearch_log_index,
            query=query,
            size=size,
            sort=[{time_field: "desc"}],
        )


def index_templates() -> dict[str, dict[str, Any]]:
    return {
        settings.elasticsearch_log_index: {
            "mappings": {
                "properties": {
                    "event_id": {"type": "keyword"},
                    "event_time": {"type": "date"},
                    "ingest_time": {"type": "date"},
                    "source_type": {"type": "keyword"},
                    "username": {"type": "keyword"},
                    "src_ip": {"type": "ip", "ignore_malformed": True},
                    "src_port": {"type": "integer"},
                    "dst_ip": {"type": "ip", "ignore_malformed": True},
                    "dst_port": {"type": "integer"},
                    "action": {"type": "keyword"},
                    "resource": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "http_method": {"type": "keyword"},
                    "user_agent": {"type": "keyword"},
                    "message": {"type": "text"},
                    "raw_message": {"type": "text"},
                    "risk_tags": {"type": "keyword"},
                    "trace_id": {"type": "keyword"},
                    "original_fields": {"type": "object", "enabled": True},
                }
            }
        },
        settings.elasticsearch_alert_index: {
            "mappings": {
                "properties": {
                    "alert_id": {"type": "keyword"},
                    "event_time": {"type": "date"},
                    "detect_time": {"type": "date"},
                    "username": {"type": "keyword"},
                    "src_ip": {"type": "ip", "ignore_malformed": True},
                    "source_type": {"type": "keyword"},
                    "risk_level": {"type": "keyword"},
                    "risk_score": {"type": "integer"},
                    "rule_hits": {"type": "keyword"},
                    "evidence": {"type": "object", "enabled": True},
                    "related_event_ids": {"type": "keyword"},
                    "related_logs_summary": {"type": "text"},
                    "status": {"type": "keyword"},
                    "llm_analysis_id": {"type": "keyword"},
                    "alert_type": {"type": "keyword"},
                    "ueba_score": {"type": "integer"},
                    "baseline_id": {"type": "keyword"},
                    "baseline_confidence": {"type": "float"},
                    "deviation_features": {"type": "object", "enabled": True},
                    "anomaly_reasons": {"type": "keyword"},
                }
            }
        },
        settings.elasticsearch_ai_index: {
            "mappings": {
                "properties": {
                    "ai_report_id": {"type": "keyword"},
                    "alert_id": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "attack_type": {"type": "keyword"},
                    "risk_level": {"type": "keyword"},
                    "reason": {"type": "text"},
                    "suggestion": {"type": "text"},
                    "confidence": {"type": "float"},
                    "next_steps": {"type": "keyword"},
                    "raw_response": {"type": "object", "enabled": True},
                }
            }
        },
        settings.elasticsearch_daily_index: {
            "mappings": {
                "properties": {
                    "report_id": {"type": "keyword"},
                    "date": {"type": "date", "format": "yyyy-MM-dd"},
                    "created_at": {"type": "date"},
                    "overall_score": {"type": "float"},
                    "log_count": {"type": "integer"},
                    "alert_count": {"type": "integer"},
                    "high_risk_count": {"type": "integer"},
                    "major_risks": {"type": "keyword"},
                    "high_risk_users": {"type": "keyword"},
                    "typical_alerts": {"type": "object", "enabled": True},
                    "ai_summary": {"type": "text"},
                    "recommendation": {"type": "text"},
                    "markdown": {"type": "text"},
                }
            }
        },
        settings.elasticsearch_baseline_index: {
            "mappings": {
                "properties": {
                    "username": {"type": "keyword"},
                    "active_hours": {"type": "keyword"},
                    "common_ips": {"type": "keyword"},
                    "common_user_agents": {"type": "keyword"},
                    "avg_api_calls_per_minute": {"type": "float"},
                    "common_resources": {"type": "keyword"},
                    "failed_login_count_7d": {"type": "integer"},
                    "sensitive_access_rate": {"type": "float"},
                    "updated_at": {"type": "date"},
                }
            }
        },
    }
