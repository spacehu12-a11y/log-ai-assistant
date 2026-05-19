from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.config import settings
from src.schemas import AlertEvent, NormalizedLog
from src.storage.elastic_client import ElasticStorage


@dataclass
class UebaResult:
    ueba_score: int
    risk_level: str
    deviation_features: dict[str, int]
    anomaly_reasons: list[str]
    evidence: dict[str, Any]


class UebaScorer:
    def __init__(self, storage: ElasticStorage):
        self.storage = storage
        self._baseline_cache: dict[str, dict[str, Any] | None] = {}

    def evaluate_log(self, log: NormalizedLog) -> list[AlertEvent]:
        if log.source_type != "vpn":
            return []

        if not log.username:
            return []

        if log.action != "login":
            return []

        baseline = self._get_baseline(log.username)

        if not baseline:
            return []

        result = self.score(log, baseline)

        if result.ueba_score < 40:
            return []

        return [self._build_alert(log, baseline, result)]

    def score(self, log: NormalizedLog, baseline: dict[str, Any]) -> UebaResult:
        original = self._original_fields(log)

        time_score, time_reasons = self._score_time(log, baseline)
        ip_score, ip_reasons = self._score_ip(log, baseline)
        geo_score, geo_reasons = self._score_geo(original, baseline)
        access_score, access_reasons = self._score_access(original, log, baseline)
        volume_score, volume_reasons = self._score_volume(original, baseline)
        result_score, result_reasons = self._score_result(log, baseline)

        deviation_features = {
            "time_score": time_score,
            "ip_score": ip_score,
            "geo_score": geo_score,
            "access_score": access_score,
            "volume_score": volume_score,
            "result_score": result_score,
        }

        weighted = (
            time_score * 0.15
            + ip_score * 0.25
            + geo_score * 0.15
            + access_score * 0.10
            + volume_score * 0.20
            + result_score * 0.15
        )

        ueba_score = min(100, int(round(weighted)))

        reasons = (
            time_reasons
            + ip_reasons
            + geo_reasons
            + access_reasons
            + volume_reasons
            + result_reasons
        )

        risk_level = self._risk_level(ueba_score)

        evidence = {
            "username": log.username,
            "baseline_version": baseline.get("baseline_version", "ueba-v1"),
            "baseline_confidence": baseline.get("baseline_confidence", 0.0),
            "sample_count": baseline.get("sample_count", 0),
            "deviation_features": deviation_features,
            "anomaly_reasons": reasons,
            "current_behavior": {
                "event_time": log.event_time.isoformat(),
                "src_ip": log.src_ip,
                "country": original.get("src_country"),
                "city": original.get("src_city"),
                "gateway": original.get("vpn_gateway"),
                "protocol": original.get("proto") or original.get("protocol"),
                "auth_method": original.get("auth") or original.get("auth_method"),
                "client": original.get("client") or log.user_agent,
                "status": log.status,
                "bytes_recv": original.get("bytes_recv"),
            },
        }

        return UebaResult(
            ueba_score=ueba_score,
            risk_level=risk_level,
            deviation_features=deviation_features,
            anomaly_reasons=reasons,
            evidence=evidence,
        )

    def _get_baseline(self, username: str) -> dict[str, Any] | None:
        if username in self._baseline_cache:
            return self._baseline_cache[username]

        rows = self.storage.search(
            index=settings.elasticsearch_baseline_index,
            query={"term": {"username": username}},
            size=1,
        )

        baseline = rows[0] if rows else None
        self._baseline_cache[username] = baseline
        return baseline

    @staticmethod
    def _original_fields(log: NormalizedLog) -> dict[str, Any]:
        data = log.model_dump(mode="json")
        original = data.get("original_fields")
        if isinstance(original, dict):
            return original
        return {}

    @staticmethod
    def _ip_prefix(ip: str | None) -> str | None:
        if not ip:
            return None

        parts = str(ip).split(".")
        if len(parts) >= 3:
            return ".".join(parts[:3])

        return ip

    @staticmethod
    def _prob_score(prob: float) -> int:
        if prob >= 0.20:
            return 0
        if prob >= 0.10:
            return 20
        if prob >= 0.03:
            return 45
        if prob > 0:
            return 65
        return 80

    def _score_time(self, log: NormalizedLog, baseline: dict[str, Any]) -> tuple[int, list[str]]:
        profile = baseline.get("time_profile") or {}
        hour_hist = profile.get("hour_hist") or {}
        weekday_hist = profile.get("weekday_hist") or {}

        hour = str(log.event_time.hour)
        weekday = str(log.event_time.weekday())

        hour_prob = float(hour_hist.get(hour, 0))
        weekday_prob = float(weekday_hist.get(weekday, 0))

        hour_score = self._prob_score(hour_prob)
        weekday_score = self._prob_score(weekday_prob)

        score = min(100, int(hour_score * 0.7 + weekday_score * 0.3))

        reasons: list[str] = []

        if hour_score >= 65:
            reasons.append(f"登录小时 {hour}:00 不符合该用户常用登录时段")

        if weekday_score >= 65:
            reasons.append(f"登录星期 {weekday} 不符合该用户常用登录星期分布")

        return score, reasons

    def _score_ip(self, log: NormalizedLog, baseline: dict[str, Any]) -> tuple[int, list[str]]:
        profile = baseline.get("location_profile") or {}
        prefixes = profile.get("ip_prefixes") or {}

        prefix = self._ip_prefix(log.src_ip)
        prob = float(prefixes.get(prefix, 0)) if prefix else 0.0

        score = self._prob_score(prob)
        reasons: list[str] = []

        if score >= 65:
            common = list(prefixes.keys())[:5]
            reasons.append(f"来源 IP 前缀 {prefix} 偏离用户历史常用前缀 {common}")

        return score, reasons

    def _score_geo(self, original: dict[str, Any], baseline: dict[str, Any]) -> tuple[int, list[str]]:
        profile = baseline.get("location_profile") or {}
        countries = profile.get("countries") or {}
        cities = profile.get("cities") or {}

        country = original.get("src_country")
        city = original.get("src_city")

        country_prob = float(countries.get(country, 0)) if country else 0.0
        city_prob = float(cities.get(city, 0)) if city else 0.0

        country_score = self._prob_score(country_prob)
        city_score = self._prob_score(city_prob)

        if country and country not in {"中国", "内网"}:
            country_score = max(country_score, 90)

        score = min(100, int(country_score * 0.65 + city_score * 0.35))

        reasons: list[str] = []

        if country_score >= 70:
            reasons.append(f"来源国家 {country} 偏离用户历史地理位置分布")

        if city_score >= 65:
            reasons.append(f"来源城市 {city} 偏离用户历史城市分布")

        return score, reasons

    def _score_access(
        self,
        original: dict[str, Any],
        log: NormalizedLog,
        baseline: dict[str, Any],
    ) -> tuple[int, list[str]]:
        profile = baseline.get("access_profile") or {}

        gateway = original.get("vpn_gateway")
        protocol = original.get("proto") or original.get("protocol")
        auth = original.get("auth") or original.get("auth_method")
        client = original.get("client") or log.user_agent

        checks = [
            ("VPN网关", gateway, profile.get("gateways") or {}),
            ("协议", protocol, profile.get("protocols") or {}),
            ("认证方式", auth, profile.get("auth_methods") or {}),
            ("客户端", client, profile.get("clients") or {}),
        ]

        scores: list[int] = []
        reasons: list[str] = []

        for name, value, dist in checks:
            if not value:
                continue

            prob = float(dist.get(value, 0))
            item_score = self._prob_score(prob)
            scores.append(item_score)

            if item_score >= 65:
                reasons.append(f"{name} {value} 偏离用户历史访问方式")

        if not scores:
            return 0, []

        score = min(100, int(sum(scores) / len(scores)))
        return score, reasons

    def _score_volume(self, original: dict[str, Any], baseline: dict[str, Any]) -> tuple[int, list[str]]:
        profile = baseline.get("volume_profile") or {}

        current = self._safe_float(original.get("bytes_recv"))
        if current is None:
            return 0, []

        median = self._safe_float(profile.get("bytes_recv_median")) or 0.0
        iqr = self._safe_float(profile.get("bytes_recv_iqr")) or 0.0
        p95 = self._safe_float(profile.get("bytes_recv_p95")) or 0.0

        if median <= 0 and p95 <= 0:
            return 0, []

        reasons: list[str] = []

        if p95 > 0 and current > p95:
            score = 55
            reasons.append(f"下载流量 {int(current)} 超过用户历史 P95={int(p95)}")
        else:
            score = 0

        if iqr > 0:
            z = (current - median) / iqr

            if z >= 5:
                score = max(score, 90)
                reasons.append(f"下载流量超过用户历史中位数约 {z:.1f} 个 IQR")
            elif z >= 3:
                score = max(score, 70)
                reasons.append(f"下载流量超过用户历史中位数约 {z:.1f} 个 IQR")

        return min(100, score), reasons

    def _score_result(self, log: NormalizedLog, baseline: dict[str, Any]) -> tuple[int, list[str]]:
        if log.status != "failed":
            return 0, []

        profile = baseline.get("result_profile") or {}
        failure_rate = float(profile.get("failure_rate", 0.0))

        if failure_rate <= 0.02:
            return 60, ["该用户历史失败率很低，本次出现登录失败"]

        if failure_rate <= 0.10:
            return 40, ["本次登录失败，需结合其他偏离特征判断"]

        return 25, ["本次登录失败"]

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value is None:
            return None

        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _risk_level(score: int) -> str:
        if score >= 75:
            return "高"
        if score >= 40:
            return "中"
        return "低"

    def _build_alert(
        self,
        log: NormalizedLog,
        baseline: dict[str, Any],
        result: UebaResult,
    ) -> AlertEvent:
        summary = (
            f"user={log.username or 'unknown'} src_ip={log.src_ip or 'unknown'} "
            f"action={log.action} status={log.status} "
            f"ueba_score={result.ueba_score}"
        )

        payload = {
            "alert_id": str(uuid.uuid4()),
            "event_time": log.event_time,
            "detect_time": datetime.now(timezone.utc),
            "username": log.username,
            "src_ip": log.src_ip,
            "source_type": log.source_type,
            "risk_level": result.risk_level,
            "risk_score": result.ueba_score,
            "rule_hits": ["UEBA用户行为偏离检测"],
            "evidence": result.evidence,
            "related_event_ids": [log.event_id],
            "related_logs_summary": summary,
            "status": "new",
            "llm_analysis_id": None,
            "alert_type": "ueba_anomaly",
            "ueba_score": result.ueba_score,
            "baseline_id": f"{log.username}:{baseline.get('baseline_version', 'ueba-v1')}",
            "baseline_confidence": baseline.get("baseline_confidence", 0.0),
            "deviation_features": result.deviation_features,
            "anomaly_reasons": result.anomaly_reasons,
        }

        return AlertEvent.model_validate(payload)