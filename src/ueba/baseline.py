from __future__ import annotations

import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import settings
from src.schemas import UserBaseline
from src.storage.elastic_client import ElasticStorage


BASELINE_VERSION = "ueba-v1"


def _to_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass

        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    return datetime.now(timezone.utc)


def _ip_prefix(ip: str | None) -> str | None:
    if not ip:
        return None

    parts = str(ip).split(".")
    if len(parts) >= 3:
        return ".".join(parts[:3])

    return ip


def _counter_ratio(counter: Counter[str]) -> dict[str, float]:
    total = sum(counter.values())
    if total <= 0:
        return {}

    return {
        key: round(value / total, 4)
        for key, value in counter.most_common()
    }


def _safe_num(value: Any) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0

    values = sorted(values)
    if len(values) == 1:
        return float(values[0])

    pos = (len(values) - 1) * q
    low = int(pos)
    high = min(low + 1, len(values) - 1)

    if low == high:
        return float(values[low])

    return float(values[low] * (high - pos) + values[high] * (pos - low))


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(statistics.median(values))


def _iqr(values: list[float]) -> float:
    if len(values) < 4:
        return 0.0
    return max(0.0, _quantile(values, 0.75) - _quantile(values, 0.25))


def _confidence(sample_count: int) -> float:
    if sample_count <= 0:
        return 0.0
    if sample_count >= 100:
        return 1.0
    return round(sample_count / 100, 2)


def _top_keys(counter: Counter[str], n: int) -> list[str]:
    return [key for key, _ in counter.most_common(n)]


def _active_hour_range(hour_counter: Counter[str]) -> list[str]:
    if not hour_counter:
        return []

    top_hours = sorted(int(hour) for hour, _ in hour_counter.most_common(4))
    if not top_hours:
        return []

    return [f"{top_hours[0]:02d}:00-{(top_hours[-1] + 1) % 24:02d}:00"]


def _get_original(log: dict[str, Any]) -> dict[str, Any]:
    original = log.get("original_fields")
    if isinstance(original, dict):
        return original
    return {}


def _is_usable_for_baseline(log: dict[str, Any]) -> bool:
    if log.get("source_type") != "vpn":
        return False

    if log.get("action") != "login":
        return False

    # 基线尽量使用成功登录，避免把失败爆破、异常尝试写进正常画像。
    if log.get("status") != "success":
        return False

    risk_tags = log.get("risk_tags") or []
    if isinstance(risk_tags, list) and risk_tags:
        return False

    return True


def build_baselines_from_logs(logs: list[dict[str, Any]]) -> list[UserBaseline]:
    by_user: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for log in logs:
        username = log.get("username")
        if username and _is_usable_for_baseline(log):
            by_user[str(username)].append(log)

    baselines: list[UserBaseline] = []

    for username, user_logs in by_user.items():
        hour_counter: Counter[str] = Counter()
        weekday_counter: Counter[str] = Counter()

        ip_prefix_counter: Counter[str] = Counter()
        country_counter: Counter[str] = Counter()
        city_counter: Counter[str] = Counter()

        gateway_counter: Counter[str] = Counter()
        protocol_counter: Counter[str] = Counter()
        auth_counter: Counter[str] = Counter()
        client_counter: Counter[str] = Counter()

        durations: list[float] = []
        bytes_recv_values: list[float] = []
        bytes_sent_values: list[float] = []

        success_count = 0
        failure_count = 0

        dept = None
        role = None
        event_times: list[datetime] = []

        for log in user_logs:
            original = _get_original(log)
            event_time = _to_dt(log.get("event_time"))

            event_times.append(event_time)
            hour_counter[str(event_time.hour)] += 1
            weekday_counter[str(event_time.weekday())] += 1

            dept = dept or original.get("dept")
            role = role or original.get("role")

            prefix = _ip_prefix(log.get("src_ip"))
            if prefix:
                ip_prefix_counter[prefix] += 1

            country = original.get("src_country")
            city = original.get("src_city")
            gateway = original.get("vpn_gateway")
            protocol = original.get("proto") or original.get("protocol")
            auth = original.get("auth") or original.get("auth_method")
            client = original.get("client") or log.get("user_agent")

            if country:
                country_counter[str(country)] += 1
            if city:
                city_counter[str(city)] += 1
            if gateway:
                gateway_counter[str(gateway)] += 1
            if protocol:
                protocol_counter[str(protocol)] += 1
            if auth:
                auth_counter[str(auth)] += 1
            if client:
                client_counter[str(client)] += 1

            duration = _safe_num(original.get("session_duration_sec"))
            if duration is None:
                duration = _safe_num(original.get("duration"))

            if duration is not None:
                durations.append(duration)

            recv = _safe_num(original.get("bytes_recv"))
            sent = _safe_num(original.get("bytes_sent"))

            if recv is not None:
                bytes_recv_values.append(recv)
            if sent is not None:
                bytes_sent_values.append(sent)

            if log.get("status") == "success":
                success_count += 1
            elif log.get("status") == "failed":
                failure_count += 1

        sample_count = len(user_logs)
        total_result = success_count + failure_count

        time_profile = {
            "hour_hist": _counter_ratio(hour_counter),
            "weekday_hist": _counter_ratio(weekday_counter),
            "usual_hour_range": _active_hour_range(hour_counter),
        }

        location_profile = {
            "ip_prefixes": _counter_ratio(ip_prefix_counter),
            "countries": _counter_ratio(country_counter),
            "cities": _counter_ratio(city_counter),
        }

        access_profile = {
            "gateways": _counter_ratio(gateway_counter),
            "protocols": _counter_ratio(protocol_counter),
            "auth_methods": _counter_ratio(auth_counter),
            "clients": _counter_ratio(client_counter),
        }

        volume_profile = {
            "session_duration_median": round(_median(durations), 2),
            "session_duration_iqr": round(_iqr(durations), 2),
            "bytes_recv_median": round(_median(bytes_recv_values), 2),
            "bytes_recv_iqr": round(_iqr(bytes_recv_values), 2),
            "bytes_recv_p95": round(_quantile(bytes_recv_values, 0.95), 2),
            "bytes_sent_median": round(_median(bytes_sent_values), 2),
            "bytes_sent_iqr": round(_iqr(bytes_sent_values), 2),
        }

        result_profile = {
            "success_rate": round(success_count / total_result, 4) if total_result else 0.0,
            "failure_rate": round(failure_count / total_result, 4) if total_result else 0.0,
            "success_count": success_count,
            "failure_count": failure_count,
        }

        baseline = UserBaseline(
            username=username,
            dept=dept,
            role=role,
            baseline_version=BASELINE_VERSION,
            sample_count=sample_count,
            baseline_confidence=_confidence(sample_count),
            trained_from=min(event_times) if event_times else None,
            trained_to=max(event_times) if event_times else None,
            updated_at=datetime.now(timezone.utc),
            time_profile=time_profile,
            location_profile=location_profile,
            access_profile=access_profile,
            volume_profile=volume_profile,
            result_profile=result_profile,
            active_hours=time_profile["usual_hour_range"],
            common_ips=_top_keys(ip_prefix_counter, 5),
            common_user_agents=_top_keys(client_counter, 3),
            common_resources=_top_keys(gateway_counter, 5),
            failed_login_count_7d=failure_count,
            sensitive_access_rate=0.0,
        )

        baselines.append(baseline)

    return baselines


def build_and_store_baselines(
    storage: ElasticStorage,
    output_path: Path | None = None,
) -> list[UserBaseline]:
    logs = storage.fetch_recent_logs_by_field(
        hours=24 * 7,
        size=10000,
        time_field="ingest_time",
    )

    if not logs:
        logs = storage.search(
            index=settings.elasticsearch_log_index,
            query={"match_all": {}},
            size=10000,
            sort=[{"ingest_time": "desc"}],
        )

    baselines = build_baselines_from_logs(logs)
    docs = [item.model_dump(mode="json") for item in baselines]

    storage.bulk_index(
        index=settings.elasticsearch_baseline_index,
        documents=docs,
        id_field="username",
    )

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(docs, f, ensure_ascii=False, indent=2)

    return baselines