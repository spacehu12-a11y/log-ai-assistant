from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


RiskLevel = Literal["低", "中", "高"]
SourceType = Literal["vpn", "oa", "api", "system", "security_device"]


class NormalizedLog(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_id: str
    event_time: datetime
    ingest_time: datetime
    source_type: SourceType = "vpn"

    username: str | None = None
    src_ip: str | None = None
    src_port: int | None = None
    dst_ip: str | None = None
    dst_port: int | None = None

    action: str
    resource: str | None = None
    status: str

    http_method: str | None = None
    user_agent: str | None = None

    message: str
    raw_message: str
    risk_tags: list[str] = Field(default_factory=list)
    trace_id: str | None = None


class AlertEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    alert_id: str
    event_time: datetime
    detect_time: datetime

    username: str | None = None
    src_ip: str | None = None
    source_type: SourceType = "vpn"

    risk_level: RiskLevel
    risk_score: int
    rule_hits: list[str]
    evidence: dict[str, Any]

    related_event_ids: list[str] = Field(default_factory=list)
    related_logs_summary: str = ""

    status: str = "new"
    llm_analysis_id: str | None = None


class UserBaseline(BaseModel):
    model_config = ConfigDict(extra="allow")

    username: str
    dept: str | None = None
    role: str | None = None

    baseline_version: str = "ueba-v1"
    sample_count: int = 0
    baseline_confidence: float = 0.0

    trained_from: datetime | None = None
    trained_to: datetime | None = None
    updated_at: datetime

    time_profile: dict[str, Any] = Field(default_factory=dict)
    location_profile: dict[str, Any] = Field(default_factory=dict)
    access_profile: dict[str, Any] = Field(default_factory=dict)
    volume_profile: dict[str, Any] = Field(default_factory=dict)
    result_profile: dict[str, Any] = Field(default_factory=dict)

    # 兼容旧版本字段
    active_hours: list[str] = Field(default_factory=list)
    common_ips: list[str] = Field(default_factory=list)
    common_user_agents: list[str] = Field(default_factory=list)
    avg_api_calls_per_minute: float = 0.0
    common_resources: list[str] = Field(default_factory=list)
    failed_login_count_7d: int = 0
    sensitive_access_rate: float = 0.0


class AIReport(BaseModel):
    model_config = ConfigDict(extra="allow")

    ai_report_id: str
    alert_id: str
    created_at: datetime

    attack_type: str
    risk_level: RiskLevel
    reason: str
    suggestion: str
    confidence: float
    next_steps: list[str] = Field(default_factory=list)
    raw_response: dict[str, Any] = Field(default_factory=dict)


class DailyReport(BaseModel):
    model_config = ConfigDict(extra="allow")

    report_id: str
    date: str
    created_at: datetime

    overall_score: float
    log_count: int
    alert_count: int
    high_risk_count: int

    major_risks: list[str]
    high_risk_users: list[str]
    typical_alerts: list[dict[str, Any]]

    ai_summary: str
    recommendation: str
    markdown: str
