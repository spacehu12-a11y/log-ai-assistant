from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
import sys

import altair as alt
import pandas as pd
import streamlit as st

# Ensure project root is importable when launched via `streamlit run src/dashboard/app.py`
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ai_engine import AIAnalyzer
from src.config import settings
from src.report.daily_report import generate_daily_report
from src.schemas import AlertEvent
from src.storage import ElasticStorage

st.set_page_config(
    page_title="日志分析 AI 助手",
    layout="wide",
    initial_sidebar_state="expanded",
)

PAGES = [
    "系统概览",
    "UEBA 告警分析",
    "用户画像",
    "实时异常解释",
    "最近日志",
    "AI 研判",
    "每日安全态势简报",
    "系统运行状态",
]


# ---------------------------------------------------------------------
# Global resources
# ---------------------------------------------------------------------


@st.cache_resource
def get_storage() -> ElasticStorage:
    return ElasticStorage()


def get_analyzer() -> AIAnalyzer:
    # Keep the analyzer uncached to avoid Streamlit cache-state edge cases.
    return AIAnalyzer()


def _rerun() -> None:
    try:
        st.rerun()
    except AttributeError:
        st.experimental_rerun()


def init_state() -> None:
    if "current_page" not in st.session_state:
        st.session_state.current_page = PAGES[0]


def set_page(page: str) -> None:
    st.session_state.current_page = page


def render_sidebar_nav() -> None:
    st.sidebar.title("日志分析 AI 助手")
    st.sidebar.caption("实时日志流 · Docker Flink · UEBA 用户画像")
    st.sidebar.markdown("### 页面切换")

    current_page = st.session_state.get("current_page", PAGES[0])
    clicked_page: str | None = None

    for idx, page in enumerate(PAGES):
        label = page
        if page == current_page:
            label = f"👉 {page}"

        if st.sidebar.button(label, key=f"nav_btn_{idx}", use_container_width=True):
            clicked_page = page

    if clicked_page and clicked_page != current_page:
        set_page(clicked_page)
        _rerun()

    st.sidebar.divider()
    st.sidebar.caption(f"当前页面：{st.session_state.get('current_page', PAGES[0])}")

    if st.sidebar.button("刷新当前页面", use_container_width=True, key="refresh_current_page"):
        _rerun()


# ---------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat()


def today_range_query(time_field: str) -> dict[str, Any]:
    now = utc_now()
    start = datetime.combine(now.date(), datetime.min.time(), tzinfo=timezone.utc)
    return {
        "range": {
            time_field: {
                "gte": iso(start),
                "lte": iso(now),
            }
        }
    }


def recent_range_query(time_field: str, hours: int = 24) -> dict[str, Any]:
    now = utc_now()
    start = now - timedelta(hours=hours)
    return {
        "range": {
            time_field: {
                "gte": iso(start),
                "lte": iso(now),
            }
        }
    }


def ueba_filter_query() -> dict[str, Any]:
    return {
        "bool": {
            "should": [
                {"term": {"alert_type": "ueba_anomaly"}},
                {"term": {"alert_type.keyword": "ueba_anomaly"}},
                {"match_phrase": {"alert_type": "ueba_anomaly"}},
            ],
            "minimum_should_match": 1,
        }
    }


def safe_get_path(data: Any, *keys: str, default: Any = None) -> Any:
    current: Any = data
    for key in keys:
        try:
            current = current[key]
        except (KeyError, TypeError):
            return default
    return current if current is not None else default


def select_existing_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    existing = [c for c in columns if c in df.columns]
    return df[existing] if existing else df


def as_percent(value: Any) -> float:
    try:
        return round(float(value) * 100, 2)
    except (TypeError, ValueError):
        return 0.0


def distribution_to_df(
    dist: dict[str, Any] | None,
    key_name: str,
    value_name: str = "占比(%)",
    top_n: int | None = None,
) -> pd.DataFrame:
    if not isinstance(dist, dict) or not dist:
        return pd.DataFrame(columns=[key_name, value_name])

    rows = [{key_name: str(k), value_name: as_percent(v)} for k, v in dist.items()]
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.sort_values(value_name, ascending=False)
    if top_n:
        df = df.head(top_n)
    return df


def hour_hist_to_df(dist: dict[str, Any] | None) -> pd.DataFrame:
    dist = dist or {}
    return pd.DataFrame(
        [
            {"小时": f"{h:02d}:00", "占比(%)": as_percent(dist.get(str(h), 0))}
            for h in range(24)
        ]
    )


def weekday_hist_to_df(dist: dict[str, Any] | None) -> pd.DataFrame:
    dist = dist or {}
    labels = {
        "0": "周一",
        "1": "周二",
        "2": "周三",
        "3": "周四",
        "4": "周五",
        "5": "周六",
        "6": "周日",
    }
    return pd.DataFrame(
        [
            {"星期": labels[str(i)], "占比(%)": as_percent(dist.get(str(i), 0))}
            for i in range(7)
        ]
    )


def chart_bar(df: pd.DataFrame, x_col: str, y_col: str, title: str, height: int = 320) -> None:
    st.subheader(title)
    if df.empty:
        st.info("暂无数据")
        return

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(
                f"{x_col}:N",
                sort="-y",
                axis=alt.Axis(title=None, labelAngle=0, labelLimit=160),
            ),
            y=alt.Y(f"{y_col}:Q", axis=alt.Axis(title=None)),
            tooltip=list(df.columns),
        )
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


def chart_line(df: pd.DataFrame, x_col: str, y_col: str, title: str, height: int = 320) -> None:
    st.subheader(title)
    if df.empty:
        st.info("暂无数据")
        return

    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X(
                f"{x_col}:N",
                axis=alt.Axis(title=None, labelAngle=0, labelLimit=160),
            ),
            y=alt.Y(f"{y_col}:Q", axis=alt.Axis(title=None)),
            tooltip=list(df.columns),
        )
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


def date_histogram(
    storage: ElasticStorage,
    index: str,
    time_field: str,
    query: dict[str, Any],
    interval: str = "1h",
) -> pd.DataFrame:
    body = {
        "size": 0,
        "query": query,
        "aggs": {
            "trend": {
                "date_histogram": {
                    "field": time_field,
                    "fixed_interval": interval,
                    "min_doc_count": 0,
                }
            }
        },
    }

    try:
        resp = storage.aggregate(index, body)
        buckets = resp.get("aggregations", {}).get("trend", {}).get("buckets", [])
    except Exception:
        buckets = []

    rows = []
    for b in buckets:
        raw_time = b.get("key_as_string", "")
        try:
            dt = pd.to_datetime(raw_time)
            label = dt.strftime("%H:%M")
        except Exception:
            label = raw_time

        rows.append({"时间": label, "原始时间": raw_time, "数量": b.get("doc_count", 0)})

    return pd.DataFrame(rows)


def terms_aggregation(
    storage: ElasticStorage,
    index: str,
    field: str,
    query: dict[str, Any] | None = None,
    size: int = 10,
) -> pd.DataFrame:
    body = {
        "size": 0,
        "query": query or {"match_all": {}},
        "aggs": {"items": {"terms": {"field": field, "size": size}}},
    }

    try:
        resp = storage.aggregate(index, body)
        buckets = resp.get("aggregations", {}).get("items", {}).get("buckets", [])
    except Exception:
        buckets = []

    return pd.DataFrame([{"名称": b.get("key"), "数量": b.get("doc_count", 0)} for b in buckets])


def ueba_score_histogram(storage: ElasticStorage, query: dict[str, Any]) -> pd.DataFrame:
    body = {
        "size": 0,
        "query": query,
        "aggs": {
            "score_hist": {
                "histogram": {
                    "field": "ueba_score",
                    "interval": 10,
                    "min_doc_count": 0,
                }
            }
        },
    }

    try:
        resp = storage.aggregate(settings.elasticsearch_alert_index, body)
        buckets = resp.get("aggregations", {}).get("score_hist", {}).get("buckets", [])
    except Exception:
        buckets = []

    return pd.DataFrame(
        [
            {"分数段": f"{int(b.get('key', 0))}-{int(b.get('key', 0)) + 9}", "告警数": b.get("doc_count", 0)}
            for b in buckets
        ]
    )


def top_risky_users(storage: ElasticStorage, query: dict[str, Any], size: int = 10) -> pd.DataFrame:
    body = {
        "size": 0,
        "query": query,
        "aggs": {
            "users": {
                "terms": {"field": "username", "size": size},
                "aggs": {
                    "avg_ueba_score": {"avg": {"field": "ueba_score"}},
                    "max_ueba_score": {"max": {"field": "ueba_score"}},
                    "last_time": {"max": {"field": "detect_time"}},
                },
            }
        },
    }

    try:
        resp = storage.aggregate(settings.elasticsearch_alert_index, body)
        buckets = resp.get("aggregations", {}).get("users", {}).get("buckets", [])
    except Exception:
        buckets = []

    rows = []
    for b in buckets:
        rows.append(
            {
                "用户": b.get("key"),
                "告警数": b.get("doc_count", 0),
                "平均UEBA分": round(b.get("avg_ueba_score", {}).get("value") or 0, 2),
                "最高UEBA分": round(b.get("max_ueba_score", {}).get("value") or 0, 2),
                "最近告警时间": b.get("last_time", {}).get("value_as_string", ""),
            }
        )
    return pd.DataFrame(rows)


def flatten_ueba_alert(alert: dict[str, Any]) -> dict[str, Any]:
    evidence = alert.get("evidence") or {}
    deviation = alert.get("deviation_features") or evidence.get("deviation_features") or {}
    reasons = alert.get("anomaly_reasons") or evidence.get("anomaly_reasons") or []
    current = evidence.get("current_behavior") or {}

    return {
        "alert_id": alert.get("alert_id"),
        "detect_time": alert.get("detect_time"),
        "event_time": alert.get("event_time"),
        "username": alert.get("username"),
        "src_ip": alert.get("src_ip"),
        "risk_level": alert.get("risk_level"),
        "ueba_score": alert.get("ueba_score") or alert.get("risk_score"),
        "baseline_confidence": alert.get("baseline_confidence") or evidence.get("baseline_confidence"),
        "time_score": deviation.get("time_score"),
        "ip_score": deviation.get("ip_score"),
        "geo_score": deviation.get("geo_score"),
        "access_score": deviation.get("access_score"),
        "volume_score": deviation.get("volume_score"),
        "result_score": deviation.get("result_score"),
        "country": current.get("country"),
        "city": current.get("city"),
        "protocol": current.get("protocol"),
        "auth_method": current.get("auth_method"),
        "client": current.get("client"),
        "status": current.get("status"),
        "anomaly_reasons": "；".join(reasons) if isinstance(reasons, list) else str(reasons),
    }


def load_ueba_alerts(
    storage: ElasticStorage,
    hours: int = 24,
    risk: str = "全部",
    username: str = "",
    size: int = 500,
) -> list[dict[str, Any]]:
    must: list[dict[str, Any]] = [recent_range_query("detect_time", hours), ueba_filter_query()]

    if risk != "全部":
        must.append({"term": {"risk_level": risk}})
    if username:
        must.append({"term": {"username": username}})

    return storage.search(
        settings.elasticsearch_alert_index,
        query={"bool": {"must": must}},
        size=size,
        sort=[{"detect_time": "desc"}],
    )


def load_baselines(storage: ElasticStorage, size: int = 1000) -> list[dict[str, Any]]:
    return storage.search(
        settings.elasticsearch_baseline_index,
        query={"match_all": {}},
        size=size,
        sort=[{"updated_at": "desc"}],
    )


def deviation_average_df(alerts: list[dict[str, Any]]) -> pd.DataFrame:
    if not alerts:
        return pd.DataFrame(columns=["维度", "平均分"])

    rows = []
    for alert in alerts:
        evidence = alert.get("evidence") or {}
        deviation = alert.get("deviation_features") or evidence.get("deviation_features") or {}
        rows.append(
            {
                "时间偏离": deviation.get("time_score", 0) or 0,
                "IP偏离": deviation.get("ip_score", 0) or 0,
                "地理位置偏离": deviation.get("geo_score", 0) or 0,
                "访问方式偏离": deviation.get("access_score", 0) or 0,
                "流量偏离": deviation.get("volume_score", 0) or 0,
                "登录结果偏离": deviation.get("result_score", 0) or 0,
            }
        )

    df = pd.DataFrame(rows)
    return pd.DataFrame({"维度": df.columns, "平均分": [round(float(df[col].mean()), 2) for col in df.columns]})


def render_time_window_controls() -> tuple[datetime, datetime]:
    st.markdown("#### 时间范围")
    mode = st.selectbox(
        "选择时间范围",
        ["最近15分钟", "最近1小时", "最近6小时", "最近24小时", "最近7天", "自定义范围"],
        index=3,
        key="recent_logs_range_mode",
    )

    now = utc_now()
    preset_map = {
        "最近15分钟": timedelta(minutes=15),
        "最近1小时": timedelta(hours=1),
        "最近6小时": timedelta(hours=6),
        "最近24小时": timedelta(hours=24),
        "最近7天": timedelta(days=7),
    }

    if mode != "自定义范围":
        end_time = now
        start_time = now - preset_map[mode]
        st.caption(f"当前查询窗口：{iso(start_time)} ~ {iso(end_time)} (UTC)")
        return start_time, end_time

    st.caption("自定义范围按 UTC 时间查询")
    c1, c2, c3, c4 = st.columns(4)
    start_date = c1.date_input(
        "开始日期",
        value=(now - timedelta(days=1)).date(),
        key="recent_logs_start_date",
    )
    start_clock = c2.time_input(
        "开始时刻",
        value=(now - timedelta(days=1)).time().replace(microsecond=0),
        key="recent_logs_start_time",
    )
    end_date = c3.date_input("结束日期", value=now.date(), key="recent_logs_end_date")
    end_clock = c4.time_input(
        "结束时刻",
        value=now.time().replace(microsecond=0),
        key="recent_logs_end_time",
    )

    start_time = datetime.combine(start_date, start_clock).replace(tzinfo=timezone.utc)
    end_time = datetime.combine(end_date, end_clock).replace(tzinfo=timezone.utc)

    if end_time < start_time:
        st.warning("结束时间早于开始时间，已自动交换。")
        start_time, end_time = end_time, start_time

    st.caption(f"当前查询窗口：{iso(start_time)} ~ {iso(end_time)} (UTC)")
    return start_time, end_time


# ---------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------


def page_overview(storage: ElasticStorage) -> None:
    st.title("系统概览")

    today_log_query = today_range_query("ingest_time")
    today_alert_query = {"bool": {"must": [today_range_query("detect_time"), ueba_filter_query()]}}

    log_count = storage.count(settings.elasticsearch_log_index, today_log_query)
    ueba_alert_count = storage.count(settings.elasticsearch_alert_index, today_alert_query)
    high_ueba_count = storage.count(
        settings.elasticsearch_alert_index,
        {"bool": {"must": [today_range_query("detect_time"), ueba_filter_query(), {"term": {"risk_level": "高"}}]}},
    )
    baseline_count = storage.count(settings.elasticsearch_baseline_index)
    ai_count = storage.count(settings.elasticsearch_ai_index)

    user_agg = storage.aggregate(
        settings.elasticsearch_log_index,
        {
            "size": 0,
            "query": today_log_query,
            "aggs": {
                "users": {"cardinality": {"field": "username"}},
                "ips": {"cardinality": {"field": "src_ip"}},
            },
        },
    )

    users = safe_get_path(user_agg, "aggregations", "users", "value", default=0)
    ips = safe_get_path(user_agg, "aggregations", "ips", "value", default=0)

    avg_score_resp = storage.aggregate(
        settings.elasticsearch_alert_index,
        {
            "size": 0,
            "query": today_alert_query,
            "aggs": {
                "avg_ueba_score": {"avg": {"field": "ueba_score"}},
                "avg_risk_score": {"avg": {"field": "risk_score"}},
            },
        },
    )

    ueba_avg_val = safe_get_path(avg_score_resp, "aggregations", "avg_ueba_score", "value")
    risk_avg_val = safe_get_path(avg_score_resp, "aggregations", "avg_risk_score", "value")
    avg_score = ueba_avg_val if ueba_avg_val is not None else (risk_avg_val if risk_avg_val is not None else 0)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("今日日志总量", log_count)
    c2.metric("今日UEBA告警", ueba_alert_count)
    c3.metric("高危UEBA告警", high_ueba_count)
    c4.metric("用户画像数量", baseline_count)
    c5.metric("涉及来源IP", ips)
    c6.metric("平均UEBA分", round(float(avg_score), 2))

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        chart_line(
            date_histogram(storage, settings.elasticsearch_log_index, "ingest_time", today_log_query, interval="1h"),
            "时间",
            "数量",
            "今日日志流入趋势",
        )
    with col2:
        chart_line(
            date_histogram(storage, settings.elasticsearch_alert_index, "detect_time", today_alert_query, interval="1h"),
            "时间",
            "数量",
            "今日UEBA告警趋势",
        )

    st.divider()

    recent_alerts = load_ueba_alerts(storage, hours=24, size=300)
    col3, col4 = st.columns(2)
    with col3:
        chart_bar(
            terms_aggregation(storage, settings.elasticsearch_alert_index, "risk_level", today_alert_query, size=10),
            "名称",
            "数量",
            "今日UEBA风险等级分布",
        )
    with col4:
        top_users = top_risky_users(storage, today_alert_query, size=10)
        if not top_users.empty:
            chart_bar(top_users, "用户", "告警数", "今日Top风险用户")
        else:
            st.subheader("今日Top风险用户")
            st.info("暂无数据")

    chart_bar(deviation_average_df(recent_alerts), "维度", "平均分", "最近24小时平均偏离维度")


def page_ueba_alerts(storage: ElasticStorage, analyzer: AIAnalyzer) -> None:
    st.title("UEBA 告警分析")

    c1, c2, c3, c4 = st.columns(4)
    hours = c1.selectbox("时间范围", [1, 6, 12, 24, 72, 168], index=3, format_func=lambda x: f"最近{x}小时")
    risk = c2.selectbox("风险等级", ["全部", "低", "中", "高"])
    username = c3.text_input("用户")
    size = c4.number_input("读取告警数", min_value=50, max_value=2000, value=500, step=50)

    alerts = load_ueba_alerts(storage, hours=int(hours), risk=risk, username=username, size=int(size))
    if not alerts:
        st.info("暂无 UEBA 告警")
        return

    df = pd.DataFrame([flatten_ueba_alert(item) for item in alerts])

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("UEBA告警数", len(df))
    m2.metric("平均UEBA分", round(float(df["ueba_score"].fillna(0).mean()), 2))
    m3.metric("最高UEBA分", round(float(df["ueba_score"].fillna(0).max()), 2))
    m4.metric("涉及用户数", int(df["username"].nunique()))

    st.divider()

    score_query = {"bool": {"must": [recent_range_query("detect_time", int(hours)), ueba_filter_query()]}}
    c5, c6 = st.columns(2)
    with c5:
        chart_bar(ueba_score_histogram(storage, score_query), "分数段", "告警数", "UEBA 分数分布")
    with c6:
        top_users_df = top_risky_users(storage, score_query, size=10)
        if not top_users_df.empty:
            chart_bar(top_users_df, "用户", "告警数", "Top 10 告警用户")
        else:
            st.subheader("Top 10 告警用户")
            st.info("暂无数据")

    chart_bar(deviation_average_df(alerts), "维度", "平均分", "平均偏离维度")

    st.subheader("UEBA 告警明细")
    st.dataframe(
        select_existing_columns(
            df,
            [
                "detect_time",
                "username",
                "src_ip",
                "risk_level",
                "ueba_score",
                "baseline_confidence",
                "time_score",
                "ip_score",
                "geo_score",
                "access_score",
                "volume_score",
                "result_score",
                "country",
                "city",
                "status",
                "anomaly_reasons",
            ],
        ),
        use_container_width=True,
    )

    selected_id = st.selectbox("选择告警查看详情", options=df["alert_id"].tolist())
    selected = next(item for item in alerts if item.get("alert_id") == selected_id)

    st.subheader("UEBA 告警详情")
    d1, d2 = st.columns([1, 1])
    with d1:
        st.markdown("**异常解释**")
        reasons = selected.get("anomaly_reasons") or safe_get_path(selected, "evidence", "anomaly_reasons", default=[])
        if reasons:
            for reason in reasons:
                st.write(f"- {reason}")
        else:
            st.info("暂无异常解释")
        st.markdown("**当前行为**")
        st.json(safe_get_path(selected, "evidence", "current_behavior", default={}))
    with d2:
        deviation = selected.get("deviation_features") or safe_get_path(selected, "evidence", "deviation_features", default={})
        dev_df = pd.DataFrame(
            [
                {"维度": "时间偏离", "分数": deviation.get("time_score", 0)},
                {"维度": "IP偏离", "分数": deviation.get("ip_score", 0)},
                {"维度": "地理位置偏离", "分数": deviation.get("geo_score", 0)},
                {"维度": "访问方式偏离", "分数": deviation.get("access_score", 0)},
                {"维度": "流量偏离", "分数": deviation.get("volume_score", 0)},
                {"维度": "登录结果偏离", "分数": deviation.get("result_score", 0)},
            ]
        )
        chart_bar(dev_df, "维度", "分数", "单条告警偏离分解")

    with st.expander("完整告警 JSON"):
        st.json(selected)

    related_logs = storage.search(
        settings.elasticsearch_log_index,
        query={"terms": {"event_id": selected.get("related_event_ids", [])}},
        size=50,
    )
    with st.expander("相关日志"):
        st.dataframe(pd.DataFrame(related_logs), use_container_width=True)

    baseline = storage.search(
        settings.elasticsearch_baseline_index,
        query={"term": {"username": selected.get("username")}},
        size=1,
    )
    with st.expander("用户行为画像"):
        st.json(baseline[0] if baseline else {})

    ai_report = storage.search(
        settings.elasticsearch_ai_index,
        query={"term": {"alert_id": selected_id}},
        size=1,
        sort=[{"created_at": "desc"}],
    )
    with st.expander("AI 研判结果"):
        st.json(ai_report[0] if ai_report else {})

    if st.button("重新 AI 研判", key=f"reanalyze-{selected_id}"):
        alert = AlertEvent.model_validate(selected)
        baseline_doc = baseline[0] if baseline else {}
        report = analyzer.analyze(alert, baseline=baseline_doc, related_logs=related_logs)
        storage.index_document(settings.elasticsearch_ai_index, report.model_dump(mode="json"), doc_id=report.ai_report_id)
        storage.update_document(
            settings.elasticsearch_alert_index,
            selected_id,
            {"llm_analysis_id": report.ai_report_id, "status": "analyzed"},
        )
        st.success("已重新生成 AI 研判")


def page_user_profile(storage: ElasticStorage) -> None:
    st.title("用户画像")

    baselines = load_baselines(storage)
    if not baselines:
        st.info("暂无用户画像，请先运行：python src/main.py build-baseline")
        return

    usernames = sorted([item.get("username") for item in baselines if item.get("username")])
    selected_username = st.selectbox("选择用户", usernames)
    baseline = next(item for item in baselines if item.get("username") == selected_username)

    st.subheader(f"用户画像：{selected_username}")

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("样本数", baseline.get("sample_count", 0))
    m2.metric("画像置信度", baseline.get("baseline_confidence", 0))
    m3.metric("部门", baseline.get("dept") or "N/A")
    m4.metric("画像版本", baseline.get("baseline_version", "N/A"))
    m5.metric("更新时间", str(baseline.get("updated_at", "N/A"))[:19])

    time_profile = baseline.get("time_profile") or {}
    location_profile = baseline.get("location_profile") or {}
    access_profile = baseline.get("access_profile") or {}
    volume_profile = baseline.get("volume_profile") or {}
    result_profile = baseline.get("result_profile") or {}

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        chart_bar(hour_hist_to_df(time_profile.get("hour_hist")), "小时", "占比(%)", "登录小时分布")
    with col2:
        chart_bar(weekday_hist_to_df(time_profile.get("weekday_hist")), "星期", "占比(%)", "星期登录分布")

    st.divider()

    col3, col4 = st.columns(2)
    with col3:
        chart_bar(distribution_to_df(location_profile.get("ip_prefixes"), "IP前缀", top_n=10), "IP前缀", "占比(%)", "常用 IP 前缀 Top 10")
    with col4:
        chart_bar(distribution_to_df(location_profile.get("cities"), "城市", top_n=10), "城市", "占比(%)", "常用城市 Top 10")

    st.divider()

    col5, col6 = st.columns(2)
    with col5:
        chart_bar(distribution_to_df(access_profile.get("protocols"), "协议", top_n=10), "协议", "占比(%)", "VPN 协议分布")
    with col6:
        chart_bar(distribution_to_df(access_profile.get("clients"), "客户端", top_n=10), "客户端", "占比(%)", "客户端分布")

    col7, col8 = st.columns(2)
    with col7:
        chart_bar(distribution_to_df(access_profile.get("gateways"), "网关", top_n=10), "网关", "占比(%)", "VPN 网关分布")
    with col8:
        chart_bar(distribution_to_df(access_profile.get("auth_methods"), "认证方式", top_n=10), "认证方式", "占比(%)", "认证方式分布")

    st.divider()

    st.subheader("流量与会话基线")
    v1, v2, v3, v4, v5 = st.columns(5)
    v1.metric("会话时长中位数", int(volume_profile.get("session_duration_median") or 0))
    v2.metric("下载流量中位数", int(volume_profile.get("bytes_recv_median") or 0))
    v3.metric("下载流量P95", int(volume_profile.get("bytes_recv_p95") or 0))
    v4.metric("上传流量中位数", int(volume_profile.get("bytes_sent_median") or 0))
    v5.metric("失败率", round(float(result_profile.get("failure_rate") or 0), 4))

    st.divider()

    st.subheader("该用户最近 UEBA 告警")
    user_alerts = load_ueba_alerts(storage, hours=168, username=selected_username, size=200)

    if not user_alerts:
        st.info("该用户最近暂无 UEBA 告警")
    else:
        user_df = pd.DataFrame([flatten_ueba_alert(item) for item in user_alerts]).sort_values("detect_time")
        chart_line(user_df, "detect_time", "ueba_score", "该用户 UEBA 分数变化")
        st.dataframe(
            select_existing_columns(
                user_df,
                [
                    "detect_time",
                    "src_ip",
                    "risk_level",
                    "ueba_score",
                    "time_score",
                    "ip_score",
                    "geo_score",
                    "access_score",
                    "volume_score",
                    "result_score",
                    "anomaly_reasons",
                ],
            ),
            use_container_width=True,
        )

    with st.expander("完整用户画像 JSON"):
        st.json(baseline)


def page_realtime_explain(storage: ElasticStorage) -> None:
    st.title("实时异常解释")

    alerts = load_ueba_alerts(storage, hours=24, size=30)
    if not alerts:
        st.info("最近 24 小时暂无 UEBA 告警")
        return

    for alert in alerts:
        score = alert.get("ueba_score") or alert.get("risk_score")
        username = alert.get("username") or "unknown"
        src_ip = alert.get("src_ip") or "unknown"
        risk_level = alert.get("risk_level") or "N/A"
        detect_time = alert.get("detect_time") or ""

        with st.expander(f"[{risk_level}] {username} / {src_ip} / UEBA={score} / {detect_time}", expanded=False):
            evidence = alert.get("evidence") or {}
            reasons = alert.get("anomaly_reasons") or evidence.get("anomaly_reasons") or []
            current = evidence.get("current_behavior") or {}
            deviation = alert.get("deviation_features") or evidence.get("deviation_features") or {}

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("UEBA分数", score)
            c2.metric("风险等级", risk_level)
            c3.metric("画像置信度", alert.get("baseline_confidence") or evidence.get("baseline_confidence") or 0)
            c4.metric("样本数", evidence.get("sample_count", 0))

            st.markdown("**为什么异常：**")
            if reasons:
                for reason in reasons:
                    st.write(f"- {reason}")
            else:
                st.write("暂无异常原因")

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**当前行为：**")
                st.json(current)
            with col2:
                dev_df = pd.DataFrame(
                    [
                        {"维度": "时间偏离", "分数": deviation.get("time_score", 0)},
                        {"维度": "IP偏离", "分数": deviation.get("ip_score", 0)},
                        {"维度": "地理位置偏离", "分数": deviation.get("geo_score", 0)},
                        {"维度": "访问方式偏离", "分数": deviation.get("access_score", 0)},
                        {"维度": "流量偏离", "分数": deviation.get("volume_score", 0)},
                        {"维度": "登录结果偏离", "分数": deviation.get("result_score", 0)},
                    ]
                )
                chart_bar(dev_df, "维度", "分数", "偏离分解", height=260)


def page_recent_logs(storage: ElasticStorage) -> None:
    st.title("最近日志")

    col1, col2, col3, col4 = st.columns(4)
    source_type = col1.selectbox("source_type", ["全部", "vpn", "oa", "api", "system", "security_device"])
    username = col2.text_input("username")
    src_ip = col3.text_input("src_ip")
    status = col4.selectbox("status", ["全部", "success", "failed", "denied", "error"])

    start_time, end_time = render_time_window_controls()

    must = [
        {
            "range": {
                "ingest_time": {
                    "gte": iso(start_time),
                    "lte": iso(end_time),
                }
            }
        }
    ]

    if source_type != "全部":
        must.append({"term": {"source_type": source_type}})
    if username:
        must.append({"term": {"username": username}})
    if src_ip:
        must.append({"term": {"src_ip": src_ip}})
    if status != "全部":
        must.append({"term": {"status": status}})

    logs = storage.search(
        settings.elasticsearch_log_index,
        query={"bool": {"must": must}},
        size=500,
        sort=[{"ingest_time": "desc"}],
    )

    if not logs:
        st.info("暂无日志")
        return

    df = pd.DataFrame(logs)
    visible_cols = [
        "ingest_time",
        "event_time",
        "source_type",
        "username",
        "src_ip",
        "action",
        "status",
        "resource",
        "message",
        "parser",
        "pipeline_stage",
    ]
    st.dataframe(select_existing_columns(df, visible_cols), use_container_width=True)

    with st.expander("完整日志数据"):
        st.dataframe(df, use_container_width=True)


def page_ai_reports(storage: ElasticStorage) -> None:
    st.title("AI 研判")

    reports = storage.search(
        settings.elasticsearch_ai_index,
        query={"match_all": {}},
        size=200,
        sort=[{"created_at": "desc"}],
    )

    if not reports:
        st.info("暂无 AI 研判报告")
        return

    st.dataframe(pd.DataFrame(reports), use_container_width=True)


def page_daily_report(storage: ElasticStorage) -> None:
    st.title("每日安全态势简报")

    if st.button("生成今日简报"):
        report = generate_daily_report(storage)
        storage.index_document(settings.elasticsearch_daily_index, report.model_dump(mode="json"), doc_id=report.report_id)
        st.success("简报已生成")
        st.markdown(report.markdown)

    reports = storage.search(
        settings.elasticsearch_daily_index,
        query={"match_all": {}},
        size=20,
        sort=[{"created_at": "desc"}],
    )

    if reports:
        selected = st.selectbox("历史简报", options=[r["report_id"] for r in reports])
        report = next(r for r in reports if r["report_id"] == selected)
        st.markdown(report.get("markdown", ""))


def page_system_health(storage: ElasticStorage, analyzer: AIAnalyzer) -> None:
    st.title("系统运行状态")

    kafka_ok = True
    try:
        from kafka import KafkaAdminClient

        admin = KafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
        admin.list_topics()
        admin.close()
    except Exception:
        kafka_ok = False

    es_ok = storage.health()

    flink_ok = False
    try:
        import requests

        resp = requests.get(f"{settings.flink_dashboard_url}/overview", timeout=3)
        flink_ok = resp.ok
    except Exception:
        flink_ok = False

    st.write(f"Kafka 连接: {'正常' if kafka_ok else '异常'}")
    st.write(f"Elasticsearch 连接: {'正常' if es_ok else '异常'}")
    st.write(f"Flink Dashboard: {settings.flink_dashboard_url} ({'正常' if flink_ok else '异常'})")
    st.write(f"DashScope API: {'已配置' if not analyzer.mock_mode else '未配置，当前为 mock 模式'}")

    latest = storage.search(
        settings.elasticsearch_log_index,
        query={"match_all": {}},
        size=1,
        sort=[{"ingest_time": "desc"}],
    )
    st.write(f"最近一次数据更新时间: {latest[0].get('ingest_time') if latest else 'N/A'}")

    st.subheader("服务统计")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("security-logs", storage.count(settings.elasticsearch_log_index))
    c2.metric("security-alerts", storage.count(settings.elasticsearch_alert_index))
    c3.metric("user-baselines", storage.count(settings.elasticsearch_baseline_index))
    c4.metric("ai-reports", storage.count(settings.elasticsearch_ai_index))


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------


def main() -> None:
    init_state()
    storage = get_storage()
    analyzer = get_analyzer()
    render_sidebar_nav()

    page = st.session_state.get("current_page", PAGES[0])
    if page == "系统概览":
        page_overview(storage)
    elif page == "UEBA 告警分析":
        page_ueba_alerts(storage, analyzer)
    elif page == "用户画像":
        page_user_profile(storage)
    elif page == "实时异常解释":
        page_realtime_explain(storage)
    elif page == "最近日志":
        page_recent_logs(storage)
    elif page == "AI 研判":
        page_ai_reports(storage)
    elif page == "每日安全态势简报":
        page_daily_report(storage)
    elif page == "系统运行状态":
        page_system_health(storage, analyzer)


if __name__ == "__main__":
    main()