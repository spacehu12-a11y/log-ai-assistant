from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.parser.log_parser import normalize_raw_record  # noqa: E402


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_parse_error(raw: str, error: Exception) -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": utc_now_iso(),
        "ingest_time": utc_now_iso(),
        "source_type": "system",
        "username": None,
        "src_ip": None,
        "src_port": None,
        "dst_ip": None,
        "dst_port": None,
        "action": "parse",
        "resource": "raw_logs",
        "status": "error",
        "http_method": None,
        "user_agent": None,
        "message": f"Flink parse error: {error}",
        "raw_message": raw,
        "risk_tags": ["parse_error"],
        "trace_id": None,
        "original_fields": {
            "parser": "docker_flink_raw_to_parsed",
            "error": str(error),
        },
        "parser": "flink",
        "pipeline_stage": "parsed",
    }


def to_parsed_json(raw: str) -> str:
    try:
        normalized = normalize_raw_record(raw, source_type_hint="vpn")
        doc = normalized.model_dump(mode="json")
        doc["parser"] = "flink"
        doc["pipeline_stage"] = "parsed"
        return json.dumps(doc, ensure_ascii=False)
    except Exception as exc:
        return json.dumps(build_parse_error(raw, exc), ensure_ascii=False)


def make_offsets_initializer(mode: str):
    if mode == "earliest":
        return KafkaOffsetsInitializer.earliest()
    if mode == "latest":
        return KafkaOffsetsInitializer.latest()
    raise ValueError(f"Unsupported starting offsets mode: {mode}")


def run_job(
    bootstrap_servers: str,
    raw_topic: str,
    parsed_topic: str,
    group_id: str,
    starting_offsets: str,
    parallelism: int,
    checkpoint_interval_ms: int,
) -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)

    if checkpoint_interval_ms > 0:
        env.enable_checkpointing(checkpoint_interval_ms)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(raw_topic)
        .set_group_id(group_id)
        .set_starting_offsets(make_offsets_initializer(starting_offsets))
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(parsed_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    ds = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "kafka_raw_logs_source",
    )

    parsed = ds.map(
        to_parsed_json,
        output_type=Types.STRING(),
    )

    parsed.sink_to(sink)

    env.execute("docker_flink_raw_logs_to_parsed_logs")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Docker Flink streaming job: raw_logs -> parsed_logs"
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    parser.add_argument(
        "--raw-topic",
        default=os.getenv("KAFKA_RAW_TOPIC", "raw_logs"),
    )
    parser.add_argument(
        "--parsed-topic",
        default=os.getenv("KAFKA_PARSED_TOPIC", "parsed_logs"),
    )
    parser.add_argument(
        "--group-id",
        default="flink-raw-to-parsed",
    )
    parser.add_argument(
        "--starting-offsets",
        choices=["latest", "earliest"],
        default="latest",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--checkpoint-interval-ms",
        type=int,
        default=10000,
    )

    return parser


def main() -> None:
    args = build_parser().parse_args()

    print("=" * 80)
    print("[Docker Flink Job] raw_logs -> parsed_logs")
    print(f"bootstrap_servers      : {args.bootstrap_servers}")
    print(f"raw_topic              : {args.raw_topic}")
    print(f"parsed_topic           : {args.parsed_topic}")
    print(f"group_id               : {args.group_id}")
    print(f"starting_offsets       : {args.starting_offsets}")
    print(f"parallelism            : {args.parallelism}")
    print(f"checkpoint_interval_ms : {args.checkpoint_interval_ms}")
    print("=" * 80)

    run_job(
        bootstrap_servers=args.bootstrap_servers,
        raw_topic=args.raw_topic,
        parsed_topic=args.parsed_topic,
        group_id=args.group_id,
        starting_offsets=args.starting_offsets,
        parallelism=args.parallelism,
        checkpoint_interval_ms=args.checkpoint_interval_ms,
    )


if __name__ == "__main__":
    main()
