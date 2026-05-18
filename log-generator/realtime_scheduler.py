#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
realtime_scheduler.py

作用：
1. 不修改gen_vpn_logs.py
2. 提供两种运行模式：
   - once   ：单次生成日志，等价于手动运行 gen_vpn_logs.py
   - stream ：持续随机生成日志，并 append 到目标日志文件，供 Filebeat 实时采集



单次模式：
python realtime_scheduler.py once \
  --start 2026-04-01 \
  --days 1 \
  --count 120 \
  --outdir ./vpn_output \
  --format all \
  --seed 42

持续模式：
python realtime_scheduler.py stream \
  --outdir ./vpn_output \
  --format all \
  --interval-min 3 \
  --interval-max 8 \
  --count-min 20 \
  --count-max 80
"""

import argparse
import csv
import os
import random
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional


RUNNING = True


def handle_signal(signum, frame):
    global RUNNING
    print("\n[Scheduler] 收到退出信号，准备停止...")
    RUNNING = False


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def get_script_dir() -> Path:
    return Path(__file__).resolve().parent


def get_generator_path() -> Path:
    return get_script_dir() / "gen_vpn_logs.py"


def ensure_generator_exists() -> Path:
    generator = get_generator_path()
    if not generator.exists():
        raise FileNotFoundError(f"未找到日志生成器: {generator}")
    return generator


def run_generator(
    start: str,
    days: int,
    count: int,
    outdir: Path,
    output_format: str,
    seed: int,
) -> None:
    generator = ensure_generator_exists()
    outdir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        str(generator),
        "--start",
        start,
        "--days",
        str(days),
        "--count",
        str(count),
        "--outdir",
        str(outdir),
        "--format",
        output_format,
        "--seed",
        str(seed),
    ]

    print("[Generator] 执行命令:")
    print(" ".join(cmd))

    subprocess.run(cmd, check=True)


def clear_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        return sum(1 for _ in f)


def append_text_file(src: Path, dst: Path) -> int:
    """
    用于 syslog / jsonl。
    直接逐行追加。
    """
    if not src.exists():
        return 0

    dst.parent.mkdir(parents=True, exist_ok=True)

    line_count = 0
    with src.open("r", encoding="utf-8", errors="ignore") as fsrc, \
            dst.open("a", encoding="utf-8") as fdst:
        for line in fsrc:
            fdst.write(line)
            line_count += 1

    return line_count


def append_csv_file(src: Path, dst: Path) -> int:
    """
    追加 CSV。
    如果目标文件不存在，写入表头；
    如果目标文件已存在，跳过临时文件表头，只追加数据行。
    """
    if not src.exists():
        return 0

    dst.parent.mkdir(parents=True, exist_ok=True)

    dst_exists = dst.exists() and dst.stat().st_size > 0
    appended = 0

    with src.open("r", encoding="utf-8-sig", newline="") as fsrc:
        reader = list(csv.reader(fsrc))

    if not reader:
        return 0

    header = reader[0]
    rows = reader[1:]

    with dst.open("a", encoding="utf-8-sig", newline="") as fdst:
        writer = csv.writer(fdst)

        if not dst_exists:
            writer.writerow(header)

        for row in rows:
            writer.writerow(row)
            appended += 1

    return appended


def append_generated_files(tmp_dir: Path, final_dir: Path, output_format: str) -> dict:
    """
    将临时目录中生成的文件 append 到最终目录。
    """
    final_dir.mkdir(parents=True, exist_ok=True)

    stats = {
        "syslog": 0,
        "jsonl": 0,
        "csv": 0,
    }

    if output_format in ("syslog", "all"):
        stats["syslog"] = append_text_file(
            tmp_dir / "vpn_logs.log",
            final_dir / "vpn_logs.log",
        )

    if output_format in ("jsonl", "all"):
        stats["jsonl"] = append_text_file(
            tmp_dir / "vpn_logs.jsonl",
            final_dir / "vpn_logs.jsonl",
        )

    if output_format in ("csv", "all"):
        stats["csv"] = append_csv_file(
            tmp_dir / "vpn_logs.csv",
            final_dir / "vpn_logs.csv",
        )

    return stats


def maybe_reset_output(outdir: Path, output_format: str) -> None:
    """
    持续模式下可选择启动前清空目标文件。
    """
    targets = []

    if output_format in ("syslog", "all"):
        targets.append(outdir / "vpn_logs.log")
    if output_format in ("jsonl", "all"):
        targets.append(outdir / "vpn_logs.jsonl")
    if output_format in ("csv", "all"):
        targets.append(outdir / "vpn_logs.csv")

    for target in targets:
        if target.exists():
            target.unlink()
            print(f"[Reset] 已删除旧文件: {target}")


def run_once(args) -> None:
    """
    单次生成模式。
    等价于直接调用 gen_vpn_logs.py。
    """
    outdir = Path(args.outdir).resolve()

    print("=" * 70)
    print("[Mode] once 单次生成模式")
    print(f"[Output] {outdir}")
    print("=" * 70)

    run_generator(
        start=args.start,
        days=args.days,
        count=args.count,
        outdir=outdir,
        output_format=args.format,
        seed=args.seed,
    )

    print("[Once] 单次生成完成")


def run_stream(args) -> None:
    """
    持续生成模式。
    每轮随机 count、随机 interval、随机 seed。
    先生成到 tmp_output，再 append 到 outdir。
    """
    script_dir = get_script_dir()
    tmp_dir = Path(args.tmp_dir).resolve() if args.tmp_dir else script_dir / "tmp_output"
    outdir = Path(args.outdir).resolve()

    if args.count_min <= 0 or args.count_max <= 0:
        raise ValueError("--count-min 和 --count-max 必须大于 0")

    if args.count_min > args.count_max:
        raise ValueError("--count-min 不能大于 --count-max")

    if args.interval_min <= 0 or args.interval_max <= 0:
        raise ValueError("--interval-min 和 --interval-max 必须大于 0")

    if args.interval_min > args.interval_max:
        raise ValueError("--interval-min 不能大于 --interval-max")

    outdir.mkdir(parents=True, exist_ok=True)
    clear_dir(tmp_dir)

    if args.reset:
        maybe_reset_output(outdir, args.format)

    print("=" * 70)
    print("[Mode] stream 持续随机生成模式")
    print(f"[Generator] {get_generator_path()}")
    print(f"[Tmp Dir]   {tmp_dir}")
    print(f"[Out Dir]   {outdir}")
    print(f"[Format]    {args.format}")
    print(f"[Count]     每轮随机 {args.count_min} ~ {args.count_max}")
    print(f"[Interval]  每轮随机 {args.interval_min}s ~ {args.interval_max}s")
    print(f"[MaxRounds] {args.max_rounds if args.max_rounds > 0 else '无限运行'}")
    print("=" * 70)

    round_idx = 0

    while RUNNING:
        round_idx += 1

        if args.max_rounds > 0 and round_idx > args.max_rounds:
            print("[Stream] 已达到最大轮数，停止运行")
            break

        batch_count = random.randint(args.count_min, args.count_max)
        interval = random.randint(args.interval_min, args.interval_max)

        if args.start == "today":
            start_date = datetime.now().strftime("%Y-%m-%d")
        else:
            start_date = args.start

        if args.seed is None:
            seed = random.randint(1, 2_000_000_000)
        else:
            seed = args.seed + round_idx + random.randint(1, 9999)

        print()
        print("-" * 70)
        print(f"[Round {round_idx}] 开始生成")
        print(f"[Round {round_idx}] start={start_date}, days={args.days}, count={batch_count}, seed={seed}")

        clear_dir(tmp_dir)

        try:
            run_generator(
                start=start_date,
                days=args.days,
                count=batch_count,
                outdir=tmp_dir,
                output_format=args.format,
                seed=seed,
            )
        except subprocess.CalledProcessError as exc:
            print(f"[ERROR] 第 {round_idx} 轮生成失败: {exc}")
            print(f"[Stream] 等待 {interval}s 后重试")
            sleep_with_interrupt(interval)
            continue

        stats = append_generated_files(
            tmp_dir=tmp_dir,
            final_dir=outdir,
            output_format=args.format,
        )

        print(f"[Round {round_idx}] 追加完成:")
        print(f"  syslog: {stats['syslog']} 行")
        print(f"  jsonl : {stats['jsonl']} 行")
        print(f"  csv   : {stats['csv']} 行")

        if args.format in ("syslog", "all"):
            final_syslog = outdir / "vpn_logs.log"
            print(f"[Round {round_idx}] 当前 syslog 总行数: {count_lines(final_syslog)}")
            print(f"[Round {round_idx}] Filebeat 应采集文件: {final_syslog}")

        print(f"[Round {round_idx}] 等待 {interval}s 后进入下一轮")
        sleep_with_interrupt(interval)

    print()
    print("[Stream] 已停止")


def sleep_with_interrupt(seconds: int) -> None:
    slept = 0
    while RUNNING and slept < seconds:
        time.sleep(1)
        slept += 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="VPN 日志调度器：支持单次生成和持续随机生成"
    )

    subparsers = parser.add_subparsers(
        dest="mode",
        required=True,
        help="运行模式",
    )

    once = subparsers.add_parser(
        "once",
        help="单次生成日志，等价于直接调用 gen_vpn_logs.py",
    )

    once.add_argument("--start", default="2026-04-01", help="起始日期 YYYY-MM-DD")
    once.add_argument("--days", type=int, default=1, help="生成天数")
    once.add_argument("--count", type=int, default=120, help="每天正常登录条数")
    once.add_argument("--outdir", default="../logs", help="输出目录")
    once.add_argument(
        "--format",
        default="all",
        choices=["csv", "jsonl", "syslog", "all"],
        help="输出格式",
    )
    once.add_argument("--seed", type=int, default=42, help="随机种子")
    once.set_defaults(func=run_once)

    stream = subparsers.add_parser(
        "stream",
        help="持续随机生成日志，并 append 到目标目录",
    )

    stream.add_argument(
        "--start",
        default="today",
        help="起始日期。默认 today 表示使用当天日期；也可传 2026-04-01",
    )
    stream.add_argument("--days", type=int, default=1, help="每轮生成天数")
    stream.add_argument("--outdir", default="./vpn_output", help="最终输出目录")
    stream.add_argument("--tmp-dir", default=None, help="临时输出目录，默认 log-generator/tmp_output")
    stream.add_argument(
        "--format",
        default="all",
        choices=["csv", "jsonl", "syslog", "all"],
        help="输出格式。Filebeat 主要采集 syslog；all 可保留 CSV/JSONL 辅助验证",
    )

    stream.add_argument("--count-min", type=int, default=20, help="每轮最小正常登录条数")
    stream.add_argument("--count-max", type=int, default=80, help="每轮最大正常登录条数")

    stream.add_argument("--interval-min", type=int, default=3, help="最小生成间隔秒数")
    stream.add_argument("--interval-max", type=int, default=8, help="最大生成间隔秒数")

    stream.add_argument(
        "--seed",
        type=int,
        default=None,
        help="基础随机种子。不传则每轮完全随机",
    )

    stream.add_argument(
        "--max-rounds",
        type=int,
        default=0,
        help="最大运行轮数。0 表示无限运行",
    )

    stream.add_argument(
        "--reset",
        action="store_true",
        help="启动前清空目标输出文件",
    )

    stream.set_defaults(func=run_stream)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()