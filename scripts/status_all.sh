#!/usr/bin/env bash
set -Eeuo pipefail

# Show project runtime status.
# Usage:
#   bash scripts/status_all.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

VENV_DIR="${PROJECT_ROOT}/.venv"
RUNTIME_DIR="${PROJECT_ROOT}/runtime"
PID_DIR="${RUNTIME_DIR}/pids"
LOG_DIR="${RUNTIME_DIR}/logs"

STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"
FLINK_JOB_NAME="${FLINK_JOB_NAME:-docker_flink_raw_logs_to_parsed_logs}"

timestamp() { date '+%Y-%m-%d %H:%M:%S'; }
section() { echo; echo "===== $* ====="; }

pid_status() {
  local name="$1"
  local pid_file="${PID_DIR}/${name}.pid"

  if [[ ! -f "${pid_file}" ]]; then
    printf "%-18s %s\n" "${name}" "not started"
    return 0
  fi

  local pid
  pid="$(cat "${pid_file}" 2>/dev/null || true)"

  if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
    printf "%-18s running, pid=%s\n" "${name}" "${pid}"
  else
    printf "%-18s stopped or stale pid=%s\n" "${name}" "${pid:-N/A}"
  fi
}

es_http_get() {
  local path="$1"
  python - "$path" <<'PY'
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

path = sys.argv[1]
url = f"http://localhost:9200{path}"
try:
    req = Request(url, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=5) as resp:
        print(resp.read().decode("utf-8"))
except Exception as exc:
    print(json.dumps({"error": str(exc)}))
PY
}

show_es_counts() {
  python - <<'PY' || true
import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.config import settings

indices = [
    ("security-logs", settings.elasticsearch_log_index),
    ("security-alerts", settings.elasticsearch_alert_index),
    ("user-baselines", settings.elasticsearch_baseline_index),
    ("ai-reports", settings.elasticsearch_ai_index),
]

for label, index in indices:
    try:
        req = Request(f"http://localhost:9200/{index}/_count", headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        print(f"{label:<18} {data.get('count', 0)}")
    except Exception as exc:
        print(f"{label:<18} ERROR: {exc}")
PY
}

git_status_block() {
  section "Git"
  git status -sb || true
  git log --oneline -1 || true
}

docker_block() {
  section "Docker Compose"
  docker compose ps || true
}

flink_block() {
  section "Flink Jobs"
  if docker ps --format '{{.Names}}' | grep -qx "flink-jobmanager"; then
    local output
    output="$(docker exec flink-jobmanager flink list -a 2>/dev/null || true)"
    printf '%s\n' "${output}"

    local count
    count="$(printf '%s\n' "${output}" | awk -v name="${FLINK_JOB_NAME}" '$0 ~ name && $0 ~ /\(RUNNING\)/ {c++} END {print c+0}')"
    if [[ "${count}" -gt 1 ]]; then
      echo "[WARN] duplicate RUNNING jobs detected for ${FLINK_JOB_NAME}: ${count}"
    elif [[ "${count}" -eq 1 ]]; then
      echo "[OK] ${FLINK_JOB_NAME} is RUNNING"
    else
      echo "[WARN] ${FLINK_JOB_NAME} is not confirmed RUNNING"
    fi
  else
    echo "[WARN] flink-jobmanager container is not running"
  fi
}

local_process_block() {
  section "Local Background Processes"
  pid_status "log-generator"
  pid_status "ueba-consumer"
  pid_status "streamlit"
}

es_block() {
  section "Elasticsearch Counts"
  if [[ -d "${VENV_DIR}" ]]; then
    # shellcheck disable=SC1091
    source "${VENV_DIR}/bin/activate"
    show_es_counts
  else
    echo "[WARN] .venv not found, skipped ES counts"
  fi
}

urls_block() {
  section "Useful URLs"
  echo "Streamlit dashboard: http://localhost:${STREAMLIT_PORT}"
  echo "Runtime logs:        ${LOG_DIR}"
}

echo "Status time: $(timestamp)"
echo "Project root: ${PROJECT_ROOT}"

git_status_block
docker_block
flink_block
local_process_block
es_block
urls_block

