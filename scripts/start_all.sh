#!/usr/bin/env bash
set -Eeuo pipefail

# log-ai-assistant one-command startup script.
# Usage:
#   bash scripts/start_all.sh
#
# Optional environment variables:
#   STREAMLIT_PORT=8501
#   LOG_COUNT_MIN=20
#   LOG_COUNT_MAX=80
#   LOG_INTERVAL_MIN=3
#   LOG_INTERVAL_MAX=8
#   LOG_RESET=0
#   BUILD_BASELINE=auto   # auto|1|0
#   BOOTSTRAP_BASELINE=0  # 1 to run a one-off bootstrap ingest before baseline build
#
# Notes:
# - Runtime pid/log files are stored under runtime/ and should not be committed to Git.
# - The script will attempt to keep only one running Flink job with the expected name.
# - If a background process was started manually, the script will reuse it and record its PID.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

VENV_DIR="${PROJECT_ROOT}/.venv"
RUNTIME_DIR="${PROJECT_ROOT}/runtime"
PID_DIR="${RUNTIME_DIR}/pids"
LOG_DIR="${RUNTIME_DIR}/logs"

STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"
LOG_COUNT_MIN="${LOG_COUNT_MIN:-20}"
LOG_COUNT_MAX="${LOG_COUNT_MAX:-80}"
LOG_INTERVAL_MIN="${LOG_INTERVAL_MIN:-3}"
LOG_INTERVAL_MAX="${LOG_INTERVAL_MAX:-8}"
LOG_RESET="${LOG_RESET:-0}"
BUILD_BASELINE="${BUILD_BASELINE:-auto}"
BOOTSTRAP_BASELINE="${BOOTSTRAP_BASELINE:-0}"
CONSUMER_GROUP_ID="${CONSUMER_GROUP_ID:-es-consumer-ueba-v1}"
BOOTSTRAP_CONSUMER_GROUP_ID="${BOOTSTRAP_CONSUMER_GROUP_ID:-es-bootstrap-baseline}"
FLINK_JOB_NAME="${FLINK_JOB_NAME:-docker_flink_raw_logs_to_parsed_logs}"

mkdir -p "${PID_DIR}" "${LOG_DIR}"

timestamp() { date '+%Y-%m-%d %H:%M:%S'; }
info() { echo "[$(timestamp)] [INFO] $*"; }
ok() { echo "[$(timestamp)] [OK] $*"; }
warn() { echo "[$(timestamp)] [WARN] $*"; }
fail() { echo "[$(timestamp)] [ERROR] $*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Command not found: $1"
}

source_venv() {
  [[ -d "${VENV_DIR}" ]] || fail "Python virtual environment not found: ${VENV_DIR}"
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
}

is_pid_running() {
  local pid_file="$1"
  [[ -f "${pid_file}" ]] || return 1

  local pid
  pid="$(cat "${pid_file}" 2>/dev/null || true)"
  [[ -n "${pid}" ]] || return 1
  kill -0 "${pid}" >/dev/null 2>&1
}

record_pid_from_pattern() {
  local pid_file="$1"
  local pattern="$2"
  local pid
  pid="$(pgrep -f "${pattern}" | head -n1 || true)"
  if [[ -n "${pid}" ]]; then
    echo "${pid}" > "${pid_file}"
    return 0
  fi
  return 1
}

start_background() {
  local name="$1"
  local pattern="$2"
  shift 2

  local pid_file="${PID_DIR}/${name}.pid"
  local log_file="${LOG_DIR}/${name}.log"

  if is_pid_running "${pid_file}"; then
    local pid
    pid="$(cat "${pid_file}")"
    ok "${name} already running, pid=${pid}"
    return 0
  fi

  if record_pid_from_pattern "${pid_file}" "${pattern}"; then
    local pid
    pid="$(cat "${pid_file}")"
    ok "${name} already running (discovered by process scan), pid=${pid}"
    return 0
  fi

  rm -f "${pid_file}"
  info "Starting ${name} ..."
  info "Log file: ${log_file}"

  nohup "$@" >> "${log_file}" 2>&1 &
  local pid="$!"
  echo "${pid}" > "${pid_file}"

  sleep 1

  if is_pid_running "${pid_file}"; then
    ok "${name} started, pid=${pid}"
  else
    warn "${name} may have exited immediately. Check: ${log_file}"
  fi
}

http_get_json() {
  local url="$1"
  python - "$url" <<'PY'
import json
import sys
from urllib.request import Request, urlopen

url = sys.argv[1]
req = Request(url, headers={"Content-Type": "application/json"})
with urlopen(req, timeout=5) as resp:
    data = json.loads(resp.read().decode("utf-8"))
print(json.dumps(data, ensure_ascii=False))
PY
}

es_count() {
  local index="$1"
  local url="http://localhost:9200/${index}/_count"
  python - "$url" <<'PY'
import json
import sys
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

url = sys.argv[1]
try:
    req = Request(url, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=5) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    print(data.get("count", 0))
except Exception:
    print("")
PY
}

service_up() {
  local container_name="$1"
  docker ps --format '{{.Names}}' | grep -qx "${container_name}"
}

wait_for_container() {
  local container_name="$1"
  local max_wait_seconds="${2:-60}"
  local waited=0

  while [[ "${waited}" -lt "${max_wait_seconds}" ]]; do
    if service_up "${container_name}"; then
      ok "Container is running: ${container_name}"
      return 0
    fi
    sleep 2
    waited=$((waited + 2))
  done

  warn "Container not detected after ${max_wait_seconds}s: ${container_name}"
  return 1
}

init_project() {
  info "Initializing Elasticsearch indices ..."
  python src/main.py init
  ok "Elasticsearch indices initialized"
}

start_docker_services() {
  info "Starting Docker base services ..."
  docker compose up -d
  ok "Docker base services started"

  wait_for_container "kafka" 60 || true
  wait_for_container "elasticsearch" 60 || true
  wait_for_container "flink-jobmanager" 60 || true
  wait_for_container "flink-taskmanager" 60 || true

  info "Starting Filebeat profile ..."
  docker compose --profile extras up -d filebeat
  wait_for_container "filebeat" 60 || true
  ok "Filebeat start command completed"
}

get_running_flink_jobs() {
  docker exec flink-jobmanager flink list -a 2>/dev/null \
    | awk -v name="${FLINK_JOB_NAME}" '$0 ~ name && $0 ~ /\(RUNNING\)/ { gsub(/^[[:space:]]+|[[:space:]]+$/, "", $1); gsub(/:$/, "", $1); print $1 }'
}

ensure_single_flink_job() {
  if ! service_up "flink-jobmanager"; then
    warn "Flink JobManager is not running; skip Flink job checks"
    return 0
  fi

  mapfile -t running_jobs < <(get_running_flink_jobs || true)
  local count="${#running_jobs[@]}"

  if [[ "${count}" -eq 0 ]]; then
    info "Submitting Flink raw_logs -> parsed_logs job ..."
    bash scripts/submit_flink_raw_to_parsed.sh
    sleep 3
    mapfile -t running_jobs < <(get_running_flink_jobs || true)
    count="${#running_jobs[@]}"
  fi

  if [[ "${count}" -gt 1 ]]; then
    local keep_id="${running_jobs[$((count - 1))]}"
    warn "Multiple Flink jobs detected for ${FLINK_JOB_NAME}. Keeping latest job id=${keep_id} and cancelling the rest."
    for job_id in "${running_jobs[@]}"; do
      if [[ "${job_id}" != "${keep_id}" ]]; then
        docker exec flink-jobmanager flink cancel "${job_id}" >/dev/null 2>&1 || true
      fi
    done
    sleep 5
  fi

  mapfile -t running_jobs < <(get_running_flink_jobs || true)
  count="${#running_jobs[@]}"

  if [[ "${count}" -eq 1 ]]; then
    ok "Flink job is running: ${FLINK_JOB_NAME} (job_id=${running_jobs[0]})"
  elif [[ "${count}" -gt 1 ]]; then
    warn "Flink job duplicates still exist after cleanup. Check Flink dashboard."
  else
    warn "No running Flink job confirmed after submit/cleanup. Check Flink dashboard/logs."
  fi
}

maybe_bootstrap_and_build_baseline() {
  local baseline_count="$(es_count "user-baselines")"

  if [[ "${BOOTSTRAP_BASELINE}" == "1" ]]; then
    info "Bootstrap ingest is enabled. Waiting briefly for Filebeat/Flink to move generated logs ..."
    sleep 12
    info "Running one-off bootstrap ingest ..."
    set +e
    timeout 25s python src/main.py consume-to-es --idle-timeout-ms 5000 --group-id "${BOOTSTRAP_CONSUMER_GROUP_ID}" >> "${LOG_DIR}/bootstrap-ingest.log" 2>&1
    local code="$?"
    set -e
    if [[ "${code}" == "0" || "${code}" == "124" ]]; then
      ok "Bootstrap ingest completed or timed out as expected"
    else
      warn "Bootstrap ingest exited with code ${code}. Check: ${LOG_DIR}/bootstrap-ingest.log"
    fi
  fi

  case "${BUILD_BASELINE}" in
    1)
      info "Building UEBA user baselines (forced) ..."
      python src/main.py build-baseline || warn "UEBA baseline build failed; see logs above"
      ok "UEBA baseline build step finished"
      ;;
    0)
      info "Skipping baseline build because BUILD_BASELINE=0"
      ;;
    auto)
      if [[ -z "${baseline_count}" || "${baseline_count}" == "0" ]]; then
        info "No user-baselines found, building UEBA baselines ..."
        python src/main.py build-baseline || warn "UEBA baseline build failed; see logs above"
        ok "UEBA baseline build step finished"
      else
        ok "user-baselines already exist (${baseline_count}); skip baseline build"
      fi
      ;;
    *)
      warn "Unknown BUILD_BASELINE=${BUILD_BASELINE}; skip baseline build"
      ;;
  esac
}

start_log_generator() {
  local reset_flag=()
  if [[ "${LOG_RESET}" == "1" ]]; then
    reset_flag=(--reset)
  fi

  start_background \
    "log-generator" \
    "realtime_scheduler.py stream" \
    bash -lc "cd '${PROJECT_ROOT}/log-generator' && exec '${VENV_DIR}/bin/python' realtime_scheduler.py stream --outdir ../logs --format all --count-min '${LOG_COUNT_MIN}' --count-max '${LOG_COUNT_MAX}' --interval-min '${LOG_INTERVAL_MIN}' --interval-max '${LOG_INTERVAL_MAX}' ${reset_flag[*]}"
}

start_ueba_consumer() {
  start_background \
    "ueba-consumer" \
    "src/main.py consume-to-es" \
    python src/main.py consume-to-es --idle-timeout-ms -1 --group-id "${CONSUMER_GROUP_ID}"
}

start_dashboard() {
  start_background \
    "streamlit" \
    "streamlit run src/dashboard/app.py" \
    streamlit run src/dashboard/app.py --server.port "${STREAMLIT_PORT}" --server.headless true
}

main() {
  require_cmd docker
  require_cmd bash
  require_cmd timeout
  require_cmd pgrep

  source_venv
  require_cmd python
  require_cmd streamlit

  info "Project root: ${PROJECT_ROOT}"
  info "Runtime directory: ${RUNTIME_DIR}"

  start_docker_services
  init_project
  ensure_single_flink_job
  start_log_generator
  maybe_bootstrap_and_build_baseline
  start_ueba_consumer
  start_dashboard

  echo
  ok "All startup commands completed."
  echo
  echo "Dashboard:"
  echo "  http://localhost:${STREAMLIT_PORT}"
  echo
  echo "Useful commands:"
  echo "  bash scripts/status_all.sh"
  echo "  bash scripts/stop_all.sh"
  echo
  echo "Runtime logs:"
  echo "  ${LOG_DIR}"
}

main "$@"

