#!/usr/bin/env bash
set -Eeuo pipefail

# Stop local background processes started by scripts/start_all.sh.
# Usage:
#   bash scripts/stop_all.sh
#   bash scripts/stop_all.sh --with-docker

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

RUNTIME_DIR="${PROJECT_ROOT}/runtime"
PID_DIR="${RUNTIME_DIR}/pids"
WITH_DOCKER=0

timestamp() { date '+%Y-%m-%d %H:%M:%S'; }
info() { echo "[$(timestamp)] [INFO] $*"; }
ok() { echo "[$(timestamp)] [OK] $*"; }
warn() { echo "[$(timestamp)] [WARN] $*"; }

stop_pid() {
  local name="$1"
  local pid_file="${PID_DIR}/${name}.pid"

  if [[ ! -f "${pid_file}" ]]; then
    warn "${name}: pid file not found"
    return 0
  fi

  local pid
  pid="$(cat "${pid_file}" 2>/dev/null || true)"

  if [[ -z "${pid}" ]]; then
    warn "${name}: empty pid file"
    rm -f "${pid_file}"
    return 0
  fi

  if ! kill -0 "${pid}" >/dev/null 2>&1; then
    warn "${name}: process not running, pid=${pid}"
    rm -f "${pid_file}"
    return 0
  fi

  info "Stopping ${name}, pid=${pid} ..."
  kill "${pid}" >/dev/null 2>&1 || true

  for _ in $(seq 1 10); do
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      rm -f "${pid_file}"
      ok "${name} stopped"
      return 0
    fi
    sleep 1
  done

  warn "${name}: still running after graceful stop, sending SIGKILL"
  kill -9 "${pid}" >/dev/null 2>&1 || true
  rm -f "${pid_file}"
  ok "${name} killed"
}

for arg in "$@"; do
  case "${arg}" in
    --with-docker)
      WITH_DOCKER=1
      ;;
    *)
      warn "Unknown argument ignored: ${arg}"
      ;;
  esac
done

stop_pid "log-generator"
stop_pid "ueba-consumer"
stop_pid "streamlit"

if [[ "${WITH_DOCKER}" == "1" ]]; then
  info "Stopping Docker Compose services ..."
  docker compose --profile extras stop || docker compose stop || true
  ok "Docker stop command completed"
else
  echo
  info "Docker services were left running."
  info "To stop them as well, run:"
  echo "  bash scripts/stop_all.sh --with-docker"
fi
