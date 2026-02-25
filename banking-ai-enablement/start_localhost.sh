#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="${ROOT_DIR}/.server.pid"
LOG_FILE="${ROOT_DIR}/.server.log"
PORT_FILE="${ROOT_DIR}/.server.port"
BASE_PORT="${1:-8123}"

find_free_port() {
  local port="$1"
  while lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; do
    port=$((port + 1))
  done
  echo "${port}"
}

is_listening() {
  local port="$1"
  lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
}

if [[ -f "${PID_FILE}" ]]; then
  OLD_PID="$(cat "${PID_FILE}")"
  PORT="${BASE_PORT}"
  if [[ -f "${PORT_FILE}" ]]; then
    PORT="$(cat "${PORT_FILE}")"
  fi
  if kill -0 "${OLD_PID}" >/dev/null 2>&1 && is_listening "${PORT}"; then
    echo "Server already running on PID ${OLD_PID}."
    echo "Open: http://127.0.0.1:${PORT}"
    exit 0
  else
    rm -f "${PID_FILE}"
    rm -f "${PORT_FILE}"
  fi
fi

PORT="$(find_free_port "${BASE_PORT}")"
cd "${ROOT_DIR}"
nohup python3 -m http.server "${PORT}" --bind 127.0.0.1 --directory "${ROOT_DIR}" >"${LOG_FILE}" 2>&1 &
PID=$!
echo "${PID}" > "${PID_FILE}"
echo "${PORT}" > "${PORT_FILE}"
sleep 1

if kill -0 "${PID}" >/dev/null 2>&1 && is_listening "${PORT}"; then
  echo "Started on http://127.0.0.1:${PORT}"
  echo "PID: ${PID}"
  echo "Log: ${LOG_FILE}"
else
  echo "Failed to start. Check log: ${LOG_FILE}"
  exit 1
fi
