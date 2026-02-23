#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="${ROOT_DIR}/.server.pid"
LOG_FILE="${ROOT_DIR}/.server.log"
PORT="${1:-8080}"

if [[ -f "${PID_FILE}" ]]; then
  OLD_PID="$(cat "${PID_FILE}")"
  if kill -0 "${OLD_PID}" >/dev/null 2>&1; then
    echo "Server already running on PID ${OLD_PID}."
    echo "Open: http://127.0.0.1:${PORT}"
    exit 0
  fi
fi

cd "${ROOT_DIR}"
nohup python3 -m http.server "${PORT}" --bind 127.0.0.1 --directory "${ROOT_DIR}" >"${LOG_FILE}" 2>&1 &
PID=$!
echo "${PID}" > "${PID_FILE}"
sleep 1

if kill -0 "${PID}" >/dev/null 2>&1; then
  echo "Started on http://127.0.0.1:${PORT}"
  echo "PID: ${PID}"
  echo "Log: ${LOG_FILE}"
else
  echo "Failed to start. Check log: ${LOG_FILE}"
  exit 1
fi
