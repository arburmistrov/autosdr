#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="${ROOT_DIR}/.server.pid"
PORT_FILE="${ROOT_DIR}/.server.port"

if [[ ! -f "${PID_FILE}" ]]; then
  echo "No PID file found. Server is not running."
  exit 0
fi

PID="$(cat "${PID_FILE}")"
if kill -0 "${PID}" >/dev/null 2>&1; then
  kill "${PID}"
  echo "Stopped server PID ${PID}."
else
  echo "Process ${PID} not running."
fi

rm -f "${PID_FILE}"
rm -f "${PORT_FILE}"
