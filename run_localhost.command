#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT="${1:-8080}"

cd "${ROOT_DIR}"
echo "Starting server at http://127.0.0.1:${PORT}"
open "http://127.0.0.1:${PORT}" || true
python3 -m http.server "${PORT}" --bind 127.0.0.1 --directory "${ROOT_DIR}"
