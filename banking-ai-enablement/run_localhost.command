#!/usr/bin/env bash
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
./start_localhost.sh 8123
PORT="$(cat "${DIR}/.server.port")"
open "http://127.0.0.1:${PORT}"
