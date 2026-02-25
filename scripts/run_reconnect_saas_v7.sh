#!/usr/bin/env bash
set -euo pipefail

export APP_BASE_URL="${APP_BASE_URL:-http://127.0.0.1:8080}"
export GOOGLE_OAUTH_CLIENT_ID="${GOOGLE_OAUTH_CLIENT_ID:-}"
export GOOGLE_OAUTH_CLIENT_SECRET="${GOOGLE_OAUTH_CLIENT_SECRET:-}"
export GOOGLE_OAUTH_REDIRECT_URI="${GOOGLE_OAUTH_REDIRECT_URI:-${APP_BASE_URL}/api/auth/google/callback}"

python3 -m uvicorn apps.reconnect_saas_v7.main:app --host 0.0.0.0 --port 8080
