# Reconnect SaaS v7 (Real OAuth + Real Queue)

This version removes Google Console steps from end users.

## 1) One-time admin setup

Set environment variables on server:

```bash
export APP_BASE_URL="https://your-domain.com"
export GOOGLE_OAUTH_CLIENT_ID="...apps.googleusercontent.com"
export GOOGLE_OAUTH_CLIENT_SECRET="..."
export GOOGLE_OAUTH_REDIRECT_URI="https://your-domain.com/api/auth/google/callback"
```

In Google Cloud OAuth client (Web application), add:
- Authorized JavaScript origins: `https://your-domain.com`
- Authorized redirect URIs: `https://your-domain.com/api/auth/google/callback`

## 2) Run locally

```bash
pip install -r requirements.txt
export APP_BASE_URL="http://127.0.0.1:8080"
export GOOGLE_OAUTH_CLIENT_ID="...apps.googleusercontent.com"
export GOOGLE_OAUTH_CLIENT_SECRET="..."
export GOOGLE_OAUTH_REDIRECT_URI="http://127.0.0.1:8080/api/auth/google/callback"
uvicorn apps.reconnect_saas_v7.main:app --host 0.0.0.0 --port 8080
```

Open: `http://127.0.0.1:8080`

## 3) User flow

1. Save user name + email.
2. Click `Connect my Gmail` and consent in Google.
3. Save Pipedrive domain + API token.
4. Click `Generate from my Gmail history`.

Queue is generated from the connected user's mailbox (organization-level rows).
