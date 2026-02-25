# Deploy Reconnect SaaS v7 on Render (Permanent URL)

## Why this

- Permanent HTTPS URL (no unstable tunnel URLs).
- End users only click `Connect my Gmail`.
- No Google Console steps for end users.

## 1) Deploy (2 clicks)

1. Open: `https://render.com/deploy?repo=https://github.com/arburmistrov/autosdr`
2. Confirm service `reconnect-saas-v7` from `render.yaml`.

Render will build and give a permanent URL like:
- `https://reconnect-saas-v7.onrender.com`

## 2) Set secrets in Render

In service settings -> Environment, set:
- `GOOGLE_OAUTH_CLIENT_ID`
- `GOOGLE_OAUTH_CLIENT_SECRET`

## 3) One-time Google OAuth setup (admin only)

In Google Cloud Console -> OAuth Client (Web application):

- Authorized JavaScript origins:
  - `https://reconnect-saas-v7.onrender.com`
- Authorized redirect URIs:
  - `https://reconnect-saas-v7.onrender.com/api/auth/google/callback`

If OAuth app is in `Testing`, add team emails in `Test users`.

## 4) Test flow

1. Open product URL.
2. Save user name + email.
3. Click `Connect my Gmail`.
4. Save Pipedrive domain + token.
5. Click `Generate from my Gmail history`.

## Notes

- Free Render instances can sleep when idle.
- SQLite on Render free plan is ephemeral (`/tmp`). For persistence, switch to Postgres later.
