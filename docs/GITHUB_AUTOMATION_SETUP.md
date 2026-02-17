# Simple Free Automation (GitHub Actions)

This is the easiest free setup that runs even when your laptop is off.

## 1) Push this folder to a GitHub repository

If repo is not created yet, create one on GitHub and push `S-PRO` there.

## 2) Add GitHub Secrets

In GitHub repo:
`Settings -> Secrets and variables -> Actions -> New repository secret`

Create these secrets:

- `PIPEDRIVE_DOMAIN` = `s-pro1`
- `PIPEDRIVE_API_TOKEN` = your token
- `SMTP_HOST` = `smtp.gmail.com`
- `SMTP_PORT` = `587`
- `SMTP_USERNAME` = `aburmistrov@s-pro.io`
- `SMTP_PASSWORD` = app password
- `SMTP_SENDER` = `aburmistrov@s-pro.io`

## 3) Enable workflow

Workflow file:
`.github/workflows/pipedrive-reactivation.yml`

It runs:
- Weekdays at 08:00 UTC
- Also manually via `Run workflow`

## 4) What it does each run

1. Builds stale lead queue (`>240 days`)
2. Ranks and keeps top-20% priority
3. Sends daily batch (`30/day`, clean + top20)
4. Generates dashboard JSON + HTML
5. Uploads dashboard as workflow artifacts

## 5) Where to view results

- GitHub Actions run logs
- Artifacts: `reactivation-dashboard`

## Notes

- This is free on GitHub Actions within plan limits.
- If needed, daily limit can be changed in workflow (`--daily-limit 30`).
