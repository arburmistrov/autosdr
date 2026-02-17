# S-PRO Swiss Enterprise Outreach Automation

Company-first outreach workflow for Swiss enterprise targets with a gate on the first 10 contacts.

## What this implements

- Build a **100-company longlist** from Sales Navigator export
- Score and cut to **Top-30 companies**
- Select **1-2 decision makers** per Top-30 company
- Prepare **first-10 review package** (table + 3 outreach drafts per contact)
- Prepare CSV payload for **Pipedrive logging**

## Workflow

1. Put Sales Navigator company export into `data/input/salesnav_companies.csv`
2. Put contact export into `data/input/salesnav_contacts.csv`
3. Run:

```bash
python3 scripts/build_company_longlist.py \
  --input data/input/salesnav_companies.csv \
  --output data/output/company_longlist_scored.csv \
  --top30 data/output/company_top30.csv

python3 scripts/select_top_contacts.py \
  --companies data/output/company_top30.csv \
  --contacts data/input/salesnav_contacts.csv \
  --output data/output/contact_candidates.csv \
  --first10 data/output/contact_first10.csv

python3 scripts/generate_outreach_pack.py \
  --contacts data/output/contact_first10.csv \
  --output data/output/first10_review_pack.csv

python3 scripts/build_pipedrive_log_payload.py \
  --contacts data/output/contact_first10.csv \
  --output data/output/pipedrive_touch_log.csv
```

## Optional Pipedrive sync

Set env vars:

```bash
export PIPEDRIVE_API_TOKEN="..."
export PIPEDRIVE_DOMAIN="yourcompany"  # from yourcompany.pipedrive.com
```

Then run:

```bash
python3 scripts/pipedrive_setup.py --mode dry-run
python3 scripts/pipedrive_setup.py --mode apply
```

`dry-run` prints intended custom fields and pipeline stages.

## Input schema

See templates:
- `data/input/salesnav_companies.template.csv`
- `data/input/salesnav_contacts.template.csv`

## Notes

- Sending on LinkedIn/email remains manual (human-in-the-loop).
- This project does not bypass platform rules or do risky scraping.

## Operator mode (fast manual sending)

For LinkedIn anti-bot-safe sending with minimal manual effort:

```bash
python3 scripts/operator_send_queue.py \
  --input data/output/first10_review_pack.csv \
  --limit 10
```

What it does per contact:
- opens profile URL,
- copies personalized message to clipboard,
- tracks progress in `data/output/operator_send_state.json`.

You only do: `Message -> Cmd+V -> Send -> s`.
