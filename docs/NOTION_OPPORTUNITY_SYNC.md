# Notion Opportunity Sync (Pipedrive -> Notion)

## Purpose

Hourly one-way sync from Pipedrive deals into Notion `Opportunities` board.

## Required secrets

- `PIPEDRIVE_DOMAIN`
- `PIPEDRIVE_API_TOKEN`
- `NOTION_API_TOKEN`
- `NOTION_DATABASE_ID`

## CLI

Dry run:

```bash
python3 scripts/sync_pipedrive_to_notion_opportunities.py --dry-run
```

Apply:

```bash
python3 scripts/sync_pipedrive_to_notion_opportunities.py --apply
```

## Stage mapping

Configured in `config/notion_stage_map.json`.

- Pre-opportunity stages -> `Future pipeline`
- `Opportunity` -> `Scope Definition`
- `Estimation` -> `Estimation`
- `Validation` -> `Validation`
- `Presented` -> `Presented`
- `Potential 80%+` -> `Potential 80%+`
- `Won` -> `Won`

## Readiness and hard rollback

Configured in `config/readiness_rules.json`.

Stage gates:

- To `Estimation`: `brief + scope + owner + deadline + budget`
- To `Validation`: `estimate link`
- To `Presented`: `presentation link`

If gate fails, sync keeps board consistent by rolling the stage back to previous allowed stage and writing reason to `Sync Notes`.

## Derived fields

- `Days in Stage`
- `SLA Color`: Green (`<=3`), Yellow (`4-7`), Red (`>7`)
- `Readiness %`
- `Docs Status`: `No links` / `Partial` / `Complete`

## Manual future fields policy

`Future Size`, `Future Domain`, `Future Confidence` are intentionally not overwritten by sync.

## Output report

Each run writes:

- `data/output/notion_sync_report.json`

Includes planned/actual create-update counts, blocked cards, skipped properties and errors.
