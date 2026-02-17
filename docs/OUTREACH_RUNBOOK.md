# Outreach Runbook (Swiss Enterprise, First-10 Gate)

## Sales Navigator setup

Create saved searches for companies with filters:
- Geography: Switzerland
- Company size: 200+ (priority: 1000+)
- Industries: FSI, Pharma/MedTech, Industrial, Retail/Services, Tech-enabled services
- Presence rule: HQ in CH or major office in CH

Export company results to `data/input/salesnav_companies.csv` using the template headers.

## Contact selection in Sales Navigator

From Top-30 companies, export contacts with roles:
- Director, VP, Head, CIO, CTO, CDO, Chief*
- Functions: IT, Digital, Innovation, Operations, Product

Save to `data/input/salesnav_contacts.csv` using template headers.

## Execution commands

```bash
python3 scripts/build_company_longlist.py --input data/input/salesnav_companies.csv --output data/output/company_longlist_scored.csv --top30 data/output/company_top30.csv
python3 scripts/select_top_contacts.py --companies data/output/company_top30.csv --contacts data/input/salesnav_contacts.csv --output data/output/contact_candidates.csv --first10 data/output/contact_first10.csv
python3 scripts/generate_outreach_pack.py --contacts data/output/contact_first10.csv --output data/output/first10_review_pack.csv --sender-name "Arseniy Burmistrov"
python3 scripts/build_pipedrive_log_payload.py --contacts data/output/contact_first10.csv --output data/output/pipedrive_touch_log.csv
```

## Review gate before sending

Review `data/output/first10_review_pack.csv`.

Checklist:
- Every row has one concrete company signal in `why_fit`
- Neutral enterprise-safe tone
- Explicit soft opt-out in email

Only after approval, send first 10 manually in order of `outreach_score`.

## Logging into Pipedrive

Import `data/output/pipedrive_touch_log.csv` and map fields:
- outreach_status
- last_touch_date
- next_touch_date
- reply_type
- meeting_qualified

After each touch, update status and next date.
