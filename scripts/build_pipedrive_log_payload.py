#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from datetime import date, timedelta
from scripts.common import read_csv, write_csv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--contacts", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    contacts = read_csv(args.contacts)
    today = date.today()

    rows = []
    for c in contacts:
        rows.append({
            "company_name": c.get("company_name", ""),
            "full_name": c.get("full_name", ""),
            "email": c.get("email", ""),
            "linkedin_url": c.get("linkedin_url", ""),
            "outreach_status": "queued",
            "last_touch_date": "",
            "next_touch_date": today.isoformat(),
            "reply_type": "",
            "meeting_qualified": "false",
            "next_step": "Send LinkedIn connection note",
            "followup_date_d3": (today + timedelta(days=3)).isoformat(),
            "followup_date_d5": (today + timedelta(days=5)).isoformat(),
        })

    fieldnames = list(rows[0].keys()) if rows else [
        "company_name", "full_name", "email", "linkedin_url", "outreach_status",
        "last_touch_date", "next_touch_date", "reply_type", "meeting_qualified", "next_step",
        "followup_date_d3", "followup_date_d5"
    ]
    write_csv(args.output, rows, fieldnames)
    print(f"Prepared Pipedrive log payload for {len(rows)} contacts.")


if __name__ == "__main__":
    main()
