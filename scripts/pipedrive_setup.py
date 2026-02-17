#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import json
import os
from urllib import request, parse

FIELD_DEFS = [
    ("company_ch_presence", "enum", ["HQ", "major_office"]),
    ("icp_segment", "enum", ["FSI", "Pharma/MedTech", "Industrial", "Retail/Services", "Tech-enabled services", "Other"]),
    ("ai_use_case_hypothesis", "text", None),
    ("outreach_language", "enum", ["EN", "DE", "FR"]),
    ("outreach_score", "double", None),
    ("outreach_status", "enum", ["queued", "step1_sent", "step2_sent", "replied", "stopped"]),
    ("last_touch_date", "date", None),
    ("next_touch_date", "date", None),
    ("reply_type", "enum", ["positive", "neutral", "referral", "not_now", "no_fit"]),
    ("meeting_qualified", "enum", ["true", "false"]),
]

PIPELINE_STAGES = [
    "Company Longlist",
    "Company Shortlist",
    "Outreach Queued",
    "Contacted",
    "Replied",
    "Qualified Meeting",
    "Opportunity",
]


def api_post(url, payload):
    req = request.Request(url, data=json.dumps(payload).encode("utf-8"), method="POST")
    req.add_header("Content-Type", "application/json")
    with request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["dry-run", "apply"], default="dry-run")
    args = ap.parse_args()

    token = os.getenv("PIPEDRIVE_API_TOKEN", "")
    domain = os.getenv("PIPEDRIVE_DOMAIN", "")

    print("Planned custom fields:")
    for f in FIELD_DEFS:
        print("-", f)

    print("Planned pipeline stages:")
    for s in PIPELINE_STAGES:
        print("-", s)

    if args.mode == "dry-run":
        return

    if not token or not domain:
        raise SystemExit("Set PIPEDRIVE_API_TOKEN and PIPEDRIVE_DOMAIN first.")

    base = f"https://{domain}.pipedrive.com/api/v1"
    token_q = parse.urlencode({"api_token": token})

    # create pipeline
    p_res = api_post(f"{base}/pipelines?{token_q}", {"name": "Swiss Enterprise Outreach"})
    pipeline_id = p_res.get("data", {}).get("id")
    if not pipeline_id:
        raise SystemExit(f"Failed to create pipeline: {p_res}")

    for idx, stage in enumerate(PIPELINE_STAGES, start=1):
        api_post(
            f"{base}/stages?{token_q}",
            {"name": stage, "pipeline_id": pipeline_id, "order_nr": idx * 10},
        )

    # create deal fields for outreach tracking
    for key, field_type, options in FIELD_DEFS:
        payload = {"name": key, "field_type": field_type}
        if options:
            payload["options"] = [{"label": o} for o in options]
        api_post(f"{base}/dealFields?{token_q}", payload)

    print("Applied pipeline and fields in Pipedrive.")


if __name__ == "__main__":
    main()
