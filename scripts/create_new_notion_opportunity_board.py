#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.sync_pipedrive_to_notion_opportunities import (
    DEFAULT_READINESS,
    DEFAULT_STAGE_MAP,
    DEFAULT_SYNC_CONFIG,
    NotionClient,
    load_json,
    run_sync,
)

DEFAULT_OUTPUT = ROOT / "data" / "output" / "new_notion_board.json"


def notion_url_from_id(db_id: str) -> str:
    return f"https://www.notion.so/{db_id.replace('-', '')}"


def build_board_properties(stage_order):
    stage_options = [{"name": s} for s in stage_order]
    return {
        "Name": {"title": {}},
        "CRM Deal ID": {"number": {"format": "number"}},
        "Stage": {"select": {"options": stage_options}},
        "Pipeline": {"rich_text": {}},
        "Company": {"rich_text": {}},
        "Contact": {"rich_text": {}},
        "Owner": {"rich_text": {}},
        "Deal Value": {"number": {"format": "number_with_commas"}},
        "Currency": {"select": {"options": [{"name": "EUR"}, {"name": "USD"}, {"name": "CHF"}, {"name": "GBP"}]}},
        "Expected Close Date": {"date": {}},
        "Pipedrive URL": {"url": {}},
        "Days in Stage": {"number": {"format": "number"}},
        "SLA Color": {"select": {"options": [{"name": "Green"}, {"name": "Yellow"}, {"name": "Red"}]}},
        "Readiness %": {"number": {"format": "percent"}},
        "Gate Status": {"select": {"options": [{"name": "Pass"}, {"name": "Blocked"}]}},
        "Sync Notes": {"rich_text": {}},
        "Docs Status": {"select": {"options": [{"name": "No links"}, {"name": "Partial"}, {"name": "Complete"}]}},
        "Brief Link": {"url": {}},
        "Scope Link": {"url": {}},
        "Estimate Link": {"url": {}},
        "Presentation Link": {"url": {}},
        "Last Sync At": {"date": {}},
        "Size": {"select": {"options": [{"name": "S"}, {"name": "M"}, {"name": "L"}]}},
        "Domain": {"multi_select": {"options": [{"name": "Mob"}, {"name": "Web"}, {"name": "Blockchain"}]}},
        "Confidence": {"select": {"options": [{"name": "Low"}, {"name": "Medium"}, {"name": "High"}]}},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--board-name", default="Opportunities - CRM Sync")
    ap.add_argument("--parent-page-id", default="")
    ap.add_argument("--max-deals", type=int, default=120)
    ap.add_argument("--skip-seed", action="store_true", help="Create empty board only, without initial CRM sync")
    ap.add_argument("--config", default=str(DEFAULT_SYNC_CONFIG))
    ap.add_argument("--stage-map", default=str(DEFAULT_STAGE_MAP))
    ap.add_argument("--readiness", default=str(DEFAULT_READINESS))
    ap.add_argument("--report", default="data/output/notion_sync_report.json")
    ap.add_argument("--output-json", default=str(DEFAULT_OUTPUT))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    notion_token = os.getenv("NOTION_API_TOKEN", "").strip()
    source_db = os.getenv("NOTION_DATABASE_ID", "").strip()
    if not notion_token or not source_db:
        raise SystemExit("Missing NOTION_API_TOKEN or NOTION_DATABASE_ID")

    sync_cfg = load_json(Path(args.config))
    stage_cfg = load_json(Path(args.stage_map))
    notion = NotionClient(
        notion_token,
        timeout_sec=int(sync_cfg.get("request_timeout_sec", 60)),
        max_retries=int(sync_cfg.get("max_retries", 4)),
        backoff_sec=float(sync_cfg.get("retry_backoff_sec", 1.5)),
    )

    parent_page_id = (args.parent_page_id or os.getenv("NOTION_PARENT_PAGE_ID", "")).strip()
    source_meta = notion.get_database(source_db)
    if not parent_page_id:
        parent = source_meta.get("parent") or {}
        parent_page_id = parent.get("page_id")
        if not parent_page_id and parent.get("type") == "data_source_id":
            ds_id = parent.get("data_source_id")
            if ds_id:
                ds = notion.get_data_source(ds_id)
                ds_parent = ds.get("parent") or {}
                parent_page_id = ds_parent.get("page_id")
    if not parent_page_id:
        raise SystemExit("Could not resolve parent page_id for source database. Open the board as a page and re-share it with integration.")

    new_db = notion.create_database(
        parent_page_id=parent_page_id,
        title=args.board_name,
        properties=build_board_properties(stage_cfg.get("stage_order", [])),
    )
    new_db_id = new_db.get("id")
    if not new_db_id:
        raise SystemExit(f"Failed to create database: {new_db}")

    if not args.skip_seed:
        os.environ["NOTION_DATABASE_ID"] = new_db_id
        sync_args = SimpleNamespace(
            config=args.config,
            stage_map=args.stage_map,
            readiness=args.readiness,
            report=args.report,
            apply=not args.dry_run,
            dry_run=args.dry_run,
            max_deals=max(0, int(args.max_deals)),
            scan_notes=False,
            pipeline_name="",
            deals_status="",
            clear_before_sync=False,
        )
        run_sync(sync_args)

    payload = {
        "created_at_utc": dt.datetime.utcnow().isoformat() + "Z",
        "new_database_id": new_db_id,
        "new_database_url": notion_url_from_id(new_db_id),
        "board_name": args.board_name,
    }
    out = Path(args.output_json)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
