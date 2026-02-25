#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import re
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib import error, parse, request


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SYNC_CONFIG = ROOT / "config" / "notion_sync.json"
DEFAULT_STAGE_MAP = ROOT / "config" / "notion_stage_map.json"
DEFAULT_READINESS = ROOT / "config" / "readiness_rules.json"
DEFAULT_REPORT = ROOT / "data" / "output" / "notion_sync_report.json"
NOTION_VERSION = "2022-06-28"
URL_RE = re.compile(r"https?://[^\s<>)\"']+", re.IGNORECASE)


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def parse_date(value: str) -> Optional[dt.date]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    fmts = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in fmts:
        try:
            return dt.datetime.strptime(text, fmt).date()
        except Exception:
            continue
    try:
        return dt.date.fromisoformat(text[:10])
    except Exception:
        return None


def truthy(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "ok", "done"}


def nested_get(data: dict, path: str):
    cur = data
    for token in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(token)
    return cur


def compute_sla_color(days_in_stage: int) -> str:
    if days_in_stage <= 3:
        return "Green"
    if days_in_stage <= 7:
        return "Yellow"
    return "Red"


def compute_readiness_percent(checks: Dict[str, bool]) -> int:
    if not checks:
        return 0
    passed = sum(1 for v in checks.values() if v)
    return int(round(100 * passed / max(1, len(checks))))


def classify_docs_status(doc_links: Dict[str, str]) -> str:
    links_count = sum(1 for v in doc_links.values() if v)
    if links_count == 0:
        return "No links"
    if links_count == len(doc_links):
        return "Complete"
    return "Partial"


def map_stage(stage_name: str, stage_config: dict) -> str:
    name = (stage_name or "").strip()
    if not name:
        return stage_config.get("pre_opportunity_target", "Future pipeline")

    explicit = stage_config.get("explicit_map", {})
    if name in explicit:
        return explicit[name]

    for src, dst in explicit.items():
        if src.lower() == name.lower():
            return dst

    pre_stages = stage_config.get("pre_opportunity_stage_names", [])
    pre_set = {s.lower() for s in pre_stages}
    if name.lower() in pre_set:
        return stage_config.get("pre_opportunity_target", "Future pipeline")

    return stage_config.get("pre_opportunity_target", "Future pipeline")


def evaluate_gate(
    target_stage: str,
    checks: Dict[str, bool],
    readiness_rules: dict,
    stage_order: List[str],
) -> Tuple[str, Optional[str]]:
    gates = readiness_rules.get("gates", {})
    required_checks = gates.get(target_stage, [])
    missing = [chk for chk in required_checks if not checks.get(chk, False)]
    if not missing:
        return target_stage, None

    reason_tpl = readiness_rules.get("rollback_reason_template", "Blocked move to {target_stage}: missing {missing}")
    reason = reason_tpl.format(target_stage=target_stage, missing=", ".join(missing))

    if not readiness_rules.get("hard_rollback", True):
        return target_stage, reason

    try:
        idx = stage_order.index(target_stage)
    except ValueError:
        return target_stage, reason
    if idx == 0:
        return target_stage, reason
    return stage_order[idx - 1], reason


def dedupe_by_deal_id(deals: Iterable[dict]) -> List[dict]:
    best: Dict[int, dict] = {}
    for d in deals:
        try:
            did = int(d.get("id"))
        except Exception:
            continue
        cur = best.get(did)
        if cur is None:
            best[did] = d
            continue
        cur_updated = parse_date(cur.get("update_time") or "") or dt.date.min
        new_updated = parse_date(d.get("update_time") or "") or dt.date.min
        if new_updated >= cur_updated:
            best[did] = d
    return list(best.values())


def extract_urls(text: str) -> List[str]:
    if not text:
        return []
    return [m.group(0).rstrip(".,;") for m in URL_RE.finditer(text)]


def resolve_doc_links_from_notes(doc_hints: dict, notes: List[dict]) -> Dict[str, str]:
    links = {"brief": "", "scope": "", "estimate": "", "presentation": ""}
    all_urls: List[str] = []
    for note in notes:
        content = str(note.get("content", ""))
        all_urls.extend(extract_urls(content))
    unique_urls = []
    seen = set()
    for url in all_urls:
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        unique_urls.append(url)

    for url in unique_urls:
        low = url.lower()
        for doc_key, hints in doc_hints.items():
            if doc_key not in links or links[doc_key]:
                continue
            if any(h.lower() in low for h in hints):
                links[doc_key] = url
                break
    return links


def resolve_field_by_name(deal: dict, field_name: str, deal_field_keys: Dict[str, str]):
    key = deal_field_keys.get(field_name)
    if not key:
        return None
    return deal.get(key)


def build_doc_links(deal: dict, deal_field_keys: Dict[str, str], notes: List[dict], doc_hints: dict) -> Dict[str, str]:
    out = {"brief": "", "scope": "", "estimate": "", "presentation": ""}
    custom_fields = {
        "brief": "doc_brief_url",
        "scope": "doc_scope_url",
        "estimate": "doc_estimate_url",
        "presentation": "doc_presentation_url",
    }
    for k, fname in custom_fields.items():
        val = resolve_field_by_name(deal, fname, deal_field_keys)
        if isinstance(val, str) and val.strip().startswith("http"):
            out[k] = val.strip()

    note_links = resolve_doc_links_from_notes(doc_hints, notes)
    for k in out.keys():
        if not out[k] and note_links.get(k):
            out[k] = note_links[k]
    return out


def compute_checks(
    deal: dict,
    doc_links: Dict[str, str],
    deal_field_keys: Dict[str, str],
    readiness_rules: dict,
) -> Dict[str, bool]:
    checks = {}
    check_defs = readiness_rules.get("checks", {})
    for name, cfg in check_defs.items():
        kind = cfg.get("kind")
        ok = False
        if kind == "doc_present":
            key = cfg.get("doc_key")
            ok = bool(doc_links.get(key or "", ""))
        elif kind == "owner_assigned":
            owner_id = nested_get(deal, "owner_id.value") or nested_get(deal, "owner_id.id") or deal.get("owner_id")
            ok = owner_id not in (None, "", 0, "0")
        elif kind == "builtin_presence":
            ok = nested_get(deal, cfg.get("path", "")) not in (None, "", 0, "0")
        elif kind == "custom_field_presence":
            value = resolve_field_by_name(deal, cfg.get("field_name", ""), deal_field_keys)
            ok = value not in (None, "", 0, "0")
        elif kind == "custom_field_bool":
            value = resolve_field_by_name(deal, cfg.get("field_name", ""), deal_field_keys)
            ok = truthy(value)
        checks[name] = ok
    return checks


def notion_plain_text(value: str) -> List[dict]:
    if not value:
        return []
    return [{"type": "text", "text": {"content": str(value)}}]


def extract_notion_deal_id(page: dict, prop_name: str, prop_def: dict) -> Optional[int]:
    p = (page.get("properties") or {}).get(prop_name)
    if not p or not prop_def:
        return None
    ptype = prop_def.get("type")
    try:
        if ptype == "number":
            n = p.get("number")
            return int(n) if n is not None else None
        if ptype == "rich_text":
            vals = p.get("rich_text") or []
            if vals:
                return int(vals[0].get("plain_text", "").strip())
        if ptype == "title":
            vals = p.get("title") or []
            if vals:
                return int(vals[0].get("plain_text", "").strip())
    except Exception:
        return None
    return None


def normalize_number(value):
    if value in (None, "", "None"):
        return None
    try:
        f = float(value)
    except Exception:
        return None
    if f.is_integer():
        return int(f)
    return f


def render_notion_value(value, prop_def: dict):
    ptype = (prop_def or {}).get("type")
    if ptype in {"title", "rich_text"}:
        key = "title" if ptype == "title" else "rich_text"
        return {key: notion_plain_text(str(value or ""))}
    if ptype == "number":
        return {"number": normalize_number(value)}
    if ptype == "select":
        return {"select": {"name": str(value)}} if value else {"select": None}
    if ptype == "status":
        return {"status": {"name": str(value)}} if value else {"status": None}
    if ptype == "multi_select":
        vals = value if isinstance(value, list) else []
        return {"multi_select": [{"name": str(v)} for v in vals if str(v).strip()]}
    if ptype == "url":
        return {"url": str(value)} if value else {"url": None}
    if ptype == "checkbox":
        return {"checkbox": bool(value)}
    if ptype == "date":
        if not value:
            return {"date": None}
        if isinstance(value, dt.datetime):
            return {"date": {"start": value.isoformat()}}
        if isinstance(value, dt.date):
            return {"date": {"start": value.isoformat()}}
        return {"date": {"start": str(value)}}
    return None


class PipedriveClient:
    def __init__(self, domain: str, token: str, timeout_sec: int = 60):
        self.base = f"https://{domain}.pipedrive.com/api/v1"
        self.token = token
        self.timeout_sec = timeout_sec

    def _build_url(self, path: str, params: Optional[dict] = None) -> str:
        q = dict(params or {})
        q["api_token"] = self.token
        return f"{self.base}{path}?{parse.urlencode(q, doseq=True)}"

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        url = self._build_url(path, params)
        with request.urlopen(url, timeout=self.timeout_sec) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not payload.get("success"):
            raise RuntimeError(f"Pipedrive GET failed for {path}: {payload}")
        return payload

    def iter_paginated(self, path: str, params: Optional[dict] = None, limit: int = 500) -> Iterable[dict]:
        start = 0
        while True:
            page = dict(params or {})
            page["start"] = start
            page["limit"] = limit
            payload = self.get(path, page)
            rows = payload.get("data") or []
            for row in rows:
                yield row
            pagination = (payload.get("additional_data") or {}).get("pagination") or {}
            if not pagination.get("more_items_in_collection"):
                break
            start = pagination.get("next_start")
            if start is None:
                break

    def collect_deals(self, max_items: int = 0, deal_status: str = "all_not_deleted") -> List[dict]:
        out = []
        for row in self.iter_paginated("/deals", params={"status": deal_status}, limit=500):
            out.append(row)
            if max_items > 0 and len(out) >= max_items:
                break
        return out

    def stage_id_name_map(self) -> Dict[int, str]:
        out = {}
        for row in self.iter_paginated("/stages", params={}, limit=500):
            try:
                out[int(row["id"])] = str(row.get("name", ""))
            except Exception:
                continue
        return out

    def pipeline_id_name_map(self) -> Dict[int, str]:
        out = {}
        for row in self.iter_paginated("/pipelines", params={}, limit=200):
            try:
                out[int(row["id"])] = str(row.get("name", ""))
            except Exception:
                continue
        return out

    def deal_field_name_key_map(self) -> Dict[str, str]:
        out = {}
        for row in self.iter_paginated("/dealFields", params={}, limit=500):
            name = str(row.get("name", "")).strip()
            key = str(row.get("key", "")).strip()
            if name and key:
                out[name] = key
        return out

    def notes_by_deal(self, deal_id: int, limit: int = 20) -> List[dict]:
        payload = self.get("/notes", params={"deal_id": deal_id, "start": 0, "limit": max(1, limit)})
        return payload.get("data") or []


class NotionClient:
    def __init__(self, token: str, timeout_sec: int = 60, max_retries: int = 4, backoff_sec: float = 1.5):
        self.token = token
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.backoff_sec = backoff_sec

    def _request(self, method: str, path: str, body: Optional[dict] = None) -> dict:
        url = f"https://api.notion.com{path}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }
        req = request.Request(url, data=data, method=method, headers=headers)

        for attempt in range(self.max_retries + 1):
            try:
                with request.urlopen(req, timeout=self.timeout_sec) as resp:
                    raw = resp.read().decode("utf-8")
                    return json.loads(raw) if raw else {}
            except error.HTTPError as e:
                status = e.code
                if status in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                    retry_after = e.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after else self.backoff_sec * (2 ** attempt)
                    time.sleep(wait)
                    continue
                msg = e.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"Notion API {method} {path} failed ({status}): {msg}")
            except Exception:
                if attempt < self.max_retries:
                    time.sleep(self.backoff_sec * (2 ** attempt))
                    continue
                raise
        raise RuntimeError(f"Notion API {method} {path} exceeded retries")

    def get_database(self, database_id: str) -> dict:
        return self._request("GET", f"/v1/databases/{database_id}")

    def get_data_source(self, data_source_id: str) -> dict:
        return self._request("GET", f"/v1/data-sources/{data_source_id}")

    def query_database(self, database_id: str, start_cursor: Optional[str] = None, page_size: int = 100) -> dict:
        body = {"page_size": page_size}
        if start_cursor:
            body["start_cursor"] = start_cursor
        return self._request("POST", f"/v1/databases/{database_id}/query", body)

    def list_pages(self, database_id: str) -> List[dict]:
        pages = []
        cursor = None
        while True:
            payload = self.query_database(database_id, start_cursor=cursor, page_size=100)
            pages.extend(payload.get("results") or [])
            if not payload.get("has_more"):
                break
            cursor = payload.get("next_cursor")
            if not cursor:
                break
        return pages

    def update_page(self, page_id: str, properties: dict) -> dict:
        return self._request("PATCH", f"/v1/pages/{page_id}", {"properties": properties})

    def archive_page(self, page_id: str) -> dict:
        return self._request("PATCH", f"/v1/pages/{page_id}", {"archived": True})

    def create_page(self, database_id: str, properties: dict) -> dict:
        return self._request("POST", "/v1/pages", {"parent": {"database_id": database_id}, "properties": properties})

    def create_database(self, parent_page_id: str, title: str, properties: dict) -> dict:
        body = {
            "parent": {"type": "page_id", "page_id": parent_page_id},
            "title": notion_plain_text(title),
            "properties": properties,
        }
        return self._request("POST", "/v1/databases", body)

    def create_workspace_page(self, title: str) -> dict:
        body = {
            "parent": {"workspace": True},
            "properties": {"title": {"title": notion_plain_text(title)}},
        }
        return self._request("POST", "/v1/pages", body)


def build_properties_payload(
    values: Dict[str, object],
    schema_properties: Dict[str, dict],
    mapping: Dict[str, str],
    manual_fields: List[str],
) -> Tuple[dict, List[str]]:
    payload = {}
    skipped = []
    manual_set = set(manual_fields or [])
    for logical_key, notion_name in mapping.items():
        if notion_name in manual_set:
            continue
        if notion_name not in schema_properties:
            skipped.append(notion_name)
            continue
        if logical_key not in values:
            continue
        prop_def = schema_properties[notion_name]
        rendered = render_notion_value(values.get(logical_key), prop_def)
        if rendered is None:
            skipped.append(notion_name)
            continue
        payload[notion_name] = rendered
    return payload, skipped


def plan_upsert_actions(rows: List[dict], existing_by_id: Dict[int, dict]) -> Dict[str, int]:
    creates = 0
    updates = 0
    for r in dedupe_by_deal_id(rows):
        did = int(r["id"])
        if did in existing_by_id:
            updates += 1
        else:
            creates += 1
    return {"creates": creates, "updates": updates}


def run_sync(args):
    sync_cfg = load_json(Path(args.config))
    stage_cfg = load_json(Path(args.stage_map))
    readiness = load_json(Path(args.readiness))

    pd_domain = os.getenv("PIPEDRIVE_DOMAIN", "").strip()
    pd_token = os.getenv("PIPEDRIVE_API_TOKEN", "").strip()
    notion_token = os.getenv("NOTION_API_TOKEN", "").strip()
    notion_db = os.getenv("NOTION_DATABASE_ID", "").strip() or str(sync_cfg.get("notion_database_id", "")).strip()

    if not pd_domain or not pd_token or not notion_token or not notion_db:
        raise SystemExit("Missing env vars: PIPEDRIVE_DOMAIN, PIPEDRIVE_API_TOKEN, NOTION_API_TOKEN, NOTION_DATABASE_ID")

    timeout_sec = int(sync_cfg.get("request_timeout_sec", 60))
    pd = PipedriveClient(pd_domain, pd_token, timeout_sec=timeout_sec)
    notion = NotionClient(
        notion_token,
        timeout_sec=timeout_sec,
        max_retries=int(sync_cfg.get("max_retries", 4)),
        backoff_sec=float(sync_cfg.get("retry_backoff_sec", 1.5)),
    )

    stage_map = pd.stage_id_name_map()
    pipeline_map = pd.pipeline_id_name_map()
    field_keys = pd.deal_field_name_key_map()
    max_deals = args.max_deals if args.max_deals > 0 else int(sync_cfg.get("max_deals_per_run", 0))
    scan_notes = args.scan_notes or bool(sync_cfg.get("scan_notes_for_docs", False))
    notes_limit = int(sync_cfg.get("notes_limit_per_deal", 20))
    deal_status = (args.deals_status or str(sync_cfg.get("deals_status", "all_not_deleted"))).strip()
    use_raw_stage_names = bool(sync_cfg.get("use_raw_stage_names", False))
    deals = dedupe_by_deal_id(pd.collect_deals(max_items=0, deal_status=deal_status))

    pipeline_filters = []
    if args.pipeline_name:
        pipeline_filters.extend([x.strip() for x in args.pipeline_name.split(",") if x.strip()])
    pipeline_filters.extend([x.strip() for x in sync_cfg.get("pipeline_include_names", []) if str(x).strip()])
    pipeline_filters_lower = {p.lower() for p in pipeline_filters}
    if pipeline_filters_lower:
        deals = [
            d for d in deals
            if str(pipeline_map.get(int(d.get("pipeline_id") or 0), "")).strip().lower() in pipeline_filters_lower
        ]
    if max_deals > 0 and len(deals) > max_deals:
        deals = deals[:max_deals]

    db = notion.get_database(notion_db)
    schema_props = db.get("properties") or {}
    prop_map = sync_cfg.get("properties", {})
    manual_fields = sync_cfg.get("manual_fields", [])
    stage_order = stage_cfg.get("stage_order", [])
    doc_hints = sync_cfg.get("doc_hints", {})

    existing_pages = notion.list_pages(notion_db)
    deal_id_prop = prop_map.get("crm_deal_id", "CRM Deal ID")
    deal_prop_def = schema_props.get(deal_id_prop)
    existing_by_deal_id: Dict[int, dict] = {}
    for p in existing_pages:
        did = extract_notion_deal_id(p, deal_id_prop, deal_prop_def)
        if did is not None:
            existing_by_deal_id[did] = p

    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    today = dt.date.today()
    report = {
        "mode": "apply" if args.apply else "dry-run",
        "timestamp_utc": now,
        "pipeline_filter": sorted(pipeline_filters_lower),
        "deals_status": deal_status,
        "total_deals_seen": len(deals),
        "actions_planned": plan_upsert_actions(deals, existing_by_deal_id),
        "created": 0,
        "updated": 0,
        "archived": 0,
        "blocked": 0,
        "errors": [],
        "skipped_properties": [],
        "blocked_examples": [],
    }
    skipped_props = set()

    if args.clear_before_sync and args.apply:
        for p in existing_pages:
            try:
                notion.archive_page(p["id"])
                report["archived"] += 1
            except Exception as e:
                report["errors"].append({"page_id": p.get("id"), "error": f"archive_failed: {e}"})
        existing_by_deal_id = {}

    for deal in deals:
        try:
            did = int(deal.get("id"))
            title = str(deal.get("title") or f"Deal {did}")
            stage_id = int(deal.get("stage_id") or 0)
            raw_stage = stage_map.get(stage_id, "")
            pipeline_id = int(deal.get("pipeline_id") or 0)
            pipeline_name = pipeline_map.get(pipeline_id, "")
            if use_raw_stage_names and raw_stage:
                target_stage = raw_stage
            else:
                target_stage = map_stage(raw_stage, stage_cfg)
            notes = pd.notes_by_deal(did, limit=notes_limit) if scan_notes else []
            doc_links = build_doc_links(deal, field_keys, notes, doc_hints)
            checks = compute_checks(deal, doc_links, field_keys, readiness)
            readiness_percent = compute_readiness_percent(checks)
            docs_status = classify_docs_status(doc_links)
            stage_enter_date = (
                parse_date(str(deal.get("stage_change_time") or ""))
                or parse_date(str(deal.get("update_time") or ""))
                or parse_date(str(deal.get("add_time") or ""))
                or today
            )
            days_in_stage = max(0, (today - stage_enter_date).days)
            sla_color = compute_sla_color(days_in_stage)
            final_stage, block_reason = evaluate_gate(target_stage, checks, readiness, stage_order)
            gate_status = "Blocked" if block_reason else "Pass"
            sync_note = block_reason or ""
            company_name = (
                str(nested_get(deal, "org_id.name") or deal.get("org_name") or "").strip()
            )
            contact_name = (
                str(nested_get(deal, "person_id.name") or deal.get("person_name") or "").strip()
            )
            owner_name = (
                str(nested_get(deal, "owner_id.name") or deal.get("user_name") or "").strip()
            )
            expected_close = parse_date(str(deal.get("expected_close_date") or "")) or None
            deal_value = deal.get("value")
            currency = str(deal.get("currency") or "").strip().upper()
            pipedrive_url = f"https://{pd_domain}.pipedrive.com/deal/{did}"

            values = {
                "title": title,
                "crm_deal_id": did,
                "stage": final_stage,
                "pipeline": pipeline_name,
                "company": company_name,
                "contact": contact_name,
                "owner": owner_name,
                "deal_value": deal_value,
                "currency": currency,
                "expected_close_date": expected_close,
                "pipedrive_url": pipedrive_url,
                "days_in_stage": days_in_stage,
                "sla_color": sla_color,
                "readiness_percent": readiness_percent,
                "gate_status": gate_status,
                "sync_notes": sync_note,
                "docs_status": docs_status,
                "brief_link": doc_links.get("brief", ""),
                "scope_link": doc_links.get("scope", ""),
                "estimate_link": doc_links.get("estimate", ""),
                "presentation_link": doc_links.get("presentation", ""),
                "last_sync_at": now,
            }
            payload, skipped = build_properties_payload(values, schema_props, prop_map, manual_fields)
            for s in skipped:
                skipped_props.add(s)

            existing = existing_by_deal_id.get(did)
            if block_reason:
                report["blocked"] += 1
                if len(report["blocked_examples"]) < 30:
                    report["blocked_examples"].append({"deal_id": did, "title": title, "reason": block_reason})

            if args.apply:
                if existing:
                    notion.update_page(existing["id"], payload)
                    report["updated"] += 1
                else:
                    notion.create_page(notion_db, payload)
                    report["created"] += 1
            else:
                if existing:
                    report["updated"] += 1
                else:
                    report["created"] += 1
        except Exception as e:
            report["errors"].append({"deal_id": deal.get("id"), "error": str(e)})

    report["skipped_properties"] = sorted(skipped_props)
    out = Path(args.report)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"Report written: {out}")
    print(json.dumps({k: report[k] for k in ['mode', 'created', 'updated', 'blocked']}, ensure_ascii=False))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(DEFAULT_SYNC_CONFIG))
    ap.add_argument("--stage-map", default=str(DEFAULT_STAGE_MAP))
    ap.add_argument("--readiness", default=str(DEFAULT_READINESS))
    ap.add_argument("--report", default=str(DEFAULT_REPORT))
    ap.add_argument("--max-deals", type=int, default=0, help="Limit number of deals per run (0 = from config/all)")
    ap.add_argument("--scan-notes", action="store_true", help="Enable scanning Pipedrive notes for document links")
    ap.add_argument("--pipeline-name", default="", help="Comma-separated Pipedrive pipeline names to include")
    ap.add_argument("--deals-status", default="", help="Pipedrive deals status filter: open, won, lost, deleted, all_not_deleted")
    ap.add_argument("--clear-before-sync", action="store_true", help="Archive existing pages in target Notion DB before sync")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if not args.apply:
        args.dry_run = True
    run_sync(args)


if __name__ == "__main__":
    main()
