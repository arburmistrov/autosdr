#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import json
import os
import re
import smtplib
import ssl
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib import parse, request


DATE_FMT = "%Y-%m-%d"


@dataclass
class Contact:
    person_id: int
    name: str
    email: str
    org_name: str
    owner_name: str
    last_contact_date: Optional[dt.date]
    stale_days: int
    stage: int
    next_touch_date: dt.date
    status: str


class PipedriveClient:
    def __init__(self, domain: str, token: str):
        self.base = f"https://{domain}.pipedrive.com/api/v1"
        self.token = token

    def _build_url(self, path: str, params: Optional[dict] = None) -> str:
        params = dict(params or {})
        params["api_token"] = self.token
        q = parse.urlencode(params, doseq=True)
        return f"{self.base}{path}?{q}"

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        url = self._build_url(path, params)
        with request.urlopen(url, timeout=60) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not payload.get("success"):
            raise RuntimeError(f"GET {path} failed: {payload}")
        return payload

    def post(self, path: str, body: dict) -> dict:
        url = self._build_url(path)
        req = request.Request(url, data=json.dumps(body).encode("utf-8"), method="POST")
        req.add_header("Content-Type", "application/json")
        with request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not payload.get("success"):
            raise RuntimeError(f"POST {path} failed: {payload}")
        return payload

    def put(self, path: str, body: dict) -> dict:
        url = self._build_url(path)
        req = request.Request(url, data=json.dumps(body).encode("utf-8"), method="PUT")
        req.add_header("Content-Type", "application/json")
        with request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not payload.get("success"):
            raise RuntimeError(f"PUT {path} failed: {payload}")
        return payload

    def delete(self, path: str) -> dict:
        url = self._build_url(path)
        req = request.Request(url, method="DELETE")
        with request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not payload.get("success"):
            raise RuntimeError(f"DELETE {path} failed: {payload}")
        return payload

    def iter_paginated(self, path: str, params: Optional[dict] = None, limit: int = 500) -> Iterable[dict]:
        params = dict(params or {})
        start = 0
        while True:
            page = dict(params)
            page["start"] = start
            page["limit"] = limit
            payload = self.get(path, page)
            data = payload.get("data") or []
            for row in data:
                yield row
            p = (payload.get("additional_data") or {}).get("pagination") or {}
            if not p.get("more_items_in_collection"):
                break
            start = p.get("next_start")
            if start is None:
                break


def parse_date(value: str) -> Optional[dt.date]:
    if not value:
        return None
    v = value.strip()
    if not v:
        return None
    for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"]:
        try:
            if "%H" in fmt:
                return dt.datetime.strptime(v, fmt).date()
            return dt.datetime.strptime(v, fmt).date()
        except Exception:
            continue
    try:
        return dt.date.fromisoformat(v[:10])
    except Exception:
        return None


def first_email(person: dict) -> str:
    emails = person.get("email") or []
    if isinstance(emails, list):
        for e in emails:
            if isinstance(e, dict):
                val = (e.get("value") or "").strip()
                if val:
                    return val
            elif isinstance(e, str) and e.strip():
                return e.strip()
    return ""


def collect_people(client: PipedriveClient) -> List[dict]:
    return list(client.iter_paginated("/persons"))


def get_me_user_id(client: PipedriveClient) -> int:
    payload = client.get("/users/me")
    data = payload.get("data") or {}
    uid = data.get("id")
    if not uid:
        raise RuntimeError("Could not resolve current Pipedrive user id from /users/me")
    return int(uid)


def collect_last_contact_dates(client: PipedriveClient) -> Dict[int, dt.date]:
    by_person: Dict[int, dt.date] = {}
    for act in client.iter_paginated("/activities", params={"done": 1}, limit=500):
        pid = act.get("person_id")
        if not pid:
            continue
        candidates = [
            parse_date(act.get("mark_done_time") or ""),
            parse_date(act.get("due_date") or ""),
            parse_date(act.get("update_time") or ""),
            parse_date(act.get("add_time") or ""),
        ]
        d = next((x for x in candidates if x), None)
        if not d:
            continue
        cur = by_person.get(int(pid))
        if not cur or d > cur:
            by_person[int(pid)] = d
    return by_person


def load_existing_queue(path: Path) -> Dict[int, dict]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    out = {}
    for r in rows:
        try:
            out[int(r.get("person_id", "0"))] = r
        except Exception:
            continue
    return out


def render_email(stage: int, name: str, org: str) -> Dict[str, str]:
    first = (name or "there").split(" ")[0]
    if stage == 1:
        return {
            "subject": f"Reconnect S-PRO | {org or 'your team'}",
            "body": (
                f"Hi {first},\n\n"
                "We were in touch before, and I wanted to reconnect.\n\n"
                "A lot of interesting AI technologies appeared recently, and we help teams apply AI to improve digital competitiveness.\n\n"
                "That is why we started running APEX workshops focused on quick prototyping of real use cases.\n\n"
                f"If useful for {org or 'your team'}, I can share how we usually structure this.\n\n"
                "Best regards,\n"
            ),
        }
    if stage == 2:
        return {
            "subject": f"Reconnect S-PRO | Follow-up for {org or 'your team'}",
            "body": (
                f"Hi {first},\n\n"
                "Quick follow-up on my previous note.\n\n"
                "If AI is on your agenda this year, our APEX format can help your team prototype priority use cases and decide what is worth scaling.\n\n"
                "Open to a short 20-minute call next week?\n\n"
                "Best regards,\n"
            ),
        }
    return {
        "subject": f"Reconnect S-PRO | Should I close this loop?",
        "body": (
            f"Hi {first},\n\n"
            "Final follow-up from my side.\n\n"
            "If discussing AI use-case prototyping is relevant now, I can send 2-3 options for a short call.\n"
            "If not relevant now, I will close the loop here.\n\n"
            "Best regards,\n"
        ),
    }


def smtp_send(host: str, port: int, username: str, password: str, sender: str, to: str, subject: str, body: str):
    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body)

    context = ssl.create_default_context()
    with smtplib.SMTP(host, port, timeout=60) as server:
        server.starttls(context=context)
        server.login(username, password)
        server.send_message(msg)


def create_note(client: PipedriveClient, person_id: int, deal_id: Optional[int], content: str):
    payload = {
        "person_id": person_id,
        "content": content,
    }
    if deal_id:
        payload["deal_id"] = deal_id
    client.post("/notes", payload)


def most_recent_open_deal_id(client: PipedriveClient, person_id: int) -> Optional[int]:
    try:
        payload = client.get(f"/persons/{person_id}/deals")
    except Exception:
        return None
    deals = payload.get("data") or []
    open_deals = [d for d in deals if str(d.get("status", "")).lower() == "open"]
    if not open_deals:
        return None
    open_deals.sort(key=lambda d: parse_date(d.get("update_time") or "") or dt.date.min, reverse=True)
    return int(open_deals[0]["id"])


def load_csv(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def to_int(value, default=0):
    try:
        return int(float(str(value).strip()))
    except Exception:
        return default


EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
FREE_MAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "aol.com", "mail.ru", "yandex.ru", "proton.me", "protonmail.com", "gmx.com",
}
DISPOSABLE_HINTS = {"tempmail", "mailinator", "guerrillamail", "10minutemail", "trashmail"}


def is_email_valid(email: str) -> bool:
    return bool(EMAIL_RE.match((email or "").strip()))


def email_domain(email: str) -> str:
    e = (email or "").strip().lower()
    if "@" not in e:
        return ""
    return e.split("@", 1)[1]


def looks_human_name(name: str) -> bool:
    n = (name or "").strip()
    if len(n) < 3:
        return False
    letters = sum(ch.isalpha() for ch in n)
    digits = sum(ch.isdigit() for ch in n)
    if letters < 2:
        return False
    if digits > letters:
        return False
    if n.startswith("$"):
        return False
    return True


def looks_org_valid(org: str) -> bool:
    o = (org or "").strip()
    if not o:
        return False
    if len(o) < 2:
        return False
    letters = sum(ch.isalpha() for ch in o)
    return letters >= 2


def score_row_quality(r: dict) -> tuple:
    email = (r.get("email") or "").strip()
    name = (r.get("name") or "").strip()
    org = (r.get("org_name") or "").strip()
    last_contact = (r.get("last_contact_date") or "").strip()

    score = 0
    reasons = []

    v_email = is_email_valid(email)
    if not v_email:
        reasons.append("invalid_email")
        return 0, False, "invalid_email"
    score += 30

    dom = email_domain(email)
    if any(h in dom for h in DISPOSABLE_HINTS):
        score -= 30
        reasons.append("disposable_domain")
    elif dom in FREE_MAIL_DOMAINS:
        score += 5
        reasons.append("free_mail_domain")
    else:
        score += 20

    if looks_human_name(name):
        score += 20
    else:
        score -= 20
        reasons.append("low_quality_name")

    if looks_org_valid(org):
        score += 15
    else:
        score -= 10
        reasons.append("missing_or_bad_org")

    if last_contact:
        score += 5
    else:
        reasons.append("no_contact_history")

    score = max(0, min(100, score))
    keep = score >= 55
    if not keep and not reasons:
        reasons.append("low_score")
    return score, keep, ",".join(reasons)


def cmd_rank_queue(args):
    q = Path(args.queue)
    rows = load_csv(q)
    if not rows:
        print("Queue is empty; nothing to rank.")
        return

    enriched = []
    for r in rows:
        score, keep, reason = score_row_quality(r)
        nr = dict(r)
        nr["relevance_score"] = str(score)
        nr["keep_for_send"] = "true" if keep else "false"
        nr["exclusion_reason"] = "" if keep else reason
        nr["priority_bucket"] = "rest"
        enriched.append(nr)

    eligible = [r for r in enriched if r["keep_for_send"] == "true"]
    eligible.sort(key=lambda x: (to_int(x.get("relevance_score")), to_int(x.get("stale_days"))), reverse=True)
    top_n = max(1, int(len(eligible) * (args.top_percent / 100.0))) if eligible else 0
    top_ids = set(id(r) for r in eligible[:top_n])
    for r in enriched:
        if r["keep_for_send"] == "true" and id(r) in top_ids:
            r["priority_bucket"] = f"top{args.top_percent}"

    enriched.sort(
        key=lambda x: (
            x.get("priority_bucket") != f"top{args.top_percent}",
            x.get("keep_for_send") != "true",
            -to_int(x.get("relevance_score")),
            -to_int(x.get("stale_days")),
        )
    )

    fields = list(enriched[0].keys())
    write_csv(Path(args.output), enriched, fields)

    dropped = sum(1 for r in enriched if r["keep_for_send"] != "true")
    kept = len(enriched) - dropped
    top = sum(1 for r in enriched if r["priority_bucket"] == f"top{args.top_percent}")
    print(
        f"Ranked queue: total={len(enriched)}, kept={kept}, dropped={dropped}, "
        f"{f'top{args.top_percent}'}={top} -> {args.output}"
    )


def cmd_export_top(args):
    rows = load_csv(Path(args.queue))
    if not rows:
        print("Queue is empty; nothing to export.")
        return
    target_bucket = f"top{args.top_percent}"
    filtered = [
        r for r in rows
        if (r.get("keep_for_send") or "true").lower() == "true"
        and (r.get("priority_bucket") or "") == target_bucket
    ]
    filtered.sort(key=lambda r: (to_int(r.get("relevance_score"), 0), to_int(r.get("stale_days"), 0)), reverse=True)
    top = filtered[: args.limit]
    if not top:
        print("No eligible rows in target bucket.")
        return
    fields = list(top[0].keys())
    write_csv(Path(args.output), top, fields)
    print(f"Exported top {len(top)} contacts -> {args.output}")


def cmd_mark_top_in_pipedrive(args, client: PipedriveClient):
    rows = load_csv(Path(args.input))
    if not rows:
        print("Input list is empty; nothing to mark.")
        return
    marked = 0
    for r in rows[: args.limit]:
        pid = to_int(r.get("person_id"), 0)
        if not pid:
            continue
        content = (
            f"Top priority contact ({args.tag}) selected by reactivation ranking.\n"
            f"Score: {r.get('relevance_score', '')}\n"
            f"Org: {r.get('org_name', '')}\n"
            f"Email: {r.get('email', '')}\n"
            f"Date: {dt.date.today().isoformat()}"
        )
        create_note(client, pid, None, content)
        marked += 1
    print(f"Marked {marked} contacts in Pipedrive with tag '{args.tag}'.")


def cmd_prune_dead_emails(args, client: PipedriveClient):
    rows = load_csv(Path(args.input))
    to_prune = []
    for r in rows:
        st = (r.get("verification_status") or "").strip().lower()
        reason = (r.get("verification_reason") or "").strip().lower()
        exclusion = (r.get("exclusion_reason") or "").strip().lower()
        if (
            st == "invalid"
            or reason in {"no_mx", "invalid_syntax", "rcpt_rejected"}
            or "email_rcpt_rejected" in exclusion
        ):
            pid = to_int(r.get("person_id"), 0)
            email = (r.get("email") or "").strip().lower()
            if pid and email:
                to_prune.append((pid, email, reason or st or exclusion))

    removed = 0
    for pid, email, reason in to_prune:
        try:
            person = client.get(f"/persons/{pid}").get("data") or {}
            emails = person.get("email") or []
            keep = []
            changed = False
            for e in emails:
                if isinstance(e, dict):
                    val = (e.get("value") or "").strip().lower()
                    if val == email:
                        changed = True
                        continue
                    keep.append(e)
                else:
                    val = str(e).strip().lower()
                    if val == email:
                        changed = True
                        continue
                    keep.append({"value": str(e), "primary": False, "label": "work"})
            if changed:
                client.put(f"/persons/{pid}", {"email": keep})
                create_note(
                    client,
                    pid,
                    None,
                    f"Dead email removed automatically: {email} (reason: {reason})",
                )
                removed += 1
        except Exception as e:
            # continue cleanup even if one contact fails
            print(f"warn: failed to prune person_id={pid} email={email}: {type(e).__name__}")
            continue

    print(f"Pruned dead emails in Pipedrive: {removed}")


def cmd_report(args):
    q = load_csv(Path(args.queue))
    today = dt.date.today().isoformat()
    total = len(q)
    sent = [r for r in q if (r.get("last_email_sent_at") or "").strip()]
    due = [r for r in q if (r.get("next_touch_date") or "") <= today and (r.get("next_touch_date") or "").strip()]
    keep = [r for r in q if (r.get("keep_for_send") or "true").lower() == "true"]
    top = [r for r in q if (r.get("priority_bucket") or "") == f"top{args.top_percent}"]
    invalid = [
        r for r in q
        if "email_rcpt_rejected" in (r.get("exclusion_reason") or "")
        or (r.get("verification_status") or "").lower() == "invalid"
    ]

    by_stage = {}
    by_status = {}
    for r in q:
        by_stage[r.get("stage", "")] = by_stage.get(r.get("stage", ""), 0) + 1
        by_status[r.get("status", "")] = by_status.get(r.get("status", ""), 0) + 1

    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "date": today,
        "total_contacts": total,
        "keep_for_send": len(keep),
        "top_bucket_contacts": len(top),
        "sent_marked": len(sent),
        "due_today_or_past": len(due),
        "invalid_or_rejected": len(invalid),
        "by_stage": by_stage,
        "by_status": by_status,
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

    html = f"""<!doctype html>
<html><head><meta charset='utf-8'><title>Pipedrive Reactivation Dashboard</title>
<style>
body{{font-family:Arial,sans-serif;margin:24px;background:#f7f9fc;color:#1f2a37}}
.kpi{{display:inline-block;background:#fff;border:1px solid #dbe3ef;border-radius:10px;padding:12px 14px;margin:8px;min-width:180px}}
h1{{margin:0 0 8px 0}} h2{{margin-top:24px}} table{{border-collapse:collapse;background:#fff}}
td,th{{border:1px solid #dbe3ef;padding:6px 10px}}
</style></head><body>
<h1>Pipedrive Reactivation Dashboard</h1>
<div>Snapshot: {today}</div>
<div class='kpi'><b>Total contacts</b><br>{total}</div>
<div class='kpi'><b>Keep for send</b><br>{len(keep)}</div>
<div class='kpi'><b>Top bucket</b><br>{len(top)}</div>
<div class='kpi'><b>Sent (marked)</b><br>{len(sent)}</div>
<div class='kpi'><b>Due now</b><br>{len(due)}</div>
<div class='kpi'><b>Invalid/rejected</b><br>{len(invalid)}</div>
<h2>By Stage</h2>
<table><tr><th>Stage</th><th>Count</th></tr>
{''.join(f'<tr><td>{k}</td><td>{v}</td></tr>' for k,v in sorted(by_stage.items()))}
</table>
<h2>By Status</h2>
<table><tr><th>Status</th><th>Count</th></tr>
{''.join(f'<tr><td>{k}</td><td>{v}</td></tr>' for k,v in sorted(by_status.items()))}
</table>
</body></html>"""
    out_html = Path(args.output_html)
    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text(html, encoding="utf-8")
    print(f"Report generated: {out_json} and {out_html}")


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def cmd_build_queue(args, client: PipedriveClient):
    today = dt.date.today()
    stale_cutoff = today - dt.timedelta(days=args.stale_days)

    people = collect_people(client)
    last_contact = collect_last_contact_dates(client)
    existing = load_existing_queue(Path(args.output))

    rows = []
    for p in people:
        pid = int(p.get("id") or 0)
        if not pid:
            continue
        email = first_email(p)
        if not email:
            continue

        d = last_contact.get(pid)
        stale_days = (today - d).days if d else 9999
        if d and d > stale_cutoff:
            continue

        ex = existing.get(pid, {})
        stage = int(ex.get("stage") or 1)
        next_touch = parse_date(ex.get("next_touch_date") or "") or today
        status = ex.get("status") or "queued"

        org = (p.get("org_name") or {}).get("name") if isinstance(p.get("org_name"), dict) else (p.get("org_name") or "")
        owner_name = (p.get("owner_name") or {}).get("name") if isinstance(p.get("owner_name"), dict) else (p.get("owner_name") or "")

        rows.append(
            {
                "person_id": pid,
                "name": p.get("name") or "",
                "email": email,
                "org_name": org or "",
                "owner_name": owner_name or "",
                "last_contact_date": d.isoformat() if d else "",
                "stale_days": stale_days,
                "stage": stage,
                "next_touch_date": next_touch.isoformat(),
                "status": status,
                "last_email_subject": ex.get("last_email_subject") or "",
                "last_email_sent_at": ex.get("last_email_sent_at") or "",
            }
        )

    rows.sort(key=lambda r: (-int(r["stale_days"]), r["name"]))
    fields = [
        "person_id", "name", "email", "org_name", "owner_name", "last_contact_date", "stale_days",
        "stage", "next_touch_date", "status", "last_email_subject", "last_email_sent_at"
    ]
    write_csv(Path(args.output), rows, fields)
    print(f"Queue built: {len(rows)} stale contacts -> {args.output}")


def cmd_send_daily(args, client: PipedriveClient):
    queue_path = Path(args.queue)
    rows_all = load_csv(queue_path)

    candidates = []
    for idx, row in enumerate(rows_all):
        if args.clean_only and (row.get("keep_for_send") or "true").lower() != "true":
            continue
        if args.top_bucket_only:
            target_bucket = f"top{args.top_percent}"
            if (row.get("priority_bucket") or "") != target_bucket:
                continue
        candidates.append((idx, row))

    if args.top_bucket_only:
        target_bucket = f"top{args.top_percent}"
        candidates = [(i, r) for i, r in candidates if (r.get("priority_bucket") or "") == target_bucket]
    candidates.sort(key=lambda ir: (to_int(ir[1].get("relevance_score"), 0), to_int(ir[1].get("stale_days"), 0)), reverse=True)
    today = dt.date.today()

    smtp_host = os.getenv("SMTP_HOST", "")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USERNAME", "")
    smtp_pass = os.getenv("SMTP_PASSWORD", "")
    sender = os.getenv("SMTP_SENDER", smtp_user)

    if args.send and (not smtp_host or not smtp_user or not smtp_pass or not sender):
        raise SystemExit("Set SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, SMTP_SENDER for --send mode")

    sent = 0
    for idx, r in candidates:
        if sent >= args.daily_limit:
            break
        if r.get("status") in {"do_not_contact", "replied", "booked", "closed"}:
            continue
        due = parse_date(r.get("next_touch_date") or "")
        if not due or due > today:
            continue

        stage = int(r.get("stage") or 1)
        payload = render_email(stage, r.get("name", ""), r.get("org_name", ""))
        subject = payload["subject"]
        body = payload["body"] + (args.signature or "")

        if args.send:
            smtp_send(
                host=smtp_host,
                port=smtp_port,
                username=smtp_user,
                password=smtp_pass,
                sender=sender,
                to=r["email"],
                subject=subject,
                body=body,
            )

        person_id = int(r["person_id"])
        deal_id = most_recent_open_deal_id(client, person_id)
        note = (
            f"Reactivation email stage {stage} {'SENT' if args.send else 'DRY-RUN'}\\n"
            f"Subject: {subject}\\n"
            f"To: {r['email']}\\n"
            f"Date: {today.isoformat()}"
        )
        create_note(client, person_id, deal_id, note)

        r["last_email_subject"] = subject
        r["last_email_sent_at"] = dt.datetime.now().isoformat(timespec="seconds")
        if stage >= 3:
            r["status"] = "awaiting_reply"
            r["next_touch_date"] = ""
        else:
            r["stage"] = str(stage + 1)
            r["next_touch_date"] = (today + dt.timedelta(days=args.stage_gap_days)).isoformat()
            r["status"] = "queued"

        sent += 1

    fields = list(rows_all[0].keys()) if rows_all else [
        "person_id", "name", "email", "org_name", "owner_name", "last_contact_date", "stale_days",
        "stage", "next_touch_date", "status", "last_email_subject", "last_email_sent_at"
    ]
    write_csv(queue_path, rows_all, fields)
    print(f"Daily run complete. Processed: {sent}. Send mode: {'ON' if args.send else 'DRY-RUN'}")


def cmd_booked_call(args, client: PipedriveClient):
    person_id = int(args.person_id)
    title = args.deal_title or f"Booked call - Reactivation - {args.person_name or 'Contact'}"
    owner_id = int(args.owner_user_id) if args.owner_user_id else get_me_user_id(client)

    deal_body = {
        "title": title,
        "person_id": person_id,
        "value": args.deal_value,
        "currency": args.currency,
        "status": "open",
        "user_id": owner_id,
    }
    deal = client.post("/deals", deal_body).get("data") or {}
    deal_id = int(deal.get("id"))

    call_date = args.call_date or dt.date.today().isoformat()
    activity_body = {
        "subject": "Reactivation discovery call",
        "type": "call",
        "done": 0,
        "person_id": person_id,
        "deal_id": deal_id,
        "user_id": owner_id,
        "due_date": call_date,
        "duration": "00:30",
        "note": args.call_note or "Call booked from reactivation sequence",
    }
    client.post("/activities", activity_body)
    create_note(client, person_id, deal_id, f"Booked call and created deal #{deal_id} on {call_date}")

    if args.queue:
        qp = Path(args.queue)
        rows = load_csv(qp)
        for r in rows:
            if str(r.get("person_id")) == str(person_id):
                r["status"] = "booked"
                r["next_touch_date"] = ""
                r["last_email_subject"] = r.get("last_email_subject", "")
        fields = list(rows[0].keys()) if rows else []
        if fields:
            write_csv(qp, rows, fields)

    print(f"Created deal {deal_id} and call activity for person {person_id}")


def build_parser():
    ap = argparse.ArgumentParser(description="Pipedrive stale lead reactivation automation")
    ap.add_argument("--domain", default=os.getenv("PIPEDRIVE_DOMAIN", ""))
    ap.add_argument("--token", default=os.getenv("PIPEDRIVE_API_TOKEN", ""))

    sp = ap.add_subparsers(dest="command", required=True)

    p_build = sp.add_parser("build-queue", help="Build stale lead queue")
    p_build.add_argument("--output", default="data/output/pipedrive_reactivation_queue.csv")
    p_build.add_argument("--stale-days", type=int, default=240)

    p_send = sp.add_parser("send-daily", help="Run daily touch for due leads")
    p_send.add_argument("--queue", default="data/output/pipedrive_reactivation_queue.csv")
    p_send.add_argument("--daily-limit", type=int, default=20)
    p_send.add_argument("--stage-gap-days", type=int, default=4)
    p_send.add_argument("--signature", default="")
    p_send.add_argument("--send", action="store_true", help="Actually send via SMTP. Without this flag -> dry-run")
    p_send.add_argument("--clean-only", action="store_true", help="Send only rows with keep_for_send=true")
    p_send.add_argument("--top-bucket-only", action="store_true", help="Send only rows in top percentile bucket")
    p_send.add_argument("--top-percent", type=int, default=20, help="Percent for priority bucket name, e.g. top20")

    p_rank = sp.add_parser("rank-queue", help="Clean and prioritize queue by relevance")
    p_rank.add_argument("--queue", default="data/output/pipedrive_reactivation_queue.csv")
    p_rank.add_argument("--output", default="data/output/pipedrive_reactivation_queue_ranked.csv")
    p_rank.add_argument("--top-percent", type=int, default=20)

    p_top = sp.add_parser("export-top", help="Export top-N from ranked queue")
    p_top.add_argument("--queue", default="data/output/pipedrive_reactivation_queue_ranked.csv")
    p_top.add_argument("--output", default="data/output/pipedrive_top100.csv")
    p_top.add_argument("--top-percent", type=int, default=20)
    p_top.add_argument("--limit", type=int, default=100)

    p_mark = sp.add_parser("mark-top", help="Mark top contacts in Pipedrive with notes")
    p_mark.add_argument("--input", default="data/output/pipedrive_top100.csv")
    p_mark.add_argument("--limit", type=int, default=100)
    p_mark.add_argument("--tag", default="TOP100_REACTIVATION")

    p_prune = sp.add_parser("prune-dead-emails", help="Remove dead emails from Pipedrive persons")
    p_prune.add_argument("--input", required=True, help="CSV with person_id,email,verification_status/reason")

    p_rep = sp.add_parser("report", help="Generate queue dashboard snapshot")
    p_rep.add_argument("--queue", default="data/output/pipedrive_reactivation_queue_ranked_full_cleaned.csv")
    p_rep.add_argument("--top-percent", type=int, default=20)
    p_rep.add_argument("--output-json", default="data/output/reactivation_dashboard.json")
    p_rep.add_argument("--output-html", default="data/output/reactivation_dashboard.html")

    p_book = sp.add_parser("booked-call", help="When call is booked: create deal + call activity")
    p_book.add_argument("--person-id", required=True)
    p_book.add_argument("--person-name", default="")
    p_book.add_argument("--deal-title", default="")
    p_book.add_argument("--deal-value", type=float, default=0)
    p_book.add_argument("--currency", default="CHF")
    p_book.add_argument("--call-date", default="")
    p_book.add_argument("--call-note", default="")
    p_book.add_argument("--owner-user-id", default="", help="Force owner user id. If omitted, /users/me is used.")
    p_book.add_argument("--queue", default="data/output/pipedrive_reactivation_queue.csv")

    return ap


def main():
    parser = build_parser()
    args = parser.parse_args()

    no_api_commands = {"rank-queue", "export-top", "report"}
    if args.command in no_api_commands:
        if args.command == "rank-queue":
            cmd_rank_queue(args)
        elif args.command == "export-top":
            cmd_export_top(args)
        elif args.command == "report":
            cmd_report(args)
        return

    if not args.domain or not args.token:
        raise SystemExit("Set PIPEDRIVE_DOMAIN and PIPEDRIVE_API_TOKEN (or pass --domain/--token)")

    client = PipedriveClient(args.domain, args.token)

    if args.command == "build-queue":
        cmd_build_queue(args, client)
    elif args.command == "mark-top":
        cmd_mark_top_in_pipedrive(args, client)
    elif args.command == "prune-dead-emails":
        cmd_prune_dead_emails(args, client)
    elif args.command == "send-daily":
        cmd_send_daily(args, client)
    elif args.command == "booked-call":
        cmd_booked_call(args, client)


if __name__ == "__main__":
    main()
