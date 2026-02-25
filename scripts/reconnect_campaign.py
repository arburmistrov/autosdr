#!/usr/bin/env python3
"""Gmail reconnect campaign runner with follow-ups and Pipedrive deal creation."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
from datetime import datetime, timedelta, timezone
from email.utils import parseaddr
from email.message import EmailMessage
from pathlib import Path
from typing import Optional
from urllib import parse, request

try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing Gmail API dependencies. Install with: "
        "pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib"
    ) from exc


GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
]
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().replace(microsecond=0).isoformat()


def parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Reconnect campaign automation")
    p.add_argument("--credentials", default="secrets/gmail_credentials.json")
    p.add_argument("--token", default="secrets/gmail_token_reconnect_send.json")
    p.add_argument("--state", default="data/output/reconnect_campaign_state.json")
    p.add_argument("--approved-json", default="/Users/arseniyburmistrov/Downloads/gmail_reconnect_approved.json")
    p.add_argument("--max-per-run", type=int, default=80)
    p.add_argument("--followup-gap-days", type=int, default=4)
    p.add_argument("--followup-max", type=int, default=3)
    p.add_argument("--reply-window-days", type=int, default=30)
    p.add_argument("--send", action="store_true", help="Actually send emails. Without this flag run in dry-run mode.")
    p.add_argument("--signature", default="Best,\nArseniy")
    p.add_argument("--subject-prefix", default="")
    p.add_argument("--pipedrive-domain", default=os.getenv("PIPEDRIVE_DOMAIN", ""))
    p.add_argument("--pipedrive-token", default=os.getenv("PIPEDRIVE_API_TOKEN", ""))
    p.add_argument("--pipedrive-owner-id", default="", help="Optional owner user id for created deals.")
    p.add_argument("--command", choices=["init-queue", "sync-replies", "send-due", "run-cycle", "report"], required=True)
    return p.parse_args()


def gmail_service(credentials_path: Path, token_path: Path):
    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), GMAIL_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json(), encoding="utf-8")
    return build("gmail", "v1", credentials=creds)


def normalize_email(value: str) -> str:
    return (value or "").strip().lower()


def to_rfc2822_subject(subject: str) -> str:
    s = (subject or "").strip()
    if not s:
        return "Quick reconnect"
    return s


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def first_name_from_email(email: str) -> str:
    local = (email or "").split("@")[0]
    token = re.split(r"[._-]+", local)[0] if local else ""
    if token:
        return token[:1].upper() + token[1:]
    return "there"


def make_followup_body(contact: dict, step: int, signature: str) -> tuple[str, str]:
    first = (contact.get("first_name") or "").strip() or first_name_from_email(contact.get("email", ""))
    base_subject = contact.get("base_subject") or "Quick reconnect"
    if step == 1:
        subject = f"Re: {base_subject}"
        body = (
            f"Hi {first},\n\n"
            "Quick follow-up in case my previous note got buried.\n"
            "Interested in your current priorities and whether new digital solutions could be relevant.\n\n"
            f"{signature}"
        )
        return subject, body
    if step == 2:
        subject = f"Re: {base_subject}"
        body = (
            f"Hi {first},\n\n"
            "Wanted to check once more before I close the loop.\n"
            "If relevant, happy to do a short 20-minute sync.\n\n"
            f"{signature}"
        )
        return subject, body
    subject = f"Re: {base_subject}"
    body = (
        f"Hi {first},\n\n"
        "Final follow-up from my side.\n"
        "If this is not a priority now, no problem and I will close the thread.\n\n"
        f"{signature}"
    )
    return subject, body


def gmail_send(service, to_email: str, subject: str, body: str, thread_id: str = "") -> dict:
    msg = EmailMessage()
    msg["To"] = to_email
    msg["Subject"] = to_rfc2822_subject(subject)
    msg.set_content(body)
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    payload = {"raw": raw}
    if thread_id:
        payload["threadId"] = thread_id
    return service.users().messages().send(userId="me", body=payload).execute()


def thread_has_contact_reply(service, thread_id: str, contact_email: str, after_ts: datetime) -> tuple[bool, str, str]:
    if not thread_id:
        return False, "", ""
    thread = service.users().threads().get(userId="me", id=thread_id, format="metadata", metadataHeaders=["From", "Date", "Subject"]).execute()
    msgs = thread.get("messages", []) or []
    target = normalize_email(contact_email)
    latest_date = ""
    latest_subject = ""
    for m in msgs:
        internal_ms = int(m.get("internalDate", "0") or 0)
        msg_dt = datetime.fromtimestamp(internal_ms / 1000, tz=timezone.utc) if internal_ms else None
        if msg_dt and msg_dt <= after_ts:
            continue
        headers = {h.get("name", "").lower(): h.get("value", "") for h in m.get("payload", {}).get("headers", [])}
        from_header = headers.get("from", "")
        _, from_addr = parseaddr(from_header)
        from_email = normalize_email(from_addr or from_header)
        if from_email == target:
            latest_date = msg_dt.replace(microsecond=0).isoformat() if msg_dt else ""
            latest_subject = headers.get("subject", "")
            return True, latest_date, latest_subject
    return False, "", ""


def query_has_contact_reply(service, contact_email: str, after_ts: datetime) -> tuple[bool, str, str, str]:
    after_q = int(after_ts.timestamp())
    q = f"from:{contact_email} after:{after_q}"
    res = service.users().messages().list(userId="me", q=q, maxResults=5).execute()
    msgs = res.get("messages", []) or []
    if not msgs:
        return False, "", "", ""
    msg = service.users().messages().get(
        userId="me",
        id=msgs[0]["id"],
        format="metadata",
        metadataHeaders=["From", "Date", "Subject"],
    ).execute()
    headers = {h.get("name", "").lower(): h.get("value", "") for h in msg.get("payload", {}).get("headers", [])}
    internal_ms = int(msg.get("internalDate", "0") or 0)
    msg_dt = datetime.fromtimestamp(internal_ms / 1000, tz=timezone.utc) if internal_ms else None
    return (
        True,
        msg_dt.replace(microsecond=0).isoformat() if msg_dt else "",
        headers.get("subject", ""),
        msg.get("threadId", ""),
    )


def query_has_bounce(service, contact_email: str, after_ts: datetime) -> tuple[bool, str, str]:
    after_q = int(after_ts.timestamp())
    q = (
        f'after:{after_q} '
        f'from:(mailer-daemon OR postmaster) '
        f'(subject:"undeliverable" OR subject:"delivery status notification" OR subject:"mail delivery failed") '
        f'"{contact_email}"'
    )
    res = service.users().messages().list(userId="me", q=q, maxResults=3).execute()
    msgs = res.get("messages", []) or []
    if not msgs:
        return False, "", ""
    msg = service.users().messages().get(
        userId="me",
        id=msgs[0]["id"],
        format="metadata",
        metadataHeaders=["Subject"],
    ).execute()
    internal_ms = int(msg.get("internalDate", "0") or 0)
    msg_dt = datetime.fromtimestamp(internal_ms / 1000, tz=timezone.utc) if internal_ms else None
    headers = {h.get("name", "").lower(): h.get("value", "") for h in msg.get("payload", {}).get("headers", [])}
    return True, (msg_dt.replace(microsecond=0).isoformat() if msg_dt else ""), headers.get("subject", "")


class PipedriveClient:
    def __init__(self, domain: str, token: str):
        self.base = f"https://{domain}.pipedrive.com/api/v1"
        self.token = token

    def _url(self, path: str, params: Optional[dict] = None) -> str:
        data = dict(params or {})
        data["api_token"] = self.token
        return f"{self.base}{path}?{parse.urlencode(data, doseq=True)}"

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        with request.urlopen(self._url(path, params), timeout=60) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not payload.get("success"):
            raise RuntimeError(f"GET {path} failed: {payload}")
        return payload

    def post(self, path: str, body: dict) -> dict:
        req = request.Request(self._url(path), data=json.dumps(body).encode("utf-8"), method="POST")
        req.add_header("Content-Type", "application/json")
        with request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not payload.get("success"):
            raise RuntimeError(f"POST {path} failed: {payload}")
        return payload

    def find_person_by_email(self, email: str) -> Optional[int]:
        payload = self.get("/persons/search", {"term": email, "fields": "email", "exact_match": 1})
        items = ((payload.get("data") or {}).get("items") or [])
        for it in items:
            pid = ((it.get("item") or {}).get("id"))
            if pid:
                return int(pid)
        return None

    def create_person(self, name: str, email: str, org_name: str = "") -> int:
        body = {"name": name or email, "email": email}
        if org_name:
            body["org_name"] = org_name
        data = self.post("/persons", body).get("data") or {}
        pid = data.get("id")
        if not pid:
            raise RuntimeError(f"Could not create person for {email}")
        return int(pid)

    def me_user_id(self) -> int:
        payload = self.get("/users/me")
        uid = (payload.get("data") or {}).get("id")
        if not uid:
            raise RuntimeError("Could not resolve /users/me id")
        return int(uid)

    def create_deal_for_reply(
        self,
        person_id: int,
        owner_user_id: int,
        contact_name: str,
        contact_email: str,
        reply_subject: str,
        reply_date: str,
    ) -> int:
        title = f"Reconnect reply - {contact_name or contact_email}"
        body = {"title": title, "person_id": person_id, "status": "open", "user_id": owner_user_id}
        deal = self.post("/deals", body).get("data") or {}
        did = deal.get("id")
        if not did:
            raise RuntimeError(f"Deal creation failed for person {person_id}")
        note = (
            "Auto-created from reconnect campaign reply\n"
            f"Contact: {contact_email}\n"
            f"Reply date: {reply_date}\n"
            f"Reply subject: {reply_subject}"
        )
        self.post("/notes", {"person_id": person_id, "deal_id": int(did), "content": note})
        return int(did)


def normalize_contact_from_draft(row: dict, idx: int, now: datetime) -> dict:
    email = normalize_email(row.get("primary_contact_email", ""))
    if not email or not EMAIL_RE.match(email):
        return {}
    name = (row.get("primary_contact_name") or "").strip()
    if not name:
        name = first_name_from_email(email)
    return {
        "id": row.get("id") or f"approved::{idx}",
        "email": email,
        "name": name,
        "first_name": (name.split(" ")[0] if name else first_name_from_email(email)),
        "company_name": (row.get("company_name_guess") or "").strip(),
        "base_subject": to_rfc2822_subject((row.get("draft_subject") or "").strip()),
        "base_body": (row.get("draft_body") or "").strip(),
        "status": "queued",
        "stage": 0,
        "followups_sent": 0,
        "max_followups": 3,
        "thread_id": "",
        "last_message_id": "",
        "last_sent_at": "",
        "next_touch_at": now.isoformat(),
        "reply_at": "",
        "reply_subject": "",
        "reply_detected_via": "",
        "deal_id": "",
        "person_id": "",
        "error": "",
    }


def cmd_init_queue(args: argparse.Namespace) -> None:
    now = utc_now().replace(microsecond=0)
    approved = load_json(Path(args.approved_json), [])
    if not isinstance(approved, list):
        raise SystemExit(f"Approved JSON must be a list: {args.approved_json}")

    contacts: list[dict] = []
    seen = set()
    for i, row in enumerate(approved, start=1):
        if not isinstance(row, dict):
            continue
        item = normalize_contact_from_draft(row, i, now)
        if not item:
            continue
        if item["email"] in seen:
            continue
        seen.add(item["email"])
        item["max_followups"] = int(args.followup_max)
        contacts.append(item)

    payload = {
        "generated_at": iso_now(),
        "source_approved_json": str(Path(args.approved_json)),
        "settings": {
            "followup_gap_days": args.followup_gap_days,
            "followup_max": args.followup_max,
            "reply_window_days": args.reply_window_days,
        },
        "contacts": contacts,
    }
    save_json(Path(args.state), payload)
    print(f"Queue initialized: {len(contacts)} contacts -> {args.state}")


def process_reply_for_contact(contact: dict, service, after_ts: datetime) -> tuple[bool, str]:
    replied, at, subj = thread_has_contact_reply(service, contact.get("thread_id", ""), contact["email"], after_ts)
    if replied:
        contact["status"] = "replied"
        contact["reply_at"] = at
        contact["reply_subject"] = subj
        contact["reply_detected_via"] = "thread"
        return True, "thread"

    replied2, at2, subj2, th2 = query_has_contact_reply(service, contact["email"], after_ts)
    if replied2:
        contact["status"] = "replied"
        contact["reply_at"] = at2
        contact["reply_subject"] = subj2
        contact["reply_detected_via"] = "query"
        if not contact.get("thread_id") and th2:
            contact["thread_id"] = th2
        return True, "query"
    return False, ""


def ensure_pipedrive_deal(contact: dict, pd: PipedriveClient, owner_user_id: int) -> None:
    if contact.get("deal_id"):
        return
    person_id = contact.get("person_id")
    if person_id:
        person_id_int = int(person_id)
    else:
        person_id_int = pd.find_person_by_email(contact["email"]) or pd.create_person(
            name=contact.get("name", ""),
            email=contact["email"],
            org_name=contact.get("company_name", ""),
        )
        contact["person_id"] = str(person_id_int)
    deal_id = pd.create_deal_for_reply(
        person_id=person_id_int,
        owner_user_id=owner_user_id,
        contact_name=contact.get("name", ""),
        contact_email=contact["email"],
        reply_subject=contact.get("reply_subject", ""),
        reply_date=contact.get("reply_at", ""),
    )
    contact["deal_id"] = str(deal_id)


def cmd_sync_replies(args: argparse.Namespace) -> None:
    state = load_json(Path(args.state), {})
    contacts = state.get("contacts", [])
    if not contacts:
        raise SystemExit(f"No contacts in state: {args.state}")

    service = gmail_service(Path(args.credentials), Path(args.token))
    pd = None
    owner_user_id = 0
    if args.pipedrive_domain and args.pipedrive_token:
        pd = PipedriveClient(args.pipedrive_domain, args.pipedrive_token)
        owner_user_id = int(args.pipedrive_owner_id) if args.pipedrive_owner_id else pd.me_user_id()

    checked = 0
    replied_count = 0
    bounced_count = 0
    for c in contacts:
        if c.get("status") in {"replied", "closed", "invalid"}:
            continue
        if not c.get("last_sent_at"):
            continue
        after_ts = parse_iso(c["last_sent_at"]) - timedelta(minutes=2)
        bounced, bounce_at, bounce_subject = query_has_bounce(service, c["email"], after_ts)
        if bounced:
            c["status"] = "invalid"
            c["error"] = f"bounce:{bounce_subject or 'undeliverable'}"
            c["reply_at"] = bounce_at
            c["next_touch_at"] = ""
            checked += 1
            bounced_count += 1
            continue
        found, _ = process_reply_for_contact(c, service, after_ts)
        checked += 1
        if found:
            replied_count += 1
            if pd is not None:
                try:
                    ensure_pipedrive_deal(c, pd, owner_user_id)
                except Exception as exc:
                    c["error"] = f"pipedrive:{type(exc).__name__}:{exc}"

    state["last_sync_at"] = iso_now()
    save_json(Path(args.state), state)
    print(f"Reply sync done. Checked={checked}, replied={replied_count}, bounced={bounced_count}, state={args.state}")


def should_send_now(contact: dict, now: datetime) -> bool:
    if contact.get("status") in {"replied", "closed", "invalid"}:
        return False
    next_touch = contact.get("next_touch_at", "")
    if not next_touch:
        return False
    try:
        return parse_iso(next_touch) <= now
    except Exception:
        return True


def cmd_send_due(args: argparse.Namespace) -> None:
    state = load_json(Path(args.state), {})
    contacts = state.get("contacts", [])
    if not contacts:
        raise SystemExit(f"No contacts in state: {args.state}")

    service = gmail_service(Path(args.credentials), Path(args.token))
    now = utc_now()
    sent = 0
    dry = not args.send

    for c in contacts:
        if sent >= args.max_per_run:
            break
        if not should_send_now(c, now):
            continue
        if not EMAIL_RE.match(c.get("email", "")):
            c["status"] = "invalid"
            c["error"] = "invalid_email_syntax"
            continue

        stage = int(c.get("stage", 0))
        if stage == 0:
            subject = (args.subject_prefix + c.get("base_subject", "")).strip()
            body = c.get("base_body", "").strip()
            if not body:
                first = c.get("first_name") or first_name_from_email(c["email"])
                body = (
                    f"Hi {first},\n\n"
                    "It has been a while since we last spoke.\n"
                    "Interested in your current priorities and whether new digital solutions could be relevant.\n\n"
                    f"{args.signature}"
                )
        else:
            followup_step = stage
            if followup_step > int(c.get("max_followups", args.followup_max)):
                c["status"] = "closed"
                c["next_touch_at"] = ""
                continue
            subject, body = make_followup_body(c, followup_step, args.signature)

        try:
            if not dry:
                resp = gmail_send(service, c["email"], subject, body, c.get("thread_id", ""))
                c["thread_id"] = resp.get("threadId", c.get("thread_id", ""))
                c["last_message_id"] = resp.get("id", "")
            c["last_sent_at"] = iso_now()
            c["error"] = ""
            c["stage"] = stage + 1
            if stage == 0:
                c["status"] = "contacted"
            else:
                c["followups_sent"] = int(c.get("followups_sent", 0)) + 1
            if int(c["followups_sent"]) >= int(c.get("max_followups", args.followup_max)):
                c["status"] = "awaiting_reply"
                c["next_touch_at"] = ""
            else:
                c["status"] = "queued"
                c["next_touch_at"] = (now + timedelta(days=args.followup_gap_days)).replace(microsecond=0).isoformat()
            sent += 1
        except Exception as exc:
            c["error"] = f"send:{type(exc).__name__}:{exc}"

    state["last_send_run_at"] = iso_now()
    state["last_send_mode"] = "send" if args.send else "dry-run"
    save_json(Path(args.state), state)
    print(f"Send due complete. sent={sent}, mode={'SEND' if args.send else 'DRY-RUN'}, state={args.state}")


def cmd_report(args: argparse.Namespace) -> None:
    state = load_json(Path(args.state), {})
    contacts = state.get("contacts", [])
    by_status = {}
    due_now = 0
    now = utc_now()
    for c in contacts:
        st = c.get("status", "unknown")
        by_status[st] = by_status.get(st, 0) + 1
        if should_send_now(c, now):
            due_now += 1
    payload = {
        "state": str(Path(args.state)),
        "generated_at": iso_now(),
        "total_contacts": len(contacts),
        "due_now": due_now,
        "by_status": dict(sorted(by_status.items())),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def cmd_run_cycle(args: argparse.Namespace) -> None:
    cmd_sync_replies(args)
    cmd_send_due(args)
    cmd_report(args)


def main() -> None:
    args = parse_args()
    if args.command == "init-queue":
        cmd_init_queue(args)
        return

    if not Path(args.state).exists():
        raise SystemExit(f"State not found: {args.state}. Run --command init-queue first.")

    if args.command == "sync-replies":
        cmd_sync_replies(args)
    elif args.command == "send-due":
        cmd_send_due(args)
    elif args.command == "run-cycle":
        cmd_run_cycle(args)
    elif args.command == "report":
        cmd_report(args)


if __name__ == "__main__":
    main()
