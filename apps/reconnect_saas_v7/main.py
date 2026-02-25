#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import secrets
import sqlite3
import ast
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parseaddr
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel, EmailStr

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("RECONNECT_SAAS_DB", "data/reconnect_saas_v7.db"))
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://127.0.0.1:8080").rstrip("/")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_OAUTH_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET", "")

FREE_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "yahoo.com",
    "yahoo.co.uk",
    "hotmail.com",
    "outlook.com",
    "icloud.com",
    "aol.com",
    "mail.com",
    "proton.me",
    "protonmail.com",
    "yandex.com",
    "gmx.com",
    "msn.com",
    "live.com",
    "qq.com",
    "163.com",
    "126.com",
}

NOISE_DOMAINS = {
    "email.reuters.com",
    "github.com",
    "linkedin.com",
    "mail.linkedin.com",
    "notifications.github.com",
    "pipedrive.com",
    "slack.com",
    "atlassian.com",
    "noreply.github.com",
    "notion.so",
    "mail.notion.so",
    "docusign.net",
    "google.com",
    "googlemail.com",
    "amazonaws.com",
}

NOISE_SUBJECT_PATTERNS = [
    re.compile(r"\\bnewsletter\\b", re.IGNORECASE),
    re.compile(r"\\bnotification\\b", re.IGNORECASE),
    re.compile(r"\\bsecurity alert\\b", re.IGNORECASE),
    re.compile(r"\\bpassword\\b", re.IGNORECASE),
    re.compile(r"\\binvoice\\b", re.IGNORECASE),
    re.compile(r"\\bpayment due\\b", re.IGNORECASE),
    re.compile(r"\\bverification\\b", re.IGNORECASE),
    re.compile(r"\\bsubscription\\b", re.IGNORECASE),
]

AUTOMATED_SENDER_PATTERNS = [
    re.compile(r"\\bno[-_.]?reply\\b", re.IGNORECASE),
    re.compile(r"\\bdo[-_.]?not[-_.]?reply\\b", re.IGNORECASE),
    re.compile(r"\\bnotification\\b", re.IGNORECASE),
    re.compile(r"\\bautomated\\b", re.IGNORECASE),
    re.compile(r"\\balerts?\\b", re.IGNORECASE),
]

BUSINESS_KEYWORDS = {
    "meeting",
    "call",
    "proposal",
    "partnership",
    "pricing",
    "scope",
    "timeline",
    "deal",
    "follow up",
    "follow-up",
    "demo",
    "services",
    "nda",
    "kickoff",
    "project",
    "opportunity",
    "client",
    "customer",
    "intro",
    "introduction",
    "next step",
}

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
EXTRACT_RE = re.compile(r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})")


@dataclass
class GmailConn:
    user_email: str
    connected_email: str
    access_token: str
    refresh_token: str
    expires_at: str


class SaveUserPayload(BaseModel):
    name: str
    email: EmailStr


class SavePipedrivePayload(BaseModel):
    email: EmailStr
    domain: str
    api_token: str


class QueuePayload(BaseModel):
    email: EmailStr
    max_messages: int = 180


class DecisionPayload(BaseModel):
    email: EmailStr
    organization_domain: str
    status: str


class DraftPayload(BaseModel):
    email: EmailStr


app = FastAPI(title="Reconnect SaaS v7 API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def db_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with db_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
              email TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS gmail_connections (
              user_email TEXT PRIMARY KEY,
              connected_email TEXT NOT NULL,
              access_token TEXT NOT NULL,
              refresh_token TEXT,
              expires_at TEXT,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pipedrive_connections (
              user_email TEXT PRIMARY KEY,
              domain TEXT NOT NULL,
              api_token TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS oauth_states (
              state TEXT PRIMARY KEY,
              user_email TEXT NOT NULL,
              redirect_uri TEXT,
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS queue_candidates (
              user_email TEXT NOT NULL,
              organization_domain TEXT NOT NULL,
              organization_name TEXT NOT NULL,
              primary_contact_email TEXT NOT NULL,
              primary_contact_name TEXT,
              last_message_at TEXT,
              threads_count INTEGER NOT NULL DEFAULT 0,
              followup_score INTEGER NOT NULL DEFAULT 0,
              auto_status TEXT NOT NULL DEFAULT 'pending',
              status TEXT NOT NULL DEFAULT 'pending',
              summary_text TEXT,
              payload_json TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (user_email, organization_domain)
            );
            """
        )
        try:
            conn.execute("ALTER TABLE oauth_states ADD COLUMN redirect_uri TEXT")
        except sqlite3.OperationalError:
            pass


@app.on_event("startup")
def on_startup() -> None:
    init_db()


init_db()


@app.get("/")
def index() -> FileResponse:
    return FileResponse(BASE_DIR / "static" / "index.html")


@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "time": now_iso()}


@app.post("/api/users/save")
def save_user(payload: SaveUserPayload) -> dict[str, Any]:
    email = str(payload.email).strip().lower()
    ts = now_iso()
    with db_conn() as conn:
        conn.execute(
            """
            INSERT INTO users(email, name, created_at, updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(email) DO UPDATE SET
              name=excluded.name,
              updated_at=excluded.updated_at
            """,
            (email, payload.name.strip(), ts, ts),
        )
    return {"ok": True, "email": email}


@app.get("/api/users/{email}/status")
def user_status(email: str) -> dict[str, Any]:
    user_email = email.strip().lower()
    with db_conn() as conn:
        u = conn.execute("SELECT email,name FROM users WHERE email=?", (user_email,)).fetchone()
        g = conn.execute(
            "SELECT connected_email,expires_at FROM gmail_connections WHERE user_email=?", (user_email,)
        ).fetchone()
        p = conn.execute("SELECT domain FROM pipedrive_connections WHERE user_email=?", (user_email,)).fetchone()
        qs = conn.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN status='approved' THEN 1 ELSE 0 END) AS approved,
              SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END) AS rejected,
              SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending
            FROM queue_candidates WHERE user_email=?
            """,
            (user_email,),
        ).fetchone()
    return {
        "ok": True,
        "user_exists": bool(u),
        "name": (u["name"] if u else ""),
        "gmail_connected": bool(g),
        "gmail_connected_email": (g["connected_email"] if g else ""),
        "gmail_expires_at": (g["expires_at"] if g else ""),
        "pipedrive_connected": bool(p),
        "pipedrive_domain": (p["domain"] if p else ""),
        "queue": {
            "total": int((qs["total"] if qs and qs["total"] is not None else 0) or 0),
            "approved": int((qs["approved"] if qs and qs["approved"] is not None else 0) or 0),
            "rejected": int((qs["rejected"] if qs and qs["rejected"] is not None else 0) or 0),
            "pending": int((qs["pending"] if qs and qs["pending"] is not None else 0) or 0),
        },
    }


@app.post("/api/pipedrive/connect")
def pipedrive_connect(payload: SavePipedrivePayload) -> dict[str, Any]:
    email = str(payload.email).strip().lower()
    domain = payload.domain.strip().lower()
    token = payload.api_token.strip()
    if not domain or not token:
        raise HTTPException(status_code=400, detail="domain and token are required")
    ts = now_iso()
    with db_conn() as conn:
        conn.execute(
            """
            INSERT INTO pipedrive_connections(user_email, domain, api_token, updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(user_email) DO UPDATE SET
              domain=excluded.domain,
              api_token=excluded.api_token,
              updated_at=excluded.updated_at
            """,
            (email, domain, token, ts),
        )
    return {"ok": True, "email": email, "domain": domain}


@app.post("/api/pipedrive/disconnect")
def pipedrive_disconnect(payload: SaveUserPayload) -> dict[str, Any]:
    email = str(payload.email).strip().lower()
    with db_conn() as conn:
        conn.execute("DELETE FROM pipedrive_connections WHERE user_email=?", (email,))
    return {"ok": True}


@app.post("/api/gmail/disconnect")
def gmail_disconnect(payload: SaveUserPayload) -> dict[str, Any]:
    email = str(payload.email).strip().lower()
    with db_conn() as conn:
        conn.execute("DELETE FROM gmail_connections WHERE user_email=?", (email,))
    return {"ok": True}


def require_google_config() -> None:
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="Google OAuth is not configured on server")


@app.get("/api/auth/google/start")
def google_start(request: Request, email: str = Query(...)) -> RedirectResponse:
    require_google_config()
    user_email = email.strip().lower()
    if not EMAIL_RE.match(user_email):
        raise HTTPException(status_code=400, detail="invalid email")

    state = secrets.token_urlsafe(24)
    redirect_uri = str(request.url_for("google_callback"))
    ts = now_iso()
    with db_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO oauth_states(state,user_email,redirect_uri,created_at) VALUES(?,?,?,?)",
            (state, user_email, redirect_uri, ts),
        )

    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile https://www.googleapis.com/auth/gmail.readonly",
        "access_type": "offline",
        "include_granted_scopes": "true",
        "prompt": "consent",
        "state": state,
        "login_hint": user_email,
    }
    return RedirectResponse(f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}")


async def exchange_code(code: str, redirect_uri: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    if res.status_code >= 400:
        raise HTTPException(status_code=400, detail=f"token_exchange_failed: {res.text[:240]}")
    return res.json()


async def refresh_access_token(refresh_token: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    if res.status_code >= 400:
        raise HTTPException(status_code=400, detail=f"token_refresh_failed: {res.text[:240]}")
    return res.json()


async def gmail_profile(access_token: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/profile",
            headers={"Authorization": f"Bearer {access_token}"},
        )
    if res.status_code >= 400:
        raise HTTPException(status_code=400, detail=f"gmail_profile_failed: {res.text[:240]}")
    return res.json()


@app.get("/api/auth/google/callback")
async def google_callback(code: Optional[str] = None, state: Optional[str] = None, error: Optional[str] = None):
    if error:
        return RedirectResponse(f"/?gmail=error&reason={error}")
    if not code or not state:
        return RedirectResponse("/?gmail=error&reason=missing_code_or_state")

    with db_conn() as conn:
        st = conn.execute("SELECT user_email, redirect_uri FROM oauth_states WHERE state=?", (state,)).fetchone()
        conn.execute("DELETE FROM oauth_states WHERE state=?", (state,))

    if not st:
        return RedirectResponse("/?gmail=error&reason=invalid_state")

    user_email = st["user_email"]
    redirect_uri = (st["redirect_uri"] or f"{APP_BASE_URL}/api/auth/google/callback").strip()
    token_payload = await exchange_code(code, redirect_uri)
    access_token = str(token_payload.get("access_token", ""))
    if not access_token:
        return RedirectResponse("/?gmail=error&reason=no_access_token")

    refresh_token = str(token_payload.get("refresh_token", ""))
    expires_in = int(token_payload.get("expires_in", 3600))
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=max(60, expires_in))).replace(microsecond=0).isoformat()
    profile = await gmail_profile(access_token)
    connected_email = str(profile.get("emailAddress", user_email)).strip().lower()
    ts = now_iso()

    with db_conn() as conn:
        old = conn.execute("SELECT refresh_token FROM gmail_connections WHERE user_email=?", (user_email,)).fetchone()
        final_refresh = refresh_token or (old["refresh_token"] if old else "")
        conn.execute(
            """
            INSERT INTO gmail_connections(user_email, connected_email, access_token, refresh_token, expires_at, updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(user_email) DO UPDATE SET
              connected_email=excluded.connected_email,
              access_token=excluded.access_token,
              refresh_token=excluded.refresh_token,
              expires_at=excluded.expires_at,
              updated_at=excluded.updated_at
            """,
            (user_email, connected_email, access_token, final_refresh, expires_at, ts),
        )

    return RedirectResponse(f"/?gmail=connected&email={connected_email}")


def load_gmail_connection(user_email: str) -> Optional[GmailConn]:
    with db_conn() as conn:
        row = conn.execute(
            "SELECT user_email, connected_email, access_token, refresh_token, expires_at FROM gmail_connections WHERE user_email=?",
            (user_email,),
        ).fetchone()
    if not row:
        return None
    return GmailConn(
        user_email=row["user_email"],
        connected_email=row["connected_email"],
        access_token=row["access_token"],
        refresh_token=row["refresh_token"] or "",
        expires_at=row["expires_at"] or "",
    )


async def ensure_valid_access_token(conn: GmailConn) -> str:
    if not conn.expires_at:
        return conn.access_token
    try:
        expires_at = datetime.fromisoformat(conn.expires_at.replace("Z", "+00:00"))
    except Exception:
        return conn.access_token
    if expires_at - datetime.now(timezone.utc) > timedelta(minutes=2):
        return conn.access_token
    if not conn.refresh_token:
        return conn.access_token

    refreshed = await refresh_access_token(conn.refresh_token)
    new_access = str(refreshed.get("access_token", "")) or conn.access_token
    new_expires = int(refreshed.get("expires_in", 3600))
    expires_at_new = (datetime.now(timezone.utc) + timedelta(seconds=max(60, new_expires))).replace(microsecond=0).isoformat()
    with db_conn() as db:
        db.execute(
            "UPDATE gmail_connections SET access_token=?, expires_at=?, updated_at=? WHERE user_email=?",
            (new_access, expires_at_new, now_iso(), conn.user_email),
        )
    return new_access


def extract_emails(value: str) -> list[str]:
    found = {m.group(1).strip().lower() for m in EXTRACT_RE.finditer(value or "")}
    if not found and value:
        parsed = parseaddr(value)[1].strip().lower()
        if parsed and EMAIL_RE.match(parsed):
            found.add(parsed)
    return sorted(found)


def guess_name_from_header(value: str, email: str) -> str:
    if not value:
        return ""
    name, addr = parseaddr(value)
    if addr and addr.strip().lower() == email:
        clean = (name or "").strip().strip('"')
        if clean:
            return clean
    return ""


def company_name_from_domain(domain: str) -> str:
    root = (domain or "").split(".", 1)[0]
    if not root:
        return domain
    parts = [p for p in re.split(r"[-_]+", root) if p]
    out = []
    for p in parts:
        out.append(p.upper() if p.isalpha() and len(p) <= 4 else p.title())
    return " ".join(out)


def is_noise_sender(email: str) -> bool:
    local = (email.split("@", 1)[0] if "@" in email else "").lower()
    return any(p.search(local) for p in AUTOMATED_SENDER_PATTERNS)


def is_excluded_domain(domain: str, own_domain: str) -> bool:
    dom = (domain or "").lower().strip()
    if not dom:
        return True
    if own_domain and dom == own_domain:
        return True
    if dom in FREE_DOMAINS:
        return True
    if dom in NOISE_DOMAINS:
        return True
    if any(dom.endswith(f".{x}") for x in NOISE_DOMAINS):
        return True
    return False


def has_noise_subject(subject: str) -> bool:
    s = subject or ""
    return any(p.search(s) for p in NOISE_SUBJECT_PATTERNS)


def infer_first_name(contact_name: str, email: str) -> str:
    if contact_name:
        first = re.split(r"\\s+", contact_name.strip())[0]
        if first and re.match(r"^[A-Za-z][A-Za-z'\\-]{1,30}$", first):
            return first
    local = (email.split("@", 1)[0] if "@" in email else "").strip().lower()
    token = re.split(r"[._-]", local)[0] if local else ""
    if token and re.match(r"^[a-z]{2,20}$", token):
        return token.capitalize()
    return "there"


def text_relevance_score(subjects: list[str], snippets: list[str], stakeholders_count: int, days_since_last: int) -> int:
    text = " ".join(subjects + snippets).lower()
    keyword_hits = sum(1 for kw in BUSINESS_KEYWORDS if kw in text)
    score = 35 + min(35, keyword_hits * 7)
    score += min(12, stakeholders_count * 3)
    if days_since_last >= 14:
        score += 8
    if days_since_last >= 45:
        score += 6
    if "newsletter" in text or "unsubscribe" in text:
        score -= 30
    return max(0, min(100, score))


def summarize_topics(subject_counter: Counter[str]) -> list[str]:
    topics = []
    for sub, _ in subject_counter.most_common(5):
        v = (sub or "").strip()
        if not v:
            continue
        clean = re.sub(r"^(re|fw|fwd)\\s*:\\s*", "", v, flags=re.IGNORECASE).strip()
        if clean and clean.lower() not in {x.lower() for x in topics}:
            topics.append(clean)
    return topics[:3]


def load_status_map(user_email: str) -> dict[str, str]:
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT organization_domain,status FROM queue_candidates WHERE user_email=?",
            (user_email,),
        ).fetchall()
    return {str(r["organization_domain"]): str(r["status"] or "pending") for r in rows}


def save_queue_rows(user_email: str, rows: list[dict[str, Any]]) -> None:
    existing_status = load_status_map(user_email)
    ts = now_iso()
    with db_conn() as conn:
        for row in rows:
            domain = str(row.get("organization_domain", "")).strip().lower()
            if not domain:
                continue
            default_status = "pending" if row.get("auto_status") == "pending" else "rejected"
            status = existing_status.get(domain, default_status)
            payload_json = json.dumps(row, ensure_ascii=False)
            conn.execute(
                """
                INSERT INTO queue_candidates(
                  user_email, organization_domain, organization_name, primary_contact_email, primary_contact_name,
                  last_message_at, threads_count, followup_score, auto_status, status, summary_text, payload_json, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(user_email, organization_domain) DO UPDATE SET
                  organization_name=excluded.organization_name,
                  primary_contact_email=excluded.primary_contact_email,
                  primary_contact_name=excluded.primary_contact_name,
                  last_message_at=excluded.last_message_at,
                  threads_count=excluded.threads_count,
                  followup_score=excluded.followup_score,
                  auto_status=excluded.auto_status,
                  summary_text=excluded.summary_text,
                  payload_json=excluded.payload_json,
                  updated_at=excluded.updated_at
                """,
                (
                    user_email,
                    domain,
                    row.get("organization_name", ""),
                    row.get("primary_contact_email", ""),
                    row.get("primary_contact_name", ""),
                    row.get("last_message_at", ""),
                    int(row.get("threads_count", 0)),
                    int(row.get("followup_score", 0)),
                    row.get("auto_status", "pending"),
                    status,
                    row.get("summary", ""),
                    payload_json,
                    ts,
                ),
            )


def parse_iso(v: str) -> datetime:
    try:
        return datetime.fromisoformat((v or "").replace("Z", "+00:00"))
    except Exception:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)


def parse_row_payload(raw: str) -> dict[str, Any]:
    try:
        out = json.loads(raw or "{}")
    except Exception:
        try:
            out = ast.literal_eval(raw or "{}")
        except Exception:
            out = {}
    return out if isinstance(out, dict) else {}


def load_queue_rows(user_email: str) -> list[dict[str, Any]]:
    with db_conn() as conn:
        rows = conn.execute(
            """
            SELECT organization_domain, status, auto_status, payload_json
            FROM queue_candidates
            WHERE user_email=?
            """,
            (user_email,),
        ).fetchall()

    out: list[dict[str, Any]] = []
    for r in rows:
        payload = parse_row_payload(str(r["payload_json"] or ""))
        if not payload:
            continue
        payload["organization_domain"] = r["organization_domain"]
        payload["status"] = str(r["status"] or "pending")
        payload["auto_status"] = str(r["auto_status"] or payload.get("auto_status", "pending"))
        out.append(payload)

    out.sort(
        key=lambda x: (
            0 if x.get("status") == "pending" else 1,
            0 if x.get("auto_status") == "pending" else 1,
            -int(x.get("followup_score", 0)),
            -parse_iso(str(x.get("last_message_at", ""))).timestamp(),
        )
    )
    for i, row in enumerate(out, start=1):
        row["rank"] = i
    return out


@app.get("/api/queue")
def queue_get(email: str = Query(...)) -> dict[str, Any]:
    user_email = email.strip().lower()
    rows = load_queue_rows(user_email)
    return {
        "ok": True,
        "summary": {
            "total": len(rows),
            "pending": sum(1 for r in rows if r.get("status") == "pending"),
            "approved": sum(1 for r in rows if r.get("status") == "approved"),
            "rejected": sum(1 for r in rows if r.get("status") == "rejected"),
        },
        "rows": rows,
    }


@app.post("/api/queue/decision")
def queue_decision(payload: DecisionPayload) -> dict[str, Any]:
    user_email = str(payload.email).strip().lower()
    domain = payload.organization_domain.strip().lower()
    status = payload.status.strip().lower()
    if status not in {"pending", "approved", "rejected"}:
        raise HTTPException(status_code=400, detail="invalid_status")

    with db_conn() as conn:
        cur = conn.execute(
            "UPDATE queue_candidates SET status=?, updated_at=? WHERE user_email=? AND organization_domain=?",
            (status, now_iso(), user_email, domain),
        )
    if cur.rowcount < 1:
        raise HTTPException(status_code=404, detail="row_not_found")
    return {"ok": True, "organization_domain": domain, "status": status}


@app.post("/api/queue/generate")
async def generate_queue(payload: QueuePayload) -> dict[str, Any]:
    try:
        user_email = str(payload.email).strip().lower()
        gmail_conn = load_gmail_connection(user_email)
        if not gmail_conn:
            raise HTTPException(status_code=400, detail="gmail_not_connected")

        access_token = await ensure_valid_access_token(gmail_conn)
        max_msgs = max(30, min(300, int(payload.max_messages)))

        async with httpx.AsyncClient(timeout=40) as client:
            list_res = await client.get(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages",
                params={"maxResults": max_msgs, "q": "-in:chats"},
                headers={"Authorization": f"Bearer {access_token}"},
            )
            if list_res.status_code >= 400:
                raise HTTPException(status_code=400, detail=f"gmail_list_failed: {list_res.text[:240]}")
            ids = list_res.json().get("messages") or []

            own_domain = gmail_conn.connected_email.split("@")[-1] if "@" in gmail_conn.connected_email else ""
            orgs: dict[str, dict[str, Any]] = {}

            for item in ids:
                mid = item.get("id")
                if not mid:
                    continue

                msg_res = await client.get(
                    f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{mid}",
                    params={
                        "format": "metadata",
                        "metadataHeaders": ["From", "To", "Cc", "Subject"],
                    },
                    headers={"Authorization": f"Bearer {access_token}"},
                )
                if msg_res.status_code >= 400:
                    continue

                msg = msg_res.json()
                headers = {
                    str(h.get("name", "")).lower(): h.get("value", "")
                    for h in (msg.get("payload", {}).get("headers") or [])
                }
                subject = str(headers.get("subject", "")).strip()
                snippet = str(msg.get("snippet", "") or "").strip()
                thread_id = str(msg.get("threadId", "") or "")

            from_value = str(headers.get("from", ""))
            from_emails = extract_emails(from_value)
            from_email = from_emails[0] if from_emails else ""
            all_emails = set(from_emails)
            for h in ("to", "cc"):
                for addr in extract_emails(str(headers.get(h, ""))):
                    all_emails.add(addr)

            ts = datetime.now(timezone.utc)
            try:
                ts = datetime.fromtimestamp(int(msg.get("internalDate", "0")) / 1000, tz=timezone.utc)
            except Exception:
                pass
            iso_ts = ts.replace(microsecond=0).isoformat()

            domains: set[str] = set()
            for em in all_emails:
                if "@" not in em:
                    continue
                dom = em.split("@", 1)[1].lower()
                if is_excluded_domain(dom, own_domain):
                    continue
                domains.add(dom)

            if not domains:
                continue

            for dom in domains:
                org = orgs.get(dom)
                if org is None:
                    org = {
                        "organization_domain": dom,
                        "organization_name": company_name_from_domain(dom),
                        "stakeholders": {},
                        "threads": {},
                        "subjects": Counter(),
                        "snippets": [],
                        "message_count": 0,
                        "last_message_at": "",
                        "primary_contact_email": "",
                        "primary_contact_name": "",
                    }
                    orgs[dom] = org

                org["message_count"] += 1
                if subject:
                    org["subjects"][subject] += 1
                if snippet and len(org["snippets"]) < 8:
                    org["snippets"].append(snippet)

                if not org["last_message_at"] or iso_ts > org["last_message_at"]:
                    org["last_message_at"] = iso_ts
                    if from_email.endswith("@" + dom):
                        org["primary_contact_email"] = from_email
                        org["primary_contact_name"] = guess_name_from_header(from_value, from_email)

                for em in all_emails:
                    if "@" not in em or not em.endswith("@" + dom):
                        continue
                    if is_noise_sender(em):
                        continue
                    if em not in org["stakeholders"]:
                        org["stakeholders"][em] = {
                            "email": em,
                            "name": "",
                            "touches": 0,
                            "last_message_at": iso_ts,
                        }
                    org["stakeholders"][em]["touches"] += 1
                    if iso_ts > org["stakeholders"][em]["last_message_at"]:
                        org["stakeholders"][em]["last_message_at"] = iso_ts
                    if from_email == em and from_value:
                        org["stakeholders"][em]["name"] = guess_name_from_header(from_value, em)

                if thread_id:
                    thread = org["threads"].get(thread_id)
                    if thread is None:
                        thread = {
                            "thread_id": thread_id,
                            "subject": subject,
                            "last_message_at": iso_ts,
                            "messages": 0,
                            "sample": snippet,
                        }
                        org["threads"][thread_id] = thread
                    thread["messages"] += 1
                    if iso_ts > thread["last_message_at"]:
                        thread["last_message_at"] = iso_ts
                        if subject:
                            thread["subject"] = subject
                        if snippet:
                            thread["sample"] = snippet

        rows: list[dict[str, Any]] = []
        now_dt = datetime.now(timezone.utc)

        for dom, org in orgs.items():
            threads = list(org["threads"].values())
            stakeholders = list(org["stakeholders"].values())
            if not threads or not stakeholders:
                continue

        topics = summarize_topics(org["subjects"])
        last_dt = parse_iso(str(org["last_message_at"]))
        days_since_last = max(0, (now_dt - last_dt).days)

        primary = org["primary_contact_email"]
        if not primary:
            top_st = sorted(stakeholders, key=lambda x: (int(x["touches"]), x["last_message_at"]), reverse=True)[0]
            primary = str(top_st["email"])
            org["primary_contact_name"] = str(top_st.get("name", ""))

        subject_noise = sum(1 for t in topics if has_noise_subject(t))
        business_score = text_relevance_score(topics, org["snippets"], len(stakeholders), days_since_last)
        followup_score = max(0, min(100, business_score + min(10, len(threads))))
        auto_status = "pending"
        reasons: list[str] = []
        if subject_noise >= 2:
            auto_status = "auto_reject"
            reasons.append("newsletter_or_system_subject")
        if all(is_noise_sender(s["email"]) for s in stakeholders):
            auto_status = "auto_reject"
            reasons.append("automated_senders_only")
        if followup_score < 45:
            auto_status = "auto_reject"
            reasons.append("low_relevance")

        stakeholders_sorted = sorted(stakeholders, key=lambda x: (int(x["touches"]), x["last_message_at"]), reverse=True)
        threads_sorted = sorted(threads, key=lambda x: x["last_message_at"], reverse=True)

        summary = (
            f"{len(threads_sorted)} threads merged across {len(stakeholders_sorted)} stakeholders. "
            f"Top topics: {', '.join(topics) if topics else 'n/a'}."
        )

        rows.append(
            {
                "organization_domain": dom,
                "organization_name": org["organization_name"],
                "primary_contact_email": primary,
                "primary_contact_name": org.get("primary_contact_name", ""),
                "threads_count": len(threads_sorted),
                "message_count": int(org["message_count"]),
                "last_message_at": org["last_message_at"],
                "days_since_last": days_since_last,
                "followup_score": followup_score,
                "business_score": business_score,
                "auto_status": auto_status,
                "auto_reasons": reasons,
                "summary": summary,
                "topics": topics,
                "stakeholders": stakeholders_sorted[:12],
                "threads": threads_sorted[:15],
                "last_messages": org["snippets"][:5],
                "status": "pending",
            }
        )

        rows.sort(
            key=lambda r: (
                0 if r.get("auto_status") == "pending" else 1,
                -int(r.get("followup_score", 0)),
                -parse_iso(str(r.get("last_message_at", ""))).timestamp(),
            )
        )

        save_queue_rows(user_email, rows)
        saved_rows = load_queue_rows(user_email)

        return {
            "ok": True,
            "summary": {
                "organizations": len(saved_rows),
                "messages_scanned": len(ids),
                "connected_email": gmail_conn.connected_email,
                "pending": sum(1 for r in saved_rows if r.get("status") == "pending"),
                "approved": sum(1 for r in saved_rows if r.get("status") == "approved"),
                "rejected": sum(1 for r in saved_rows if r.get("status") == "rejected"),
            },
            "rows": saved_rows,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"queue_generate_failed: {type(exc).__name__}: {exc}") from exc


@app.post("/api/drafts/generate")
def generate_drafts(payload: DraftPayload) -> dict[str, Any]:
    user_email = str(payload.email).strip().lower()
    with db_conn() as conn:
        user = conn.execute("SELECT name FROM users WHERE email=?", (user_email,)).fetchone()

    owner_name = (str(user["name"]).strip() if user and user["name"] else "Arseniy")
    rows = load_queue_rows(user_email)
    approved = [r for r in rows if r.get("status") == "approved"]

    drafts: list[dict[str, Any]] = []
    for r in approved:
        to_email = str(r.get("primary_contact_email", "")).strip().lower()
        if not to_email:
            continue
        first_name = infer_first_name(str(r.get("primary_contact_name", "")), to_email)
        body = (
            f"Hi {first_name},\\n\\n"
            "It has been a while since we last spoke.\\n"
            "Interested in your current priorities and whether new digital solutions could be relevant.\\n\\n"
            f"Best,\\n{owner_name}"
        )
        drafts.append(
            {
                "organization": r.get("organization_name") or r.get("organization_domain"),
                "organization_domain": r.get("organization_domain", ""),
                "to": to_email,
                "primary_contact_name": r.get("primary_contact_name", ""),
                "topics": r.get("topics", []),
                "summary": r.get("summary", ""),
                "draft": body,
            }
        )

    return {"ok": True, "count": len(drafts), "owner_name": owner_name, "drafts": drafts}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
