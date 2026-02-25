#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import secrets
import sqlite3
import ast
import asyncio
import base64
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
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
GENERIC_LOCALPARTS = {
    "info",
    "hello",
    "team",
    "contact",
    "office",
    "admin",
    "support",
    "help",
    "sales",
    "marketing",
    "newsletter",
    "news",
    "updates",
    "events",
    "security",
    "notifications",
    "noreply",
    "no-reply",
    "do-not-reply",
    "donotreply",
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
    max_messages: Optional[int] = None


class DecisionPayload(BaseModel):
    email: EmailStr
    organization_domain: str
    status: str


class DraftPayload(BaseModel):
    email: EmailStr


class QueueJobControlPayload(BaseModel):
    email: EmailStr
    job_id: str


class DraftUpdatePayload(BaseModel):
    email: EmailStr
    organization_domain: str
    to: EmailStr
    final_text: str
    subject_text: Optional[str] = None


class DraftDecisionPayload(BaseModel):
    email: EmailStr
    organization_domain: str
    to: EmailStr
    status: str


class CampaignStartPayload(BaseModel):
    email: EmailStr
    followups_count: int


app = FastAPI(title="Reconnect SaaS v7 API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory async jobs for long-running mailbox scans.
QUEUE_JOBS: dict[str, dict[str, Any]] = {}
QUEUE_JOB_TASKS: dict[str, asyncio.Task[Any]] = {}
CAMPAIGN_WORKER_TASK: Optional[asyncio.Task[Any]] = None


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
            CREATE TABLE IF NOT EXISTS followup_drafts (
              user_email TEXT NOT NULL,
              organization_domain TEXT NOT NULL,
              organization_name TEXT NOT NULL,
              to_email TEXT NOT NULL,
              primary_contact_name TEXT,
              context_summary TEXT,
              topics_json TEXT NOT NULL,
              subject_text TEXT NOT NULL DEFAULT 'Quick reconnect',
              draft_text TEXT NOT NULL,
              final_text TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'pending',
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (user_email, organization_domain, to_email)
            );
            CREATE TABLE IF NOT EXISTS campaigns (
              campaign_id TEXT PRIMARY KEY,
              user_email TEXT NOT NULL,
              status TEXT NOT NULL,
              followups_count INTEGER NOT NULL,
              total_targets INTEGER NOT NULL DEFAULT 0,
              sent_count INTEGER NOT NULL DEFAULT 0,
              replied_count INTEGER NOT NULL DEFAULT 0,
              deals_created INTEGER NOT NULL DEFAULT 0,
              started_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS campaign_targets (
              campaign_id TEXT NOT NULL,
              user_email TEXT NOT NULL,
              organization_domain TEXT NOT NULL,
              organization_name TEXT NOT NULL,
              to_email TEXT NOT NULL,
              token TEXT NOT NULL,
              subject_text TEXT NOT NULL DEFAULT 'Quick reconnect',
              draft_text TEXT NOT NULL,
              sent_count INTEGER NOT NULL DEFAULT 0,
              max_sends INTEGER NOT NULL,
              last_sent_at TEXT,
              next_send_at TEXT NOT NULL,
              replied_at TEXT,
              pipedrive_deal_id TEXT,
              status TEXT NOT NULL DEFAULT 'active',
              updated_at TEXT NOT NULL,
              PRIMARY KEY (campaign_id, organization_domain, to_email)
            );
            """
        )
        try:
            conn.execute("ALTER TABLE oauth_states ADD COLUMN redirect_uri TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE followup_drafts ADD COLUMN subject_text TEXT NOT NULL DEFAULT 'Quick reconnect'")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE campaign_targets ADD COLUMN subject_text TEXT NOT NULL DEFAULT 'Quick reconnect'")
        except sqlite3.OperationalError:
            pass


@app.on_event("startup")
def on_startup() -> None:
    init_db()


init_db()


@app.on_event("startup")
async def start_campaign_worker() -> None:
    global CAMPAIGN_WORKER_TASK
    if CAMPAIGN_WORKER_TASK and not CAMPAIGN_WORKER_TASK.done():
        return
    CAMPAIGN_WORKER_TASK = asyncio.create_task(campaign_worker_loop())


@app.on_event("shutdown")
async def stop_campaign_worker() -> None:
    global CAMPAIGN_WORKER_TASK
    if CAMPAIGN_WORKER_TASK and not CAMPAIGN_WORKER_TASK.done():
        CAMPAIGN_WORKER_TASK.cancel()
        try:
            await CAMPAIGN_WORKER_TASK
        except Exception:
            pass
    CAMPAIGN_WORKER_TASK = None


@app.get("/")
def index() -> FileResponse:
    return FileResponse(BASE_DIR / "static" / "index.html")


@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "time": now_iso()}


@app.get("/api/debug/oauth")
def debug_oauth() -> dict[str, Any]:
    cid = (GOOGLE_CLIENT_ID or "").strip()
    masked = ""
    if cid:
        if len(cid) <= 10:
            masked = f"{cid[:2]}***{cid[-2:]}"
        else:
            masked = f"{cid[:6]}...{cid[-6:]}"
    return {
        "ok": True,
        "google_client_id_masked": masked,
        "google_client_id_present": bool(cid),
        "app_base_url": APP_BASE_URL,
        "expected_callback_uri": f"{APP_BASE_URL}/api/auth/google/callback",
    }


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
        ds = conn.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN status='approved' THEN 1 ELSE 0 END) AS approved,
              SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END) AS rejected,
              SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending
            FROM followup_drafts WHERE user_email=?
            """,
            (user_email,),
        ).fetchone()
    return {
        "ok": True,
        "user_exists": bool(u),
        "name": (u["name"] if u else ""),
        "gmail_connected": bool(g),
        "gmail_connected_email": (g["connected_email"] if g else ""),
        "gmail_connected_matches_user": bool(g) and str(g["connected_email"] or "").strip().lower() == user_email,
        "gmail_expires_at": (g["expires_at"] if g else ""),
        "pipedrive_connected": bool(p),
        "pipedrive_domain": (p["domain"] if p else ""),
        "queue": {
            "total": int((qs["total"] if qs and qs["total"] is not None else 0) or 0),
            "approved": int((qs["approved"] if qs and qs["approved"] is not None else 0) or 0),
            "rejected": int((qs["rejected"] if qs and qs["rejected"] is not None else 0) or 0),
            "pending": int((qs["pending"] if qs and qs["pending"] is not None else 0) or 0),
        },
        "drafts": {
            "total": int((ds["total"] if ds and ds["total"] is not None else 0) or 0),
            "approved": int((ds["approved"] if ds and ds["approved"] is not None else 0) or 0),
            "rejected": int((ds["rejected"] if ds and ds["rejected"] is not None else 0) or 0),
            "pending": int((ds["pending"] if ds and ds["pending"] is not None else 0) or 0),
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
        "scope": "openid email profile https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/gmail.send",
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


def require_matching_gmail_connection(user_email: str) -> GmailConn:
    conn = load_gmail_connection(user_email)
    if not conn:
        raise HTTPException(status_code=400, detail="gmail_not_connected")
    expected = user_email.strip().lower()
    actual = str(conn.connected_email or "").strip().lower()
    if actual != expected:
        raise HTTPException(
            status_code=409,
            detail=f"gmail_connected_email_mismatch: connected={actual} expected={expected}",
        )
    return conn


def load_pipedrive_connection(user_email: str) -> Optional[dict[str, str]]:
    with db_conn() as conn:
        row = conn.execute(
            "SELECT domain, api_token FROM pipedrive_connections WHERE user_email=?",
            (user_email,),
        ).fetchone()
    if not row:
        return None
    return {"domain": str(row["domain"] or ""), "api_token": str(row["api_token"] or "")}


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
    parts = [p for p in re.split(r"[-_]+", root.lower()) if p]
    if not parts:
        return root

    suffixes = (
        "group",
        "holding",
        "holdings",
        "studio",
        "studios",
        "systems",
        "solutions",
        "digital",
        "labs",
        "lab",
        "tech",
        "software",
        "services",
        "consulting",
        "agency",
        "ventures",
        "media",
        "global",
    )

    def split_suffix_token(token: str) -> list[str]:
        for sfx in suffixes:
            if token.endswith(sfx) and len(token) > len(sfx) + 1:
                left = token[: -len(sfx)]
                # Acronym-like prefix + known company suffix (e.g. "bbcgroup" -> "BBC Group")
                if left.isalpha() and 2 <= len(left) <= 4:
                    return [left.upper(), sfx.title()]
                return [left, sfx]
        return [token]

    norm_parts: list[str] = []
    for p in parts:
        norm_parts.extend(split_suffix_token(p))

    out: list[str] = []
    for p in norm_parts:
        if p.isupper():
            out.append(p)
        elif p.isalpha() and len(p) <= 3:
            out.append(p.upper())
        else:
            out.append(p.title())
    return " ".join(out)


def is_noise_sender(email: str) -> bool:
    local = (email.split("@", 1)[0] if "@" in email else "").lower()
    return any(p.search(local) for p in AUTOMATED_SENDER_PATTERNS)


def is_generic_localpart(email: str) -> bool:
    local = (email.split("@", 1)[0] if "@" in email else "").lower().strip()
    return local in GENERIC_LOCALPARTS


def base_domain_label(domain: str) -> str:
    d = (domain or "").lower().strip()
    if not d:
        return ""
    return d.split(".", 1)[0]


def is_excluded_domain(domain: str, own_domain: str) -> bool:
    dom = (domain or "").lower().strip()
    if not dom:
        return True
    if own_domain and dom == own_domain:
        return True
    if own_domain and base_domain_label(dom) and base_domain_label(dom) == base_domain_label(own_domain):
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


def queue_job_key(user_email: str, job_id: str) -> str:
    return f"{user_email}:{job_id}"


def set_queue_job_state(key: str, **updates: Any) -> None:
    job = QUEUE_JOBS.get(key)
    if not job:
        return
    job.update(updates)


async def wait_if_queue_job_paused(job_key: Optional[str]) -> bool:
    if not job_key:
        return False
    while True:
        job = QUEUE_JOBS.get(job_key)
        if not job:
            return True
        if not bool(job.get("pause_requested")):
            return False
        set_queue_job_state(job_key, status="paused", message="Paused by user")
        await asyncio.sleep(0.4)


def load_followup_drafts(user_email: str) -> list[dict[str, Any]]:
    with db_conn() as conn:
        rows = conn.execute(
            """
            SELECT
              organization_domain, organization_name, to_email, primary_contact_name,
              context_summary, topics_json, subject_text, draft_text, final_text, status, updated_at
            FROM followup_drafts
            WHERE user_email=?
            ORDER BY
              CASE status WHEN 'pending' THEN 0 WHEN 'approved' THEN 1 ELSE 2 END,
              organization_name ASC,
              to_email ASC
            """,
            (user_email,),
        ).fetchall()

    out: list[dict[str, Any]] = []
    for r in rows:
        topics: list[str] = []
        try:
            parsed = json.loads(str(r["topics_json"] or "[]"))
            if isinstance(parsed, list):
                topics = [str(x) for x in parsed][:8]
        except Exception:
            topics = []
        out.append(
            {
                "organization_domain": str(r["organization_domain"] or ""),
                "organization": str(r["organization_name"] or ""),
                "to": str(r["to_email"] or ""),
                "primary_contact_name": str(r["primary_contact_name"] or ""),
                "summary": str(r["context_summary"] or ""),
                "topics": topics,
                "subject": str(r["subject_text"] or "Quick reconnect"),
                "draft": str(r["draft_text"] or ""),
                "final_text": str(r["final_text"] or ""),
                "status": str(r["status"] or "pending"),
                "updated_at": str(r["updated_at"] or ""),
            }
        )
    return out


def drafts_summary(drafts: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(drafts)
    pending = sum(1 for d in drafts if d.get("status") == "pending")
    approved = sum(1 for d in drafts if d.get("status") == "approved")
    rejected = sum(1 for d in drafts if d.get("status") == "rejected")
    return {
        "total": total,
        "pending": pending,
        "approved": approved,
        "rejected": rejected,
        "ready_to_send": total > 0 and approved > 0 and pending == 0,
    }


def campaign_subject(organization: str, send_index: int, token: str, initial_subject: str) -> str:
    if send_index <= 0:
        base = (initial_subject or "").strip() or "Quick reconnect"
        return base
    variants = [
        "Following up on my previous note",
        "Sharing one more idea for your team",
        "Checking if this is still relevant",
        "A different angle that might help",
        "Closing the loop for now",
    ]
    line = variants[(send_index - 1) % len(variants)]
    return f"{line} — {organization}"


def campaign_body(base_text: str, send_index: int) -> str:
    if send_index <= 0:
        return base_text.strip()
    prefixes = [
        "Quick follow-up in case my previous message got buried.",
        "Wanted to share another angle that could be useful.",
        "Checking one more time if this is relevant now.",
        "Last follow-up from my side unless timing changes.",
    ]
    prefix = prefixes[(send_index - 1) % len(prefixes)]
    return f"{prefix}\n\n{base_text.strip()}"


async def gmail_send_plain_message(access_token: str, to_email: str, subject: str, body: str) -> dict[str, Any]:
    msg = EmailMessage()
    msg["To"] = to_email
    msg["Subject"] = subject
    msg["Content-Type"] = "text/plain; charset=utf-8"
    msg.set_content(body)
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    async with httpx.AsyncClient(timeout=40) as client:
        res = await client.post(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json={"raw": raw},
        )
    if res.status_code >= 400:
        raise HTTPException(status_code=400, detail=f"gmail_send_failed: {res.text[:240]}")
    return res.json()


def gmail_query_date_from_iso(iso_ts: str) -> str:
    dt = parse_iso(iso_ts)
    return f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}"


async def gmail_has_reply_after(access_token: str, to_email: str, after_iso: str) -> bool:
    query = f'in:inbox from:{to_email} after:{gmail_query_date_from_iso(after_iso)}'
    async with httpx.AsyncClient(timeout=40) as client:
        res = await client.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"q": query, "maxResults": 5},
        )
    if res.status_code >= 400:
        return False
    body = res.json()
    msgs = body.get("messages") or []
    if not msgs:
        return False
    after_dt = parse_iso(after_iso)
    async with httpx.AsyncClient(timeout=40) as client:
        for m in msgs:
            mid = str((m or {}).get("id", "")).strip()
            if not mid:
                continue
            md = await client.get(
                f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{mid}",
                headers={"Authorization": f"Bearer {access_token}"},
                params={"format": "minimal"},
            )
            if md.status_code >= 400:
                continue
            try:
                ib = md.json()
                internal_ms = int(str(ib.get("internalDate", "0")) or "0")
                m_dt = datetime.fromtimestamp(internal_ms / 1000, tz=timezone.utc)
                if m_dt > after_dt:
                    return True
            except Exception:
                continue
    return False


async def pipedrive_create_deal_for_reply(
    user_email: str,
    organization_name: str,
    organization_domain: str,
    to_email: str,
) -> Optional[str]:
    pd = load_pipedrive_connection(user_email)
    if not pd or not pd.get("domain") or not pd.get("api_token"):
        return None
    domain = pd["domain"].strip()
    token = pd["api_token"].strip()
    title = f"Reply from {organization_name or organization_domain} ({to_email})"
    payload = {"title": title}
    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.post(
            f"https://{domain}.pipedrive.com/api/v1/deals",
            params={"api_token": token},
            json=payload,
        )
    if res.status_code >= 400:
        return None
    try:
        data = res.json().get("data") or {}
        deal_id = data.get("id")
        return str(deal_id) if deal_id is not None else None
    except Exception:
        return None


def campaign_status_for_user(user_email: str) -> dict[str, Any]:
    with db_conn() as conn:
        row = conn.execute(
            """
            SELECT campaign_id,status,followups_count,total_targets,sent_count,replied_count,deals_created,started_at,updated_at
            FROM campaigns
            WHERE user_email=?
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (user_email,),
        ).fetchone()
    if not row:
        return {"exists": False}
    return {
        "exists": True,
        "campaign_id": str(row["campaign_id"] or ""),
        "status": str(row["status"] or ""),
        "followups_count": int(row["followups_count"] or 0),
        "total_targets": int(row["total_targets"] or 0),
        "sent_count": int(row["sent_count"] or 0),
        "replied_count": int(row["replied_count"] or 0),
        "deals_created": int(row["deals_created"] or 0),
        "started_at": str(row["started_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


async def process_active_campaigns_once() -> None:
    with db_conn() as conn:
        campaigns = conn.execute(
            "SELECT campaign_id,user_email,followups_count,status FROM campaigns WHERE status='running'"
        ).fetchall()
    now = datetime.now(timezone.utc)
    now_s = now_iso()
    for c in campaigns:
        campaign_id = str(c["campaign_id"])
        user_email = str(c["user_email"]).strip().lower()
        try:
            gmail_conn = require_matching_gmail_connection(user_email)
        except HTTPException:
            with db_conn() as conn:
                conn.execute(
                    "UPDATE campaigns SET status='error', updated_at=? WHERE campaign_id=?",
                    (now_s, campaign_id),
                )
            continue
        try:
            access_token = await ensure_valid_access_token(gmail_conn)
        except Exception:
            continue
        with db_conn() as conn:
            targets = conn.execute(
                """
                SELECT organization_domain,organization_name,to_email,token,draft_text,sent_count,max_sends,next_send_at,
                       replied_at,pipedrive_deal_id,status,subject_text
                FROM campaign_targets
                WHERE campaign_id=? AND status='active'
                """,
                (campaign_id,),
            ).fetchall()
        sent_inc = 0
        replied_inc = 0
        deals_inc = 0
        for t in targets:
            org_domain = str(t["organization_domain"] or "")
            org_name = str(t["organization_name"] or org_domain)
            to_email = str(t["to_email"] or "").strip().lower()
            token = str(t["token"] or "")
            draft_text = str(t["draft_text"] or "")
            sent_count = int(t["sent_count"] or 0)
            max_sends = int(t["max_sends"] or 1)
            next_send_at = parse_iso(str(t["next_send_at"] or now_s))
            replied_at = str(t["replied_at"] or "")
            deal_id = str(t["pipedrive_deal_id"] or "")
            status = str(t["status"] or "active")
            subject_text = str(t["subject_text"] or "Quick reconnect")
            if status != "active":
                continue

            if not replied_at and sent_count > 0 and to_email and last_sent_at:
                has_reply = await gmail_has_reply_after(access_token, to_email, str(last_sent_at))
                if has_reply:
                    new_deal_id = deal_id or (await pipedrive_create_deal_for_reply(user_email, org_name, org_domain, to_email))
                    with db_conn() as conn:
                        conn.execute(
                            """
                            UPDATE campaign_targets
                            SET replied_at=?, status='replied', pipedrive_deal_id=?, updated_at=?
                            WHERE campaign_id=? AND organization_domain=? AND to_email=?
                            """,
                            (now_s, new_deal_id or deal_id, now_s, campaign_id, org_domain, to_email),
                        )
                    replied_inc += 1
                    if new_deal_id and not deal_id:
                        deals_inc += 1
                    continue

            if replied_at:
                continue
            if sent_count >= max_sends:
                with db_conn() as conn:
                    conn.execute(
                        """
                        UPDATE campaign_targets
                        SET status='completed', updated_at=?
                        WHERE campaign_id=? AND organization_domain=? AND to_email=?
                        """,
                        (now_s, campaign_id, org_domain, to_email),
                    )
                continue
            if next_send_at > now:
                continue

            subject = campaign_subject(org_name, sent_count, token, subject_text)
            body = campaign_body(draft_text, sent_count)
            try:
                await gmail_send_plain_message(access_token, to_email, subject, body)
            except Exception:
                continue

            next_send = (now + timedelta(days=2)).replace(microsecond=0).isoformat()
            with db_conn() as conn:
                conn.execute(
                    """
                    UPDATE campaign_targets
                    SET sent_count=sent_count+1, last_sent_at=?, next_send_at=?, updated_at=?
                    WHERE campaign_id=? AND organization_domain=? AND to_email=?
                    """,
                    (now_s, next_send, now_s, campaign_id, org_domain, to_email),
                )
            sent_inc += 1

        with db_conn() as conn:
            active_left = conn.execute(
                "SELECT COUNT(*) AS c FROM campaign_targets WHERE campaign_id=? AND status='active'",
                (campaign_id,),
            ).fetchone()
            running_status = "done" if int(active_left["c"] or 0) == 0 else "running"
            conn.execute(
                """
                UPDATE campaigns
                SET sent_count=sent_count+?, replied_count=replied_count+?, deals_created=deals_created+?,
                    status=?, updated_at=?
                WHERE campaign_id=?
                """,
                (sent_inc, replied_inc, deals_inc, running_status, now_s, campaign_id),
            )


async def campaign_worker_loop() -> None:
    while True:
        try:
            await process_active_campaigns_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(60)


def build_rows_from_orgs(orgs: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    now_dt = datetime.now(timezone.utc)
    for dom, org in orgs.items():
        threads = list(org["threads"].values())
        stakeholders = list(org["stakeholders"].values())
        if not threads or not stakeholders:
            continue
        if all(is_generic_localpart(str(s.get("email", ""))) for s in stakeholders):
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
        if any(is_generic_localpart(s["email"]) for s in stakeholders) and len(stakeholders) <= 1:
            auto_status = "auto_reject"
            reasons.append("generic_mailbox_only")
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
    return rows


async def fetch_gmail_message_ids(
    client: httpx.AsyncClient,
    access_token: str,
    query: str,
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    if limit is not None and limit <= 0:
        return []
    ids: list[dict[str, Any]] = []
    page_token: Optional[str] = None
    while True:
        chunk_size = 500 if limit is None else min(500, max(1, limit - len(ids)))
        params: dict[str, Any] = {"maxResults": chunk_size, "q": query}
        if page_token:
            params["pageToken"] = page_token
        list_res = await client.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages",
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if list_res.status_code >= 400:
            raise HTTPException(status_code=400, detail=f"gmail_list_failed: {list_res.text[:240]}")
        body = list_res.json()
        page_items = body.get("messages") or []
        ids.extend(page_items)
        if limit is not None and len(ids) >= limit:
            break
        page_token = body.get("nextPageToken")
        if not page_items or not page_token:
            break
    return ids if limit is None else ids[:limit]


async def fetch_gmail_message_metadata(
    client: httpx.AsyncClient,
    access_token: str,
    message_id: str,
) -> Optional[dict[str, Any]]:
    if not message_id:
        return None
    msg_res = await client.get(
        f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}",
        params={"format": "metadata", "metadataHeaders": ["From", "To", "Cc", "Subject"]},
        headers={"Authorization": f"Bearer {access_token}"},
    )
    if msg_res.status_code >= 400:
        return None
    return msg_res.json()


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


async def generate_queue_result(
    user_email: str,
    max_msgs: Optional[int] = None,
    job_key: Optional[str] = None,
) -> dict[str, Any]:
    try:
        gmail_conn = require_matching_gmail_connection(user_email)

        access_token = await ensure_valid_access_token(gmail_conn)
        if job_key:
            set_queue_job_state(
                job_key,
                status="running",
                message="Scanning mailbox range 2015 -> today...",
                result_summary={"organizations": 0, "messages_processed": 0, "messages_total": 0},
            )

        async with httpx.AsyncClient(timeout=40) as client:
            base_q = "-in:chats -category:promotions -category:social -category:updates after:2015/01/01"
            current_year = datetime.now(timezone.utc).year
            years = list(range(current_year, 2014, -1))
            dedup: dict[str, dict[str, Any]] = {}
            for year in years:
                should_exit = await wait_if_queue_job_paused(job_key)
                if should_exit:
                    raise HTTPException(status_code=404, detail="job_not_found")
                if job_key:
                    set_queue_job_state(job_key, status="running")
                if job_key:
                    set_queue_job_state(job_key, message=f"Scanning mailbox year {year}...")
                year_q = (
                    "-in:chats -category:promotions -category:social -category:updates "
                    f"after:{year}/01/01 before:{year + 1}/01/01"
                )
                year_ids = await fetch_gmail_message_ids(client, access_token, year_q, max_msgs)
                for it in year_ids:
                    mid = str(it.get("id", "")).strip()
                    if mid:
                        dedup[mid] = it
            ids = list(dedup.values())
            if max_msgs is not None and len(ids) < max_msgs:
                refill = await fetch_gmail_message_ids(client, access_token, base_q, max_msgs)
                for it in refill:
                    mid = str(it.get("id", "")).strip()
                    if mid and mid not in dedup:
                        ids.append(it)
                        dedup[mid] = it
                    if len(ids) >= max_msgs:
                        break

            own_domain = gmail_conn.connected_email.split("@")[-1] if "@" in gmail_conn.connected_email else ""
            orgs: dict[str, dict[str, Any]] = {}

            if max_msgs is not None:
                ids = ids[:max_msgs]
            concurrency = 16
            batch_size = 160
            sem = asyncio.Semaphore(concurrency)

            async def fetch_one(mid: str) -> Optional[dict[str, Any]]:
                async with sem:
                    return await fetch_gmail_message_metadata(client, access_token, mid)

            mids: list[str] = []
            for item in ids:
                mid = str(item.get("id", "")).strip()
                if mid:
                    mids.append(mid)

            for i in range(0, len(mids), batch_size):
                should_exit = await wait_if_queue_job_paused(job_key)
                if should_exit:
                    raise HTTPException(status_code=404, detail="job_not_found")
                if job_key:
                    set_queue_job_state(job_key, status="running")
                if job_key and mids:
                    pct = int((i / max(1, len(mids))) * 100)
                    set_queue_job_state(
                        job_key,
                        message=f"Reading Gmail messages... {pct}%",
                        result_summary={
                            "organizations": len(orgs),
                            "messages_processed": min(i, len(mids)),
                            "messages_total": len(mids),
                        },
                    )
                batch = mids[i : i + batch_size]
                fetched = await asyncio.gather(*(fetch_one(mid) for mid in batch), return_exceptions=True)
                for msg in fetched:
                    if not isinstance(msg, dict):
                        continue
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
                            if is_generic_localpart(em):
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

                # Persist partial queue periodically so UI can show results in portions.
                if (i // batch_size) % 4 == 0:
                    partial_rows = build_rows_from_orgs(orgs)
                    save_queue_rows(user_email, partial_rows)
                    if job_key:
                        set_queue_job_state(
                            job_key,
                            result_summary={
                                "organizations": len(partial_rows),
                                "messages_processed": min(i + len(batch), len(mids)),
                                "messages_total": len(mids),
                            },
                        )

        rows = build_rows_from_orgs(orgs)
        save_queue_rows(user_email, rows)
        saved_rows = load_queue_rows(user_email)
        out = {
            "ok": True,
            "summary": {
                "organizations": len(saved_rows),
                "messages_scanned": len(ids),
                "scan_range": "2015_to_today",
                "connected_email": gmail_conn.connected_email,
                "pending": sum(1 for r in saved_rows if r.get("status") == "pending"),
                "approved": sum(1 for r in saved_rows if r.get("status") == "approved"),
                "rejected": sum(1 for r in saved_rows if r.get("status") == "rejected"),
            },
            "rows": saved_rows,
        }
        if job_key:
            set_queue_job_state(
                job_key,
                status="done",
                message="Queue generated",
                finished_at=now_iso(),
                result_summary=out["summary"],
            )
        return out
    except HTTPException:
        if job_key:
            set_queue_job_state(job_key, status="failed", message="Generation failed", finished_at=now_iso())
        raise
    except Exception as exc:
        if job_key:
            set_queue_job_state(
                job_key,
                status="failed",
                message=f"Generation failed: {type(exc).__name__}",
                error=str(exc),
                finished_at=now_iso(),
            )
        raise HTTPException(status_code=500, detail=f"queue_generate_failed: {type(exc).__name__}: {exc}") from exc


async def run_generate_queue_job(job_key: str, user_email: str, max_msgs: Optional[int]) -> None:
    try:
        await generate_queue_result(user_email=user_email, max_msgs=max_msgs, job_key=job_key)
    finally:
        QUEUE_JOB_TASKS.pop(job_key, None)


@app.post("/api/queue/generate")
async def generate_queue(payload: QueuePayload) -> dict[str, Any]:
    user_email = str(payload.email).strip().lower()
    require_matching_gmail_connection(user_email)
    max_msgs: Optional[int] = None
    if payload.max_messages is not None:
        max_msgs = max(100, int(payload.max_messages))

    active = next(
        (
            key
            for key, job in QUEUE_JOBS.items()
            if job.get("user_email") == user_email and job.get("status") in {"running", "paused"}
        ),
        None,
    )
    if active:
        j = QUEUE_JOBS[active]
        return {
            "ok": True,
            "job_id": j["job_id"],
            "status": j["status"],
            "message": j.get("message", "Already running"),
        }

    job_id = secrets.token_urlsafe(8)
    key = queue_job_key(user_email, job_id)
    QUEUE_JOBS[key] = {
        "job_id": job_id,
        "user_email": user_email,
        "status": "running",
        "pause_requested": False,
        "message": "Starting...",
        "created_at": now_iso(),
        "finished_at": "",
        "error": "",
        "result_summary": {},
    }
    QUEUE_JOB_TASKS[key] = asyncio.create_task(run_generate_queue_job(key, user_email, max_msgs))
    return {"ok": True, "job_id": job_id, "status": "running", "message": "Started"}


@app.get("/api/queue/generate-status")
def generate_queue_status(email: str = Query(...), job_id: str = Query(...)) -> dict[str, Any]:
    user_email = email.strip().lower()
    key = queue_job_key(user_email, job_id)
    job = QUEUE_JOBS.get(key)
    if not job:
        raise HTTPException(status_code=404, detail="job_not_found")
    return {
        "ok": True,
        "job_id": job["job_id"],
        "status": job.get("status", "unknown"),
        "paused": bool(job.get("pause_requested", False)),
        "message": job.get("message", ""),
        "error": job.get("error", ""),
        "finished_at": job.get("finished_at", ""),
        "summary": job.get("result_summary", {}),
    }


@app.post("/api/queue/generate/pause")
def pause_queue_job(payload: QueueJobControlPayload) -> dict[str, Any]:
    user_email = str(payload.email).strip().lower()
    key = queue_job_key(user_email, payload.job_id.strip())
    job = QUEUE_JOBS.get(key)
    if not job:
        raise HTTPException(status_code=404, detail="job_not_found")
    if job.get("status") not in {"running", "paused"}:
        raise HTTPException(status_code=400, detail="job_not_running")
    set_queue_job_state(key, pause_requested=True, status="paused", message="Pause requested...")
    return {"ok": True, "job_id": job["job_id"], "status": "paused"}


@app.post("/api/queue/generate/resume")
def resume_queue_job(payload: QueueJobControlPayload) -> dict[str, Any]:
    user_email = str(payload.email).strip().lower()
    key = queue_job_key(user_email, payload.job_id.strip())
    job = QUEUE_JOBS.get(key)
    if not job:
        raise HTTPException(status_code=404, detail="job_not_found")
    if job.get("status") not in {"running", "paused"}:
        raise HTTPException(status_code=400, detail="job_not_running")
    set_queue_job_state(key, pause_requested=False, status="running", message="Resumed")
    return {"ok": True, "job_id": job["job_id"], "status": "running"}


@app.post("/api/drafts/generate")
def generate_drafts(payload: DraftPayload) -> dict[str, Any]:
    user_email = str(payload.email).strip().lower()
    require_matching_gmail_connection(user_email)
    with db_conn() as conn:
        user = conn.execute("SELECT name FROM users WHERE email=?", (user_email,)).fetchone()

    owner_name = (str(user["name"]).strip() if user and user["name"] else "Arseniy")
    rows = load_queue_rows(user_email)
    approved = [r for r in rows if r.get("status") == "approved"]

    generated: list[tuple[str, str]] = []
    ts = now_iso()
    with db_conn() as conn:
        for r in approved:
            org_domain = str(r.get("organization_domain", "")).strip().lower()
            to_email = str(r.get("primary_contact_email", "")).strip().lower()
            if not org_domain or not to_email:
                continue
            first_name = infer_first_name(str(r.get("primary_contact_name", "")), to_email)
            body = (
                f"Hi {first_name},\n\n"
                "It has been a while since we last spoke.\n"
                "Interested in your current priorities and whether new digital solutions could be relevant.\n\n"
                f"Best,\n{owner_name}"
            )
            topics_json = json.dumps((r.get("topics") or [])[:8], ensure_ascii=False)
            org_name = str(r.get("organization_name") or org_domain)
            summary = str(r.get("summary") or "")
            conn.execute(
                """
                INSERT INTO followup_drafts(
                  user_email, organization_domain, organization_name, to_email, primary_contact_name,
                  context_summary, topics_json, subject_text, draft_text, final_text, status, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(user_email, organization_domain, to_email) DO UPDATE SET
                  organization_name=excluded.organization_name,
                  primary_contact_name=excluded.primary_contact_name,
                  context_summary=excluded.context_summary,
                  topics_json=excluded.topics_json,
                  subject_text=CASE
                    WHEN followup_drafts.subject_text IS NULL OR followup_drafts.subject_text=''
                    THEN excluded.subject_text
                    ELSE followup_drafts.subject_text
                  END,
                  draft_text=excluded.draft_text,
                  final_text=CASE
                    WHEN followup_drafts.final_text IS NULL OR followup_drafts.final_text=''
                    THEN excluded.draft_text
                    ELSE followup_drafts.final_text
                  END,
                  status='pending',
                  updated_at=excluded.updated_at
                """,
                (
                    user_email,
                    org_domain,
                    org_name,
                    to_email,
                    str(r.get("primary_contact_name") or ""),
                    summary,
                    topics_json,
                    "Quick reconnect",
                    body,
                    body,
                    "pending",
                    ts,
                    ts,
                ),
            )
            generated.append((org_domain, to_email))

        existing = conn.execute(
            "SELECT organization_domain,to_email FROM followup_drafts WHERE user_email=?",
            (user_email,),
        ).fetchall()
        existing_set = {(str(x["organization_domain"]), str(x["to_email"])) for x in existing}
        generated_set = set(generated)
        for org_domain, to_email in existing_set - generated_set:
            conn.execute(
                "DELETE FROM followup_drafts WHERE user_email=? AND organization_domain=? AND to_email=?",
                (user_email, org_domain, to_email),
            )

    drafts = load_followup_drafts(user_email)
    summary = drafts_summary(drafts)
    return {
        "ok": True,
        "count": len(drafts),
        "owner_name": owner_name,
        "summary": summary,
        "drafts": drafts,
    }


@app.get("/api/drafts")
def list_drafts(email: str = Query(...)) -> dict[str, Any]:
    user_email = email.strip().lower()
    drafts = load_followup_drafts(user_email)
    return {"ok": True, "count": len(drafts), "summary": drafts_summary(drafts), "drafts": drafts}


@app.get("/api/drafts/readiness")
def drafts_readiness(email: str = Query(...)) -> dict[str, Any]:
    user_email = email.strip().lower()
    drafts = load_followup_drafts(user_email)
    return {"ok": True, "summary": drafts_summary(drafts)}


@app.post("/api/drafts/update")
def update_draft_text(payload: DraftUpdatePayload) -> dict[str, Any]:
    user_email = str(payload.email).strip().lower()
    org_domain = payload.organization_domain.strip().lower()
    to_email = str(payload.to).strip().lower()
    final_text = payload.final_text.strip()
    subject_text: Optional[str] = payload.subject_text
    if subject_text is not None:
        subject_text = subject_text.strip() or "Quick reconnect"
    with db_conn() as conn:
        cur = conn.execute(
            """
            UPDATE followup_drafts
            SET final_text=?, subject_text=COALESCE(?, subject_text), updated_at=?
            WHERE user_email=? AND organization_domain=? AND to_email=?
            """,
            (final_text, subject_text, now_iso(), user_email, org_domain, to_email),
        )
    if cur.rowcount < 1:
        raise HTTPException(status_code=404, detail="draft_not_found")
    return {"ok": True}


@app.post("/api/drafts/decision")
def update_draft_decision(payload: DraftDecisionPayload) -> dict[str, Any]:
    user_email = str(payload.email).strip().lower()
    org_domain = payload.organization_domain.strip().lower()
    to_email = str(payload.to).strip().lower()
    status = payload.status.strip().lower()
    if status not in {"pending", "approved", "rejected"}:
        raise HTTPException(status_code=400, detail="invalid_status")
    with db_conn() as conn:
        cur = conn.execute(
            """
            UPDATE followup_drafts
            SET status=?, updated_at=?
            WHERE user_email=? AND organization_domain=? AND to_email=?
            """,
            (status, now_iso(), user_email, org_domain, to_email),
        )
    if cur.rowcount < 1:
        raise HTTPException(status_code=404, detail="draft_not_found")
    return {"ok": True, "status": status}


@app.post("/api/campaign/start")
def start_campaign(payload: CampaignStartPayload) -> dict[str, Any]:
    user_email = str(payload.email).strip().lower()
    require_matching_gmail_connection(user_email)
    followups_count = int(payload.followups_count)
    if followups_count not in {3, 5}:
        raise HTTPException(status_code=400, detail="followups_count_must_be_3_or_5")

    drafts = load_followup_drafts(user_email)
    approved = [d for d in drafts if d.get("status") == "approved" and str(d.get("to") or "").strip()]
    if not approved:
        raise HTTPException(status_code=400, detail="no_approved_drafts")

    with db_conn() as conn:
        existing = conn.execute(
            "SELECT campaign_id FROM campaigns WHERE user_email=? AND status='running' ORDER BY started_at DESC LIMIT 1",
            (user_email,),
        ).fetchone()
        if existing:
            return {"ok": True, "campaign_id": str(existing["campaign_id"]), "already_running": True}

    campaign_id = secrets.token_urlsafe(10)
    ts = now_iso()
    with db_conn() as conn:
        conn.execute(
            """
            INSERT INTO campaigns(
              campaign_id,user_email,status,followups_count,total_targets,sent_count,replied_count,deals_created,started_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (campaign_id, user_email, "running", followups_count, len(approved), 0, 0, 0, ts, ts),
        )
        for d in approved:
            org_domain = str(d.get("organization_domain") or "").strip().lower()
            org_name = str(d.get("organization") or org_domain)
            to_email = str(d.get("to") or "").strip().lower()
            token = secrets.token_hex(4).upper()
            final_text = str(d.get("final_text") or d.get("draft") or "").strip()
            subject_text = str(d.get("subject") or "Quick reconnect").strip() or "Quick reconnect"
            if not org_domain or not to_email or not final_text:
                continue
            conn.execute(
                """
                INSERT INTO campaign_targets(
                  campaign_id,user_email,organization_domain,organization_name,to_email,token,subject_text,draft_text,
                  sent_count,max_sends,next_send_at,status,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    campaign_id,
                    user_email,
                    org_domain,
                    org_name,
                    to_email,
                    token,
                    subject_text,
                    final_text,
                    0,
                    1 + followups_count,
                    ts,
                    "active",
                    ts,
                ),
            )

    return {"ok": True, "campaign_id": campaign_id, "targets": len(approved)}


@app.get("/api/campaign/status")
def get_campaign_status(email: str = Query(...)) -> dict[str, Any]:
    user_email = email.strip().lower()
    status = campaign_status_for_user(user_email)
    if not status.get("exists"):
        return {"ok": True, "campaign": status, "targets": []}

    with db_conn() as conn:
        targets = conn.execute(
            """
            SELECT organization_domain,organization_name,to_email,sent_count,max_sends,last_sent_at,next_send_at,replied_at,status,pipedrive_deal_id
            FROM campaign_targets
            WHERE campaign_id=?
            ORDER BY
              CASE status WHEN 'active' THEN 0 WHEN 'replied' THEN 1 ELSE 2 END,
              organization_name ASC
            """,
            (status["campaign_id"],),
        ).fetchall()
    return {
        "ok": True,
        "campaign": status,
        "targets": [
            {
                "organization_domain": str(t["organization_domain"] or ""),
                "organization_name": str(t["organization_name"] or ""),
                "to": str(t["to_email"] or ""),
                "sent_count": int(t["sent_count"] or 0),
                "max_sends": int(t["max_sends"] or 0),
                "last_sent_at": str(t["last_sent_at"] or ""),
                "next_send_at": str(t["next_send_at"] or ""),
                "replied_at": str(t["replied_at"] or ""),
                "status": str(t["status"] or ""),
                "pipedrive_deal_id": str(t["pipedrive_deal_id"] or ""),
            }
            for t in targets
        ],
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
