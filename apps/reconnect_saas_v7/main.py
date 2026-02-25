#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
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
}
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


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
    max_messages: int = 150


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
            """
        )
        # Backward compatibility for existing sqlite DB created before redirect_uri column.
        try:
            conn.execute("ALTER TABLE oauth_states ADD COLUMN redirect_uri TEXT")
        except sqlite3.OperationalError:
            pass


@app.on_event("startup")
def on_startup() -> None:
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
    return {
        "ok": True,
        "user_exists": bool(u),
        "name": (u["name"] if u else ""),
        "gmail_connected": bool(g),
        "gmail_connected_email": (g["connected_email"] if g else ""),
        "gmail_expires_at": (g["expires_at"] if g else ""),
        "pipedrive_connected": bool(p),
        "pipedrive_domain": (p["domain"] if p else ""),
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
    url = f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"
    return RedirectResponse(url)


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
        raise HTTPException(status_code=400, detail=f"token_exchange_failed: {res.text[:300]}")
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
        raise HTTPException(status_code=400, detail=f"token_refresh_failed: {res.text[:300]}")
    return res.json()


async def gmail_profile(access_token: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/profile",
            headers={"Authorization": f"Bearer {access_token}"},
        )
    if res.status_code >= 400:
        raise HTTPException(status_code=400, detail=f"gmail_profile_failed: {res.text[:300]}")
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
    prof = await gmail_profile(access_token)
    connected_email = str(prof.get("emailAddress", user_email)).strip().lower()
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
    ts = now_iso()
    with db_conn() as db:
        db.execute(
            "UPDATE gmail_connections SET access_token=?, expires_at=?, updated_at=? WHERE user_email=?",
            (new_access, expires_at_new, ts, conn.user_email),
        )
    return new_access


def extract_email(header_value: str) -> str:
    m = re.search(r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})", header_value or "")
    return m.group(1).lower() if m else ""


@app.post("/api/queue/generate")
async def generate_queue(payload: QueuePayload) -> dict[str, Any]:
    user_email = str(payload.email).strip().lower()
    conn = load_gmail_connection(user_email)
    if not conn:
        raise HTTPException(status_code=400, detail="gmail_not_connected")

    access_token = await ensure_valid_access_token(conn)
    max_msgs = max(20, min(250, int(payload.max_messages)))

    async with httpx.AsyncClient(timeout=30) as client:
        list_res = await client.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages",
            params={"maxResults": max_msgs, "q": "-in:chats"},
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if list_res.status_code >= 400:
            raise HTTPException(status_code=400, detail=f"gmail_list_failed: {list_res.text[:300]}")
        ids = (list_res.json().get("messages") or [])

        own_domain = conn.connected_email.split("@")[-1] if "@" in conn.connected_email else ""
        orgs: dict[str, dict[str, Any]] = {}

        for item in ids:
            mid = item.get("id")
            if not mid:
                continue
            msg_res = await client.get(
                f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{mid}",
                params={
                    "format": "metadata",
                    "metadataHeaders": ["From", "Date", "Subject"],
                },
                headers={"Authorization": f"Bearer {access_token}"},
            )
            if msg_res.status_code >= 400:
                continue
            j = msg_res.json()
            headers = {str(h.get("name", "")).lower(): h.get("value", "") for h in (j.get("payload", {}).get("headers") or [])}
            from_email = extract_email(headers.get("from", ""))
            if not from_email or "@" not in from_email:
                continue
            dom = from_email.split("@")[-1]
            if dom == own_domain or dom in FREE_DOMAINS:
                continue

            ts = ""
            try:
                ts = datetime.fromtimestamp(int(j.get("internalDate", "0")) / 1000, tz=timezone.utc).replace(microsecond=0).isoformat()
            except Exception:
                pass

            row = orgs.get(dom) or {
                "organization_domain": dom,
                "organization_name": dom.split(".")[0].title(),
                "primary_contact_email": from_email,
                "threads_count": 0,
                "last_message_at": "",
                "status": "pending",
            }
            row["threads_count"] += 1
            if ts and (not row["last_message_at"] or ts > row["last_message_at"]):
                row["last_message_at"] = ts
                row["primary_contact_email"] = from_email
            orgs[dom] = row

    rows = sorted(orgs.values(), key=lambda r: (r.get("last_message_at", ""), int(r.get("threads_count", 0))), reverse=True)
    return {
        "ok": True,
        "summary": {
            "organizations": len(rows),
            "messages_scanned": len(ids),
            "connected_email": conn.connected_email,
        },
        "rows": rows[:200],
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
