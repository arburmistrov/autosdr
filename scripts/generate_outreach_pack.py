#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pathlib import Path
from scripts.common import read_csv, write_csv

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_DIR = ROOT / "templates"


def load_template(kind, lang):
    lang = (lang or "EN").upper()
    p = TEMPLATE_DIR / f"{kind}_{lang.lower()}.txt"
    if not p.exists():
        p = TEMPLATE_DIR / f"{kind}_en.txt"
    return p.read_text(encoding="utf-8")


def short_text(s, n=60):
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 1].rstrip() + "..."


def first_name(full_name):
    return (full_name or "").strip().split(" ")[0] if full_name else "there"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--contacts", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--sender-name", default="S-PRO Team")
    args = ap.parse_args()

    contacts = read_csv(args.contacts)
    rows = []

    for c in contacts:
        lang = (c.get("outreach_language") or "EN").upper()
        ctx = {
            "first_name": first_name(c.get("full_name")),
            "company_name": c.get("company_name", ""),
            "why_fit": c.get("why_fit", "your transformation agenda"),
            "why_fit_short": short_text(c.get("why_fit", "digital transformation"), 45),
            "ai_use_case_hypothesis": c.get("ai_use_case_hypothesis", "a practical GenAI pilot"),
            "sender_name": args.sender_name,
        }

        li_note = load_template("linkedin_connection", lang).format(**ctx)
        li_follow = load_template("linkedin_followup", lang).format(**ctx)
        email_1 = load_template("email_1", lang).format(**ctx)

        rows.append({
            "company_name": c.get("company_name", ""),
            "full_name": c.get("full_name", ""),
            "title": c.get("title", ""),
            "domain": c.get("domain", ""),
            "linkedin_url": c.get("linkedin_url", ""),
            "email": c.get("email", ""),
            "outreach_language": lang,
            "why_fit": c.get("why_fit", ""),
            "ai_use_case_hypothesis": c.get("ai_use_case_hypothesis", ""),
            "outreach_score": c.get("outreach_score", ""),
            "linkedin_connection_note": li_note,
            "linkedin_followup_1": li_follow,
            "email_1": email_1,
        })

    fieldnames = list(rows[0].keys()) if rows else [
        "company_name", "full_name", "title", "domain", "linkedin_url", "email",
        "outreach_language", "why_fit", "ai_use_case_hypothesis", "outreach_score",
        "linkedin_connection_note", "linkedin_followup_1", "email_1"
    ]
    write_csv(args.output, rows, fieldnames)
    print(f"Generated review pack for {len(rows)} contacts.")


if __name__ == "__main__":
    main()
