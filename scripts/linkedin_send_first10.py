#!/usr/bin/env python3
import argparse
import csv
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout


def read_contacts(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    valid = [r for r in rows if (r.get("linkedin_url") or "").strip()]
    return valid


def read_messages(pack_path):
    if not pack_path:
        return {}
    with open(pack_path, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    m = {}
    for r in rows:
        k = (r.get("linkedin_url") or "").strip()
        if not k:
            continue
        msg = (r.get("linkedin_connection_note") or "").strip() or (r.get("linkedin_followup_1") or "").strip()
        if msg:
            m[k] = msg
    return m


def default_message(row):
    name = (row.get("full_name") or "there").split(" ")[0]
    company = row.get("company_name", "your team")
    hypothesis = row.get("ai_use_case_hypothesis", "a practical GenAI pilot")
    return (
        f"Hi {name}, sharing a quick idea for {company}: {hypothesis}. "
        "We run a focused executive workshop to validate GenAI use cases with clear next steps. "
        "Open to a short fit-check?"
    )


def wait_for_login(page, timeout_sec=420):
    page.goto("https://www.linkedin.com/login", wait_until="domcontentloaded")
    print("ACTION REQUIRED: log into LinkedIn in the opened browser window.")
    print("Waiting for successful login...")
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=15000)
            if "feed" in page.url:
                print("Login detected.")
                return True
        except Exception:
            pass
        time.sleep(3)
    return False


def try_click_message(page):
    candidates = [
        "button:has-text('Message')",
        "button:has-text('InMail')",
        "a:has-text('Message')",
        "a:has-text('InMail')",
    ]
    for sel in candidates:
        loc = page.locator(sel)
        if loc.count() > 0:
            try:
                loc.first.click(timeout=4000)
                return True
            except Exception:
                continue
    return False


def try_fill_and_send(page, message):
    editors = [
        "div.msg-form__contenteditable[contenteditable='true']",
        "div[role='textbox'][contenteditable='true']",
    ]
    for sel in editors:
        loc = page.locator(sel)
        if loc.count() > 0:
            try:
                box = loc.first
                box.click(timeout=5000)
                box.fill(message)
                break
            except Exception:
                continue
    else:
        return False, "message_box_not_found"

    send_selectors = [
        "button.msg-form__send-button",
        "button:has-text('Send')",
    ]
    for sel in send_selectors:
        btn = page.locator(sel)
        if btn.count() > 0:
            try:
                btn.first.click(timeout=5000)
                return True, "sent"
            except Exception:
                continue
    return False, "send_button_not_found"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--contacts", required=True, help="CSV with linkedin_url/full_name/company_name")
    ap.add_argument("--review-pack", default="", help="Optional CSV with prepared LinkedIn messages")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--profile-dir", default=".pw-linkedin")
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--results", default="data/output/linkedin_send_results.csv")
    args = ap.parse_args()

    contacts = read_contacts(args.contacts)
    if len(contacts) < args.limit:
        raise SystemExit(f"Need at least {args.limit} contacts with linkedin_url. Found: {len(contacts)}")

    msg_map = read_messages(args.review_pack)
    batch = contacts[: args.limit]

    results = []

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=args.profile_dir,
            headless=args.headless,
            viewport={"width": 1440, "height": 960},
        )
        page = context.new_page()

        if not wait_for_login(page):
            context.close()
            raise SystemExit("LinkedIn login was not completed within timeout.")

        for i, row in enumerate(batch, start=1):
            url = (row.get("linkedin_url") or "").strip()
            name = row.get("full_name", "")
            company = row.get("company_name", "")
            message = msg_map.get(url) or default_message(row)
            status = "failed"
            detail = "unknown"
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(2000)

                if not try_click_message(page):
                    status = "failed"
                    detail = "message_button_not_found"
                else:
                    page.wait_for_timeout(1500)
                    ok, detail = try_fill_and_send(page, message)
                    status = "sent" if ok else "failed"
            except PWTimeout:
                status = "failed"
                detail = "timeout"
            except Exception as e:
                status = "failed"
                detail = f"error:{type(e).__name__}"

            print(f"[{i}/{args.limit}] {name} @ {company}: {status} ({detail})")
            results.append(
                {
                    "idx": i,
                    "full_name": name,
                    "company_name": company,
                    "linkedin_url": url,
                    "status": status,
                    "detail": detail,
                }
            )
            page.wait_for_timeout(1200)

        out = Path(args.results)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(results[0].keys()) if results else ["idx", "full_name", "company_name", "linkedin_url", "status", "detail"])
            w.writeheader()
            w.writerows(results)

        context.close()

    sent = sum(1 for r in results if r["status"] == "sent")
    print(f"DONE. Sent: {sent}/{len(results)}. Results: {args.results}")


if __name__ == "__main__":
    sys.exit(main())
