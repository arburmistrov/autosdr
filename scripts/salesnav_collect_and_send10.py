#!/usr/bin/env python3
import argparse
import csv
import re
import time
from pathlib import Path
from playwright.sync_api import sync_playwright


def wait_login(page, timeout_sec=600):
    page.goto("https://www.linkedin.com/login", wait_until="domcontentloaded")
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            page.goto("https://www.linkedin.com/sales/", wait_until="domcontentloaded", timeout=20000)
            if "linkedin.com/sales" in page.url and "login" not in page.url:
                return True
        except Exception:
            pass
        time.sleep(3)
    return False


def collect_lead_links(page, needed=10, timeout_sec=420):
    start = time.time()
    links = []
    seen = set()
    tick = 0

    while time.time() - start < timeout_sec:
        tick += 1
        hrefs = []
        for sel in [
            "a[href*='/sales/lead/']",
            "a[data-control-name*='view_lead']",
            "a[href*='linkedin.com/sales/lead/']",
        ]:
            try:
                part = page.eval_on_selector_all(
                    sel,
                    "els => els.map(e => e.href || e.getAttribute('href')).filter(Boolean)",
                )
                hrefs.extend(part)
            except Exception:
                continue
        for h in hrefs:
            if h.startswith("/"):
                h = "https://www.linkedin.com" + h
            h = h.split("?")[0]
            if h not in seen:
                seen.add(h)
                links.append(h)
        if tick % 3 == 0:
            print(f"Lead scan tick={tick}, found_unique={len(links)}, current_url={page.url}")
        if len(links) >= needed:
            return links[:needed]
        page.mouse.wheel(0, 2200)
        page.wait_for_timeout(1200)
    return links[:needed]


def extract_name(page):
    sels = ["h1", "[data-anonymize='person-name']", ".profile-topcard__name"]
    for s in sels:
        loc = page.locator(s)
        if loc.count() > 0:
            t = loc.first.inner_text().strip()
            if t:
                return re.sub(r"\s+", " ", t)
    return ""


def extract_company(page):
    sels = ["[data-anonymize='company-name']", ".profile-topcard__current-company", "a[href*='/sales/company/']"]
    for s in sels:
        loc = page.locator(s)
        if loc.count() > 0:
            t = loc.first.inner_text().strip()
            if t:
                return re.sub(r"\s+", " ", t)
    return ""


def short_first(name):
    return (name or "there").split(" ")[0]


def build_message(name, company):
    first = short_first(name)
    comp = company or "your team"
    return (
        f"Hi {first}, quick idea for {comp}: we run a focused executive workshop "
        "to move GenAI initiatives from strategy to validated prototype with clear governance and next steps. "
        "Open to a short fit-check this or next week?"
    )


def click_first(page, selectors):
    for s in selectors:
        loc = page.locator(s)
        if loc.count() > 0:
            try:
                loc.first.click(timeout=5000)
                return True
            except Exception:
                continue
    return False


def send_message_on_lead(page, msg):
    opened = click_first(
        page,
        [
            "button:has-text('Message')",
            "button:has-text('InMail')",
            "a:has-text('Message')",
            "a:has-text('InMail')",
        ],
    )
    if not opened:
        return False, "message_button_not_found"

    page.wait_for_timeout(1200)
    editor_ok = click_first(
        page,
        [
            "div[role='textbox'][contenteditable='true']",
            "div.msg-form__contenteditable[contenteditable='true']",
            "textarea",
        ],
    )
    if not editor_ok:
        return False, "editor_not_found"

    # Fill editor
    filled = False
    for s in [
        "div[role='textbox'][contenteditable='true']",
        "div.msg-form__contenteditable[contenteditable='true']",
        "textarea",
    ]:
        loc = page.locator(s)
        if loc.count() > 0:
            try:
                loc.first.fill(msg)
                filled = True
                break
            except Exception:
                try:
                    loc.first.click()
                    page.keyboard.type(msg, delay=8)
                    filled = True
                    break
                except Exception:
                    continue

    if not filled:
        return False, "editor_fill_failed"

    sent = click_first(
        page,
        [
            "button:has-text('Send')",
            "button.msg-form__send-button",
            "button.artdeco-button--primary:has-text('Send')",
        ],
    )
    if not sent:
        return False, "send_button_not_found"

    page.wait_for_timeout(1200)
    return True, "sent"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--profile-dir", default=".pw-linkedin")
    ap.add_argument("--results", default="data/output/salesnav_send_results.csv")
    args = ap.parse_args()

    out_rows = []

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=args.profile_dir,
            headless=False,
            viewport={"width": 1480, "height": 960},
        )
        page = ctx.new_page()

        print("ACTION: Please log in to LinkedIn/Sales Navigator in the opened browser.")
        if not wait_login(page):
            print("ERROR: login timeout")
            ctx.close()
            return 1

        page.goto("https://www.linkedin.com/sales/search/people", wait_until="domcontentloaded")
        print("ACTION: In browser, apply filters for Swiss enterprise and open results list. Waiting for lead cards...")

        leads = collect_lead_links(page, needed=args.limit)
        print(f"Found leads: {len(leads)}")
        if len(leads) < args.limit:
            print("ERROR: fewer than requested leads found on results page")

        for i, lead_url in enumerate(leads[: args.limit], start=1):
            status, detail = "failed", "unknown"
            name, company = "", ""
            try:
                page.goto(lead_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(1600)
                name = extract_name(page)
                company = extract_company(page)
                msg = build_message(name, company)
                ok, detail = send_message_on_lead(page, msg)
                status = "sent" if ok else "failed"
            except Exception as e:
                detail = f"error:{type(e).__name__}"

            print(f"[{i}/{args.limit}] {name or 'Unknown'} | {company or 'Unknown'} -> {status} ({detail})")
            out_rows.append(
                {
                    "idx": i,
                    "lead_url": lead_url,
                    "full_name": name,
                    "company_name": company,
                    "status": status,
                    "detail": detail,
                }
            )
            page.wait_for_timeout(1200)

        out = Path(args.results)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["idx", "lead_url", "full_name", "company_name", "status", "detail"])
            w.writeheader()
            w.writerows(out_rows)

        sent = sum(1 for r in out_rows if r["status"] == "sent")
        print(f"DONE: sent {sent}/{len(out_rows)}. Results at {args.results}")
        ctx.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
