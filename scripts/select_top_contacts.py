#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.common import read_csv, write_csv, to_int, clamp_0_100


TARGET_ROLE_KEYWORDS = ["director", "vp", "chief", "cdo", "cto", "cio", "head"]


def is_target_role(title):
    t = (title or "").lower()
    return any(k in t for k in TARGET_ROLE_KEYWORDS)


def role_score(title):
    t = (title or "").lower()
    if "chief" in t or "cto" in t or "cio" in t or "cdo" in t:
        return 25
    if "vp" in t:
        return 22
    if "director" in t:
        return 20
    if "head" in t:
        return 18
    return 0


def language_pick(signal):
    x = (signal or "").upper()
    if x in {"DE", "FR", "EN"}:
        return x
    return "EN"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--companies", required=True)
    ap.add_argument("--contacts", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--first10", required=True)
    args = ap.parse_args()

    companies = read_csv(args.companies)
    contacts = read_csv(args.contacts)

    company_map = {c["company_name"].strip().lower(): c for c in companies if c.get("company_name")}

    candidates = []
    for c in contacts:
        cname = c.get("company_name", "").strip()
        if cname.lower() not in company_map:
            continue
        if not is_target_role(c.get("title", "")):
            continue

        company = company_map[cname.lower()]
        company_score = to_int(company.get("outreach_score"), 0)
        rs = role_score(c.get("title", ""))
        email_conf = (c.get("email_confidence") or "").lower().strip()
        reach = 15 if email_conf == "high" else 10 if email_conf == "medium" else 5
        total = clamp_0_100(int(company_score * 0.6) + rs + reach)

        candidates.append({
            "company_name": cname,
            "domain": company.get("domain", ""),
            "icp_segment": company.get("icp_segment", "Other"),
            "full_name": c.get("full_name", "").strip(),
            "title": c.get("title", "").strip(),
            "linkedin_url": c.get("linkedin_url", "").strip(),
            "email": c.get("email", "").strip(),
            "outreach_language": language_pick(c.get("language_signal", "")),
            "why_fit": c.get("why_fit", "").strip(),
            "ai_use_case_hypothesis": c.get("use_case_hypothesis", "").strip(),
            "company_score": company_score,
            "role_seniority_score": rs,
            "reachability_score": reach,
            "outreach_score": total,
        })

    candidates.sort(key=lambda r: r["outreach_score"], reverse=True)

    # Keep max 2 contacts per company for diversified first-10
    by_company = {}
    diversified = []
    for row in candidates:
        k = row["company_name"].lower()
        by_company.setdefault(k, 0)
        if by_company[k] >= 2:
            continue
        diversified.append(row)
        by_company[k] += 1

    fieldnames = list(diversified[0].keys()) if diversified else [
        "company_name", "domain", "icp_segment", "full_name", "title", "linkedin_url", "email",
        "outreach_language", "why_fit", "ai_use_case_hypothesis", "company_score", "role_seniority_score",
        "reachability_score", "outreach_score"
    ]
    write_csv(args.output, diversified, fieldnames)
    write_csv(args.first10, diversified[:10], fieldnames)
    print(f"Selected {len(diversified)} contact candidates; first 10 prepared.")


if __name__ == "__main__":
    main()
