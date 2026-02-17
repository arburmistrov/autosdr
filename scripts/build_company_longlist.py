#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.common import read_csv, write_csv, to_int, clamp_0_100


def normalize_segment(industry):
    x = (industry or "").strip().lower()
    if any(k in x for k in ["bank", "insur", "financ"]):
        return "FSI"
    if any(k in x for k in ["pharma", "med", "health", "biotech"]):
        return "Pharma/MedTech"
    if any(k in x for k in ["manufact", "industr", "engineering", "logistics"]):
        return "Industrial"
    if any(k in x for k in ["retail", "consumer", "hospitality", "travel", "service"]):
        return "Retail/Services"
    if any(k in x for k in ["software", "saas", "technology", "tech"]):
        return "Tech-enabled services"
    return "Other"


def score_company(row):
    fit = to_int(row.get("fit_icp_score"), 0)
    ai = to_int(row.get("ai_signal_score"), 0)
    reach = to_int(row.get("reachability_score"), 0)
    total = clamp_0_100(fit + ai + reach)
    return fit, ai, reach, total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--top30", required=True)
    args = ap.parse_args()

    rows = read_csv(args.input)
    scored = []

    for row in rows:
        fit, ai, reach, total = score_company(row)
        scored.append({
            "company_name": row.get("company_name", "").strip(),
            "domain": row.get("domain", "").strip(),
            "ch_presence_type": (row.get("ch_presence_type") or "major_office").strip(),
            "city_canton": row.get("city_canton", "").strip(),
            "industry": row.get("industry", "").strip(),
            "icp_segment": normalize_segment(row.get("industry", "")),
            "employee_band": row.get("employee_band", "").strip(),
            "ai_signal_1": row.get("ai_signal_1", "").strip(),
            "ai_signal_2": row.get("ai_signal_2", "").strip(),
            "fit_icp_score": fit,
            "ai_signal_score": ai,
            "reachability_score": reach,
            "outreach_score": total,
        })

    scored.sort(key=lambda r: r["outreach_score"], reverse=True)
    fieldnames = list(scored[0].keys()) if scored else [
        "company_name", "domain", "ch_presence_type", "city_canton", "industry",
        "icp_segment", "employee_band", "ai_signal_1", "ai_signal_2", "fit_icp_score",
        "ai_signal_score", "reachability_score", "outreach_score"
    ]
    write_csv(args.output, scored, fieldnames)
    write_csv(args.top30, scored[:30], fieldnames)
    print(f"Scored {len(scored)} companies; top30 written.")


if __name__ == "__main__":
    main()
