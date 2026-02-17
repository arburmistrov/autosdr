#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.common import write_csv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", required=True)
    ap.add_argument("--count", type=int, default=100)
    args = ap.parse_args()

    rows = []
    for i in range(1, args.count + 1):
        rows.append({
            "company_name": "",
            "domain": "",
            "ch_presence_type": "major_office",
            "city_canton": "",
            "industry": "",
            "employee_band": "",
            "ai_signal_1": "",
            "ai_signal_2": "",
            "fit_icp_score": "",
            "ai_signal_score": "",
            "reachability_score": "",
            "row_id": i,
        })

    fieldnames = list(rows[0].keys())
    write_csv(args.output, rows, fieldnames)
    print(f"Created scaffold with {len(rows)} company rows: {args.output}")


if __name__ == "__main__":
    main()
