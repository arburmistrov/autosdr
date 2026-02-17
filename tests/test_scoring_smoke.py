import csv
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_end_to_end_smoke(tmp_path):
    companies = tmp_path / "companies.csv"
    contacts = tmp_path / "contacts.csv"
    out_long = tmp_path / "long.csv"
    out_top = tmp_path / "top30.csv"
    out_candidates = tmp_path / "candidates.csv"
    out_first10 = tmp_path / "first10.csv"
    out_pack = tmp_path / "pack.csv"

    with companies.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "company_name", "domain", "ch_presence_type", "city_canton", "industry", "employee_band",
            "ai_signal_1", "ai_signal_2", "fit_icp_score", "ai_signal_score", "reachability_score"
        ])
        for i in range(1, 36):
            w.writerow([
                f"Company {i}", f"c{i}.ch", "major_office", "Zurich", "Banking", "1000+", "AI", "DX", 35, 18, 12
            ])

    with contacts.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "company_name", "full_name", "title", "linkedin_url", "email", "email_confidence", "language_signal", "why_fit", "use_case_hypothesis"
        ])
        for i in range(1, 36):
            w.writerow([
                f"Company {i}", f"Alex {i}", "Director of Innovation", f"https://linkedin.com/in/a{i}",
                f"a{i}@c{i}.ch", "high", "EN", "innovation roadmap", "knowledge assistant"
            ])

    subprocess.check_call(["python3", str(ROOT / "scripts/build_company_longlist.py"), "--input", str(companies), "--output", str(out_long), "--top30", str(out_top)], cwd=ROOT)
    subprocess.check_call(["python3", str(ROOT / "scripts/select_top_contacts.py"), "--companies", str(out_top), "--contacts", str(contacts), "--output", str(out_candidates), "--first10", str(out_first10)], cwd=ROOT)
    subprocess.check_call(["python3", str(ROOT / "scripts/generate_outreach_pack.py"), "--contacts", str(out_first10), "--output", str(out_pack)], cwd=ROOT)

    with out_first10.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 10

    with out_pack.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 10
    assert all(r["linkedin_connection_note"] for r in rows)
    assert all(r["email_1"] for r in rows)
