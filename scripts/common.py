import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCORING_PATH = ROOT / "config" / "scoring.json"


def load_scoring_config():
    with SCORING_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def read_csv(path):
    with Path(path).open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fieldnames):
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def to_int(value, default=0):
    try:
        return int(float(str(value).strip()))
    except Exception:
        return default


def clamp_0_100(value):
    return max(0, min(100, int(value)))
