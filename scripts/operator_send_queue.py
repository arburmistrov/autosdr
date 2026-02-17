#!/usr/bin/env python3
import argparse
import csv
import json
import subprocess
from pathlib import Path


def read_rows(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def pick_message(row):
    for key in ["linkedin_connection_note", "linkedin_followup_1", "message", "text"]:
        v = (row.get(key) or "").strip()
        if v:
            return v
    return ""


def copy_to_clipboard(text):
    subprocess.run(["pbcopy"], input=text.encode("utf-8"), check=True)


def open_url(url):
    subprocess.run(["open", url], check=False)


def load_state(path):
    if not path.exists():
        return {"index": 0, "sent": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"index": 0, "sent": []}


def save_state(path, state):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser(description="Operator mode for LinkedIn sending.")
    ap.add_argument("--input", required=True, help="CSV with linkedin_url and message columns")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--state", default="data/output/operator_send_state.json")
    args = ap.parse_args()

    rows = [r for r in read_rows(args.input) if (r.get("linkedin_url") or "").strip()]
    if not rows:
        raise SystemExit("No rows with linkedin_url in input CSV")

    batch = rows[: args.limit]
    state_path = Path(args.state)
    state = load_state(state_path)
    i = int(state.get("index", 0))

    print(f"Loaded {len(batch)} rows. Starting from index {i + 1}.")
    print("Flow per contact: open profile -> message copied -> you paste/send -> mark status")

    while i < len(batch):
        row = batch[i]
        name = (row.get("full_name") or "").strip()
        company = (row.get("company_name") or "").strip()
        url = (row.get("linkedin_url") or "").strip()
        msg = pick_message(row)
        if not msg:
            msg = f"Hi {name.split(' ')[0] if name else 'there'}, open to a short GenAI fit-check for {company or 'your team'}?"

        print("\n" + "=" * 72)
        print(f"[{i + 1}/{len(batch)}] {name} | {company}")
        print(url)

        open_url(url)
        copy_to_clipboard(msg)
        print("Message copied to clipboard.")
        print("In LinkedIn: click Message -> Cmd+V -> Send")

        action = input("Enter: [s]=sent, [k]=skip, [b]=back, [q]=quit: ").strip().lower()
        if action == "b":
            i = max(0, i - 1)
            state["index"] = i
            save_state(state_path, state)
            continue
        if action == "q":
            state["index"] = i
            save_state(state_path, state)
            print("Paused.")
            return

        state.setdefault("sent", []).append(
            {
                "idx": i + 1,
                "full_name": name,
                "company_name": company,
                "linkedin_url": url,
                "status": "sent" if action == "s" else "skipped",
            }
        )
        i += 1
        state["index"] = i
        save_state(state_path, state)

    print("Done for current batch.")
    print(f"State saved: {state_path}")


if __name__ == "__main__":
    main()
