#!/usr/bin/env python3
import argparse
import csv
import random
import re
import smtplib
import socket
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import dns.resolver

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


@dataclass
class CheckResult:
    status: str
    reason: str
    mx: str = ""
    smtp_code: str = ""
    score: int = 0


def read_csv(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def domain_of(email: str) -> str:
    return email.split("@", 1)[1].lower().strip()


def resolve_mx(domain: str, timeout: float = 5.0) -> List[str]:
    r = dns.resolver.Resolver()
    r.timeout = timeout
    r.lifetime = timeout
    answers = r.resolve(domain, "MX")
    records = sorted([(a.preference, str(a.exchange).rstrip(".")) for a in answers], key=lambda x: x[0])
    return [mx for _, mx in records]


def smtp_rcpt_check(mx_host: str, target_email: str, verify_from: str, timeout: float = 2.5) -> Tuple[str, str]:
    try:
        server = smtplib.SMTP(mx_host, 25, timeout=timeout)
        server.ehlo_or_helo_if_needed()
        server.mail(verify_from)
        code, msg = server.rcpt(target_email)
        server.quit()
        return str(code), (msg.decode(errors="ignore") if isinstance(msg, bytes) else str(msg))
    except (socket.timeout, TimeoutError):
        return "timeout", "smtp_timeout"
    except Exception as e:
        return "error", f"smtp_error:{type(e).__name__}"


def random_local() -> str:
    return "zzcheck_" + "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(12))


def classify_code(code: str) -> str:
    if code in {"250", "251", "252"}:
        return "valid"
    if code.startswith("55") or code in {"550", "551", "553"}:
        return "invalid"
    if code.startswith("45") or code.startswith("42"):
        return "unknown"
    if code in {"timeout", "error"}:
        return "unknown"
    return "unknown"


def check_email(email: str, verify_from: str, catchall_cache: Dict[str, bool]) -> CheckResult:
    email = (email or "").strip()
    if not EMAIL_RE.match(email):
        return CheckResult("invalid", "invalid_syntax", score=0)

    dom = domain_of(email)
    try:
        mxs = resolve_mx(dom)
    except Exception:
        return CheckResult("invalid", "no_mx", score=0)
    if not mxs:
        return CheckResult("invalid", "no_mx", score=0)

    mx = mxs[0]
    code, _ = smtp_rcpt_check(mx, email, verify_from)
    status = classify_code(code)

    # detect catch-all once per domain
    if status == "valid":
        return CheckResult("valid", "rcpt_ok", mx=mx, smtp_code=code, score=90)
    if status == "invalid":
        return CheckResult("invalid", "rcpt_rejected", mx=mx, smtp_code=code, score=0)
    return CheckResult("unknown", "smtp_uncertain", mx=mx, smtp_code=code, score=55)


def main():
    ap = argparse.ArgumentParser(description="Email hygiene check for queue")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--verify-from", default="aburmistrov@s-pro.io")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--limit", type=int, default=0, help="0 = all")
    ap.add_argument("--top-bucket", default="", help="filter by priority_bucket, e.g. top20")
    args = ap.parse_args()

    rows = read_csv(Path(args.input))
    if args.top_bucket:
        rows = [r for r in rows if (r.get("priority_bucket") or "") == args.top_bucket]
    if args.limit > 0:
        rows = rows[: args.limit]

    catchall_cache: Dict[str, bool] = {}

    def work(row: dict):
        email = (row.get("email") or "").strip()
        return row, check_email(email, args.verify_from, catchall_cache)

    out_rows = []
    print(f"starting verification: rows={len(rows)} workers={args.workers}")
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(work, r) for r in rows]
        for i, fut in enumerate(as_completed(futures), start=1):
            row, res = fut.result()
            nr = dict(row)
            nr["verification_status"] = res.status
            nr["verification_reason"] = res.reason
            nr["verification_mx"] = res.mx
            nr["verification_smtp_code"] = res.smtp_code
            nr["verification_score"] = str(res.score)
            # final send gate: allow only valid + unknown(>=55); risky/invalid blocked
            nr["keep_for_send"] = "true" if res.status in {"valid", "unknown"} else "false"
            if res.status in {"invalid", "risky"}:
                prev = (nr.get("exclusion_reason") or "").strip()
                nr["exclusion_reason"] = (prev + "," if prev else "") + f"email_{res.reason}"
            out_rows.append(nr)
            if i % 200 == 0:
                print(f"checked {i}/{len(rows)}")

    out_rows.sort(key=lambda r: int(r.get("verification_score") or 0), reverse=True)
    fields = []
    if out_rows:
        seen = set()
        for r in out_rows:
            for k in r.keys():
                if k not in seen:
                    seen.add(k)
                    fields.append(k)
    write_csv(Path(args.output), out_rows, fields)

    stats = {"valid": 0, "invalid": 0, "risky": 0, "unknown": 0}
    for r in out_rows:
        stats[r.get("verification_status", "unknown")] = stats.get(r.get("verification_status", "unknown"), 0) + 1
    print(f"done: {len(out_rows)} rows -> {args.output}")
    print("stats", stats)


if __name__ == "__main__":
    main()
