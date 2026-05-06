import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path


TYPE_LABELS = {
    0: "支出",
    1: "收入",
    2: "转账",
    7: "债务-借出",
}

SRC_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_API_DIR = SRC_ROOT / "Script" / "Script_API"
QIANJI_TOKEN_FILE = SRC_ROOT / "config" / "qianji_token.json"
QIANJI_STATE_ROOT = SRC_ROOT / "config" / "qianji_state"
DATA_STAR_ROOT = SRC_ROOT / "data" / "Data_Star"
DATA_END_ROOT = SRC_ROOT / "data" / "Data_End"


def log(message):
    print(f"[qianji_main] {message}", flush=True)


def read_json(path, default=None):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
        f.write("\n")
    tmp.replace(path)


def write_compact_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, separators=(",", ":"))
    tmp.replace(path)


def run_json(cmd):
    proc = subprocess.Popen(cmd, text=True, encoding="utf-8", errors="replace", stdout=subprocess.PIPE)
    stdout, _ = proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(stdout.strip() or f"command failed: {' '.join(cmd)}")
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        pass
    last = ""
    for line in stdout.splitlines():
        if line.strip():
            last = line.strip()
    return json.loads(last) if last else {}


def ensure_token(args, token_script, token_file):
    ensure_cmd = [
        args.python,
        str(token_script),
        "--root",
        str(token_file),
        "ensure",
        "--max-age-hours",
        str(args.max_age_hours),
        "--json",
    ]

    try:
        return run_json(ensure_cmd)
    except RuntimeError as exc:
        if not args.account:
            raise RuntimeError(
                "Qianji token is missing or expired. Re-run qianji_main.py with --account, "
                "and optionally --password, so it can login and create src/config/qianji_token.json."
            ) from exc

        log("token unavailable, login with account password")
        login_cmd = [
            args.python,
            str(token_script),
            "--root",
            str(token_file),
            "login-password",
            "--account",
            args.account,
            "--json",
        ]
        if args.password is not None:
            login_cmd.extend(["--password", args.password])
        return run_json(login_cmd)


def load_changes(raw_pages):
    bills_by_id = {}
    deletes = []
    page_count = 0
    for payload in raw_pages:
        page_count += 1
        data = payload.get("data") or {}
        deletes.extend(data.get("deletes") or [])
        for bill in data.get("changes") or []:
            bill_id = bill.get("id")
            if bill_id is not None:
                bills_by_id[str(bill_id)] = bill
    return list(bills_by_id.values()), deletes, page_count


def load_books(auth_json):
    auth = read_json(auth_json, {})
    books = auth.get("books")
    return books if isinstance(books, list) else []


def resolve_book_id(books, book_name):
    if not book_name:
        return None
    for book in books:
        if str(book.get("name") or "") == book_name:
            return int(book["bookid"])
    raise RuntimeError(f"Book not found in auth json: {book_name}")


def book_export_name(books, book_id, fallback):
    if book_id is None:
        visible = [b for b in books if b.get("visible", 1) == 1]
        return f"books{len(visible) or fallback}"
    for book in books:
        if int(book.get("bookid")) == int(book_id):
            return str(book.get("name") or book_id)
    return str(book_id)


def positive_int(value):
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return False


def bill_date(value):
    if not value:
        return ""
    return datetime.fromtimestamp(int(value)).strftime("%Y-%m-%d %H:%M:%S")


def bill_category(bill):
    category = bill.get("category")
    if isinstance(category, dict):
        return category.get("name") or "其它"
    return "其它"


def official_like_bill(bill):
    type_code = bill.get("type")
    item = {
        "key": f"qj{bill.get('id')}",
        "date": bill_date(bill.get("time")),
        "category": bill_category(bill),
        "type": TYPE_LABELS.get(type_code, str(type_code)),
        "money": bill.get("money", 0),
        "currency": "CNY",
    }
    if positive_int(bill.get("assetid")):
        item["asset"] = bill.get("assetid")
    if positive_int(bill.get("fromid")):
        item["from"] = bill.get("fromid")
    if positive_int(bill.get("targetid")):
        item["target"] = bill.get("targetid")
    if bill.get("remark"):
        item["remark"] = bill.get("remark")
    item.update(
        {
            "hasbx": 0,
            "username": bill.get("username"),
            "billflag": bill.get("billflag"),
            "sourceid": "",
        }
    )
    if bill.get("images"):
        item["images"] = bill.get("images")
    return item


def official_file_name(export_name):
    return "qianji.json"


def export_official_like(sync, auth_file, book_id=None, book_name=None):
    raw_pages = sync.get("raw_pages") or []
    if not raw_pages:
        raise RuntimeError("Sync result missing raw_pages. qianji_main.py must call qianji_api.py with --include-pages.")
    log("start export")
    books = load_books(auth_file)
    if book_name:
        book_id = resolve_book_id(books, book_name)

    bills, deletes, page_count = load_changes(raw_pages)
    if book_id is not None:
        bills = [bill for bill in bills if int(bill.get("bookid")) == int(book_id)]
    bills = [bill for bill in bills if int(bill.get("status") or 0) == 1]
    bills.sort(key=lambda b: (int(b.get("time") or 0), int(b.get("id") or 0)), reverse=True)

    export_name = book_export_name(books, book_id, 1)
    end_root = DATA_END_ROOT
    start_root = DATA_STAR_ROOT
    official_file = end_root / official_file_name(export_name)
    official_like = [official_like_bill(bill) for bill in bills]
    log(f"export bills={len(official_like)} deletes={len(deletes)} book={export_name}")
    report = {
        "run_id": sync.get("run_id"),
        "source": "memory",
        "pages": page_count,
        "book_id": book_id,
        "book_name": export_name,
        "bill_count": len(bills),
        "delete_count": len(deletes),
        "official_export_file": str(official_file),
        "official_export_is_local_db_export": True,
    }
    write_compact_json(official_file, official_like)
    log(f"write final data: {official_file}")
    write_json(start_root / "qianji_raw_bills.json", bills)
    write_json(start_root / "qianji_deletes.json", deletes)
    write_json(start_root / "qianji_export_report.json", report)
    log(f"write source data: {start_root}")
    return report


def build_parser():
    parser = argparse.ArgumentParser(description="Qianji main runner")
    parser.add_argument("--token-file", default=str(QIANJI_TOKEN_FILE), help="Path to qianji token json")
    parser.add_argument("--state-root", default=str(QIANJI_STATE_ROOT), help="State directory for Qianji sync cursor")
    parser.add_argument("--api-dir", default=str(SCRIPT_API_DIR), help="Directory containing qianji_token.py and qianji_api.py")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--max-age-hours", type=int, default=24 * 5)
    parser.add_argument("--account", help="Qianji account. Used by qianji_main.py when token needs login.")
    parser.add_argument("--password", help="Qianji password. If omitted while --account is set, qianji_token.py will prompt.")
    parser.add_argument("--skip-sync", action="store_true")
    parser.add_argument("--skip-export", action="store_true")
    parser.add_argument("--book-id", type=int)
    parser.add_argument("--book-name")
    return parser


def main():
    args = build_parser().parse_args()
    token_file = Path(args.token_file)
    state_root = Path(args.state_root)
    api_dir = Path(args.api_dir)
    token_script = api_dir / "qianji_token.py"
    api_script = api_dir / "qianji_api.py"
    auth_file = token_file

    log("check token")
    auth = ensure_token(args, token_script, token_file)
    print(json.dumps({"step": "token", "uid": auth.get("uid"), "has_token": bool(auth.get("token"))}, ensure_ascii=False))
    if args.skip_sync:
        log("skip sync")
        return

    log(f"start api sync mode={args.mode}")
    sync = run_json(
        [
            args.python,
            str(api_script),
            "--root",
            str(state_root),
            "--auth-json",
            str(auth_file),
            "--mode",
            args.mode,
            "--include-pages",
        ]
    )
    sync_log = {k: v for k, v in sync.items() if k != "raw_pages"}
    print(json.dumps({"step": "sync", **sync_log}, ensure_ascii=False))
    if args.skip_export:
        log("skip export")
        return

    exported = export_official_like(sync, auth_file, args.book_id, args.book_name)
    print(json.dumps({"step": "export", **exported}, ensure_ascii=False))
    log("done")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
