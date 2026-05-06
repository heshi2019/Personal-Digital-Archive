import argparse
import hashlib
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path


DEFAULT_HOST = "https://api.qianjiapp.com/"
DEFAULT_VERSION_CODE = "405"
DEFAULT_VERSION_NAME = "4.2.2"
DEFAULT_PKG = "com.mutangtech.qianji"
MAX_PULL_PAGES = 200
REQUEST_ID_KEY = "free20170908&x_*1127"
ENCREQID_KEY = "michaeljackson"
REQUEST_TIME_OFFSET = (0x8A9 << 12) + 0x127
SRC_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STATE_ROOT = SRC_ROOT / "config" / "qianji_state"


class ScriptError(RuntimeError):
    pass


def log(message):
    print(f"[qianji_api] {message}", file=sys.stderr, flush=True)


def md5_lower(value):
    return hashlib.md5(value.encode()).hexdigest()


def now_ms():
    return int(datetime.now().timestamp() * 1000)


def read_json(path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    tmp.replace(path)


def load_auth(path):
    if str(path) == "-":
        return json.load(sys.stdin)
    return read_json(Path(path), {})


def request_id(package_name, ctrl, act, version_code):
    time_part = now_ms() + int(version_code) + REQUEST_TIME_OFFSET
    data = f"{package_name}{time_part}{ctrl}{act}"
    return md5_lower(data + REQUEST_ID_KEY)


def encreqid(reqid):
    return md5_lower(reqid + ENCREQID_KEY)


def request_tok(reqid, ctrl, act):
    return md5_lower(f"{reqid}1172020{ctrl}{encreqid(reqid)}{act}")


def sign_headers(ctrl, act, version_code):
    reqid = request_id(DEFAULT_PKG, ctrl, act, int(version_code))
    return {"reqidv2": reqid, "tok": request_tok(reqid, ctrl, act)}


def request_qianji(root, host, ctrl, act, params, auth):
    version_code = str(auth.get("version_code") or DEFAULT_VERSION_CODE)
    user = auth.get("user") or {}
    uid = str(user.get("id") or auth.get("uid") or "")
    token = str(auth.get("token") or "")
    if not uid:
        raise ScriptError("auth json missing uid")
    if not token:
        raise ScriptError("auth json missing token")

    headers = {
        "ctrl": ctrl,
        "act": act,
        "mk": auth.get("market") or "none",
        "os": "1",
        "osvs": str(auth.get("os_version") or "33"),
        "devbrand": auth.get("device_brand") or "XIAOMI",
        "devname": auth.get("device_model") or "M2012K11AC",
        "devid": auth.get("device_id") or md5_lower(str(root.resolve())),
        "vs": version_code,
        "pkg": auth.get("package") or DEFAULT_PKG,
        "vsn": auth.get("version_name") or DEFAULT_VERSION_NAME,
        "timezoneoffset": str(auth.get("timezoneoffset") or 480),
        "clang": auth.get("language") or "zh",
        "cregion": auth.get("region") or "CN",
        "htoken": "1",
        "utoken": token,
    }
    headers.update(sign_headers(ctrl, act, version_code))
    if "fr" not in params:
        params = {**params, "fr": uid}

    url = urllib.parse.urljoin(host.rstrip("/") + "/", f"{ctrl}/{act}")
    body = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise ScriptError(f"HTTP {exc.code}: {raw}") from exc
    except urllib.error.URLError as exc:
        raise ScriptError(f"Network error: {exc}") from exc
    data = json.loads(raw)
    if data.get("ec") != 200:
        raise ScriptError(format_qianji_error(data))
    return data


def format_qianji_error(data):
    em = data.get("em")
    msg = em
    if isinstance(em, str) and em.startswith("{"):
        try:
            msg = json.loads(em).get("msg") or em
        except json.JSONDecodeError:
            msg = em
    return f"Qianji error ec={data.get('ec')} msg={msg} data={data.get('data')}"


def cursor_path(root):
    return root / "state" / "sync_cursor.json"


def save_raw(root, run_id, page_index, payload):
    write_json(root / "raw" / run_id / f"page_{page_index:03d}.json", payload)


def pull_all(args):
    root = Path(args.root)
    auth = load_auth(args.auth_json)
    uid = str(auth.get("uid") or (auth.get("user") or {}).get("id") or "")
    cursor = {} if args.mode == "full" else read_json(cursor_path(root), {})
    lasttimes = None if args.mode == "full" else cursor.get("lasttimes")
    bookid = -1
    pageoffset = 0
    pagesign = ""
    total_count = 0
    pages = 0
    merged_lasttimes = {}
    raw_pages = []
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log(f"start sync mode={args.mode} run_id={run_id}")

    while True:
        pages += 1
        if pages > MAX_PULL_PAGES:
            raise ScriptError(f"Exceeded max pages: {MAX_PULL_PAGES}")
        log(f"pull page={pages} bookid={bookid} pageoffset={pageoffset}")
        params = {
            "uid": uid,
            "bookid": str(bookid),
            "pageoffset": str(pageoffset),
            "pagesign": pagesign,
        }
        if lasttimes:
            params["lasttimes"] = json.dumps(lasttimes, ensure_ascii=False, separators=(",", ":"))
        resp = request_qianji(root, args.host, "syncv2", "pull", params, auth)
        if args.save_raw:
            save_raw(root, run_id, pages, resp)
        if args.include_pages:
            raw_pages.append(resp)
        data = resp.get("data") or {}
        changes = data.get("changes") or []
        deletes = data.get("deletes") or []
        categories = data.get("categories") or []
        total_count += int(data.get("count") or len(changes) + len(deletes) + len(categories))
        log(
            "page done "
            f"changes={len(changes)} deletes={len(deletes)} categories={len(categories)} "
            f"hasmore={int(data.get('hasmore') or 0)}"
        )
        if isinstance(data.get("lasttimes"), dict):
            merged_lasttimes.update(data["lasttimes"])
        hasmore = int(data.get("hasmore") or 0) == 1
        pageoffset = int(data.get("pageoffset") or 0)
        pagesign = str(data.get("pagesign") or "")
        bookid = int(data.get("bookid") or bookid)
        if not hasmore:
            break

    if merged_lasttimes:
        write_json(
            cursor_path(root),
            {
                "lasttimes": merged_lasttimes,
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "last_run_id": run_id,
            },
        )
        log("sync cursor saved")
    result = {"run_id": run_id, "pages": pages, "count": total_count, "cursor_saved": bool(merged_lasttimes)}
    if args.include_pages:
        result["raw_pages"] = raw_pages
    log(f"sync finished pages={pages} count={total_count}")
    return result


def build_parser():
    parser = argparse.ArgumentParser(description="Qianji API full/incremental sync script")
    parser.add_argument("--root", default=str(DEFAULT_STATE_ROOT))
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--auth-json", required=True, help="Path to auth json from qianji_token.py, or - for stdin")
    parser.add_argument("--mode", choices=["full", "incremental"], required=True)
    parser.add_argument("--include-pages", action="store_true", help="Return pulled pages in stdout for qianji_main.py")
    parser.add_argument("--save-raw", action="store_true", help="Also save raw page files under raw/<run_id>")
    return parser


def main():
    args = build_parser().parse_args()
    print(json.dumps(pull_all(args), ensure_ascii=True, separators=(",", ":")))


if __name__ == "__main__":
    try:
        main()
    except ScriptError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
