import argparse
import hashlib
import getpass
import json
import secrets
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


APP_ID = "wx2a38ff386cea87ab"
DEFAULT_HOST = "https://api.qianjiapp.com/"
DEFAULT_VERSION_CODE = "405"
DEFAULT_VERSION_NAME = "4.2.2"
DEFAULT_PKG = "com.mutangtech.qianji"
REQUEST_ID_KEY = "free20170908&x_*1127"
ENCREQID_KEY = "michaeljackson"
REQUEST_TIME_OFFSET = (0x8A9 << 12) + 0x127
SRC_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TOKEN_FILE = SRC_ROOT / "config" / "qianji_token.json"


class ScriptError(RuntimeError):
    pass


def now_ms():
    return int(time.time() * 1000)


def md5_lower(value):
    return hashlib.md5(value.encode()).hexdigest()


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


def auth_path(root):
    path = Path(root)
    if path.suffix.lower() == ".json":
        return path
    return path / "state" / "auth.json"


def load_auth(root):
    return read_json(auth_path(root), {})


def save_auth(root, auth):
    auth["saved_at_ms"] = now_ms()
    write_json(auth_path(root), auth)


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


def request_qianji(root, host, ctrl, act, params, auth=None):
    auth = auth or {}
    version_code = str(auth.get("version_code") or DEFAULT_VERSION_CODE)
    user = auth.get("user") or {}
    uid = str(user.get("id") or auth.get("uid") or "")
    token = str(auth.get("token") or "")

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
    }
    headers.update(sign_headers(ctrl, act, version_code))
    if token:
        headers["htoken"] = "1"
        headers["utoken"] = token
    if uid and "fr" not in params:
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


def wait_for_wechat_code(redirect_uri, state):
    parsed = urllib.parse.urlparse(redirect_uri)
    if parsed.hostname not in ("127.0.0.1", "localhost"):
        raise ScriptError("Auto waiting only supports localhost redirect_uri")
    result = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            result["code"] = (query.get("code") or [""])[0]
            result["state"] = (query.get("state") or [""])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Qianji auth received. You can close this tab.")

        def log_message(self, *_args):
            return

    server = HTTPServer((parsed.hostname, parsed.port or 80), Handler)
    server.handle_request()
    if result.get("state") != state:
        raise ScriptError("Wechat state mismatch")
    if not result.get("code"):
        raise ScriptError("Wechat callback did not include code")
    return result["code"]


def open_wechat_qr_and_wait(redirect_uri):
    state = secrets.token_hex(12)
    params = {
        "appid": APP_ID,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "snsapi_login",
        "state": state,
    }
    url = "https://open.weixin.qq.com/connect/qrconnect?" + urllib.parse.urlencode(params) + "#wechat_redirect"
    print(json.dumps({"step": "open_browser", "url": url}, ensure_ascii=False))
    webbrowser.open(url)
    return wait_for_wechat_code(redirect_uri, state)


def login_wechat(args):
    root = Path(args.root)
    auth = load_auth(root)
    code = args.code
    if not code:
        code = open_wechat_qr_and_wait(args.redirect_uri)
    resp = request_qianji(root, args.host, "account", "loginwx", {"wxcode": code}, auth)
    data = resp.get("data") or {}
    if not data.get("token") or not data.get("user"):
        raise ScriptError(f"login response missing token/user: {resp}")
    auth.update(
        {
            "token": data["token"],
            "user": data["user"],
            "uid": str((data["user"] or {}).get("id") or ""),
            "books": data.get("books") or [],
            "is_new": bool(data.get("is_new")),
            "login_method": "wechat",
            "login_at_ms": now_ms(),
            "last_refresh_at_ms": now_ms(),
        }
    )
    save_auth(root, auth)
    print(json.dumps(auth if args.json else {"uid": auth.get("uid"), "books": len(auth.get("books") or [])}, ensure_ascii=False))


def save_login_response(root, auth, data, login_method):
    if not data.get("token") or not data.get("user"):
        raise ScriptError(f"login response missing token/user: {data}")
    auth.update(
        {
            "token": data["token"],
            "user": data["user"],
            "uid": str((data["user"] or {}).get("id") or ""),
            "books": data.get("books") or [],
            "is_new": bool(data.get("is_new")),
            "login_method": login_method,
            "login_at_ms": now_ms(),
            "last_refresh_at_ms": now_ms(),
        }
    )
    save_auth(root, auth)


def login_password(args):
    root = Path(args.root)
    auth = load_auth(root)
    account = args.account.strip()
    password = args.password if args.password is not None else getpass.getpass("Qianji password: ")
    resp = request_qianji(
        root,
        args.host,
        "account",
        "login",
        {"v": account, "pwd": md5_lower(password)},
        auth,
    )
    data = resp.get("data") or {}
    save_login_response(root, auth, data, "password")
    print(json.dumps(auth if args.json else {"uid": auth.get("uid"), "books": len(auth.get("books") or [])}, ensure_ascii=False))


def refresh(args):
    root = Path(args.root)
    auth = load_auth(root)
    uid = str(auth.get("uid") or (auth.get("user") or {}).get("id") or "")
    if not uid:
        raise ScriptError("Missing uid. Run login-wechat first or create state/auth.json.")
    resp = request_qianji(root, args.host, "account", "refreshtoken", {"uid": uid}, auth)
    data = resp.get("data") or {}
    token = data.get("v") or data.get("token")
    if not token:
        raise ScriptError(f"refresh response missing token: {resp}")
    auth["token"] = token
    auth["last_refresh_at_ms"] = now_ms()
    save_auth(root, auth)
    print(json.dumps(auth if args.json else {"uid": uid, "refreshed": True}, ensure_ascii=False))


def ensure(args):
    root = Path(args.root)
    auth = load_auth(root)
    last = int(auth.get("last_refresh_at_ms") or auth.get("login_at_ms") or 0)
    if not auth.get("token") or now_ms() - last > args.max_age_hours * 3600 * 1000:
        refresh(args)
        return
    print(json.dumps(auth if args.json else {"uid": auth.get("uid"), "fresh": True}, ensure_ascii=False))


def show(args):
    auth = load_auth(Path(args.root))
    if args.json:
        print(json.dumps(auth, ensure_ascii=False))
    else:
        print(json.dumps({"uid": auth.get("uid"), "has_token": bool(auth.get("token")), "books": len(auth.get("books") or [])}, ensure_ascii=False))


def build_parser():
    parser = argparse.ArgumentParser(description="Qianji token login and refresh script")
    parser.add_argument("--root", default=str(DEFAULT_TOKEN_FILE), help="Path to qianji token json")
    parser.add_argument("--host", default=DEFAULT_HOST)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("login-wechat")
    p.add_argument("--code", help="Wechat auth code. If omitted, open browser QR flow.")
    p.add_argument("--redirect-uri", default="http://127.0.0.1:8765/callback")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("login-password")
    p.add_argument("--account", required=True, help="Phone, email, or account id")
    p.add_argument("--password", help="If omitted, prompt without echo")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("refresh")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("ensure")
    p.add_argument("--max-age-hours", type=int, default=24 * 5)
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("show")
    p.add_argument("--json", action="store_true")
    return parser


def main():
    args = build_parser().parse_args()
    if args.cmd == "login-wechat":
        login_wechat(args)
    elif args.cmd == "login-password":
        login_password(args)
    elif args.cmd == "refresh":
        refresh(args)
    elif args.cmd == "ensure":
        ensure(args)
    elif args.cmd == "show":
        show(args)


if __name__ == "__main__":
    try:
        main()
    except ScriptError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
