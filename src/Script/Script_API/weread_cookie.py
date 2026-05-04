import argparse
import json
import time
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Union
from urllib.parse import unquote

import requests


WEREAD_BASE_URL = "https://weread.qq.com"
SRC_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_COOKIE_FILE = SRC_ROOT / "config" / "weread_cookie.json"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko)"
)

Cookie = Dict[str, str]
CookieInput = Union[str, Mapping[str, Any], Iterable[Mapping[str, Any]], None]


class WeReadCookieError(Exception):
    """Raised when a cookie operation cannot be completed."""


def parse_cookie_string(cookie_string: str) -> List[Cookie]:
    """解析浏览器格式的 Cookie 字符串。

    Args:
        cookie_string: 形如 "a=1; b=2" 的 Cookie 字符串。

    Returns:
        [{"name": "a", "value": "1"}, ...] 格式的 Cookie 列表。
    """
    cookies: List[Cookie] = []
    for part in cookie_string.split(";"):
        if "=" not in part:
            continue
        name, value = part.split("=", 1)
        name = unquote(name.strip())
        value = unquote(value.strip())
        if name:
            cookies.append({"name": name, "value": value})
    return cookies


def normalize_cookies(cookie_input: CookieInput) -> List[Cookie]:
    """把多种 Cookie 输入格式统一成列表格式。

    支持 Cookie 字符串、dict、{"cookies": [...]} JSON 结构、以及 Cookie 列表。
    """
    if cookie_input is None:
        return []

    if isinstance(cookie_input, str):
        return parse_cookie_string(cookie_input)

    if isinstance(cookie_input, Mapping):
        if "cookies" in cookie_input:
            return normalize_cookies(cookie_input["cookies"])
        if "name" in cookie_input and "value" in cookie_input:
            return [{"name": str(cookie_input["name"]), "value": str(cookie_input["value"])}]
        return [{"name": str(name), "value": str(value)} for name, value in cookie_input.items()]

    cookies: List[Cookie] = []
    for item in cookie_input:
        if "name" in item and "value" in item:
            cookies.append({"name": str(item["name"]), "value": str(item["value"])})
    return cookies


def cookie_to_dict(cookies: CookieInput) -> Dict[str, str]:
    """把 Cookie 转为 {name: value} 字典，便于查找和传给 requests。"""
    return {cookie["name"]: cookie["value"] for cookie in normalize_cookies(cookies)}


def cookie_to_string(cookies: CookieInput) -> str:
    """把 Cookie 转为浏览器请求头可用的字符串。"""
    return "; ".join(
        f"{cookie['name']}={cookie['value']}" for cookie in normalize_cookies(cookies)
    )


def load_cookie(path: Union[str, Path] = DEFAULT_COOKIE_FILE) -> List[Cookie]:
    """从本地文件静默读取 Cookie。

    文件不存在、文件为空时返回空列表，不打印、不弹窗。
    支持本模块保存的 JSON，也支持纯 Cookie 字符串文件。
    """
    cookie_path = Path(path)
    if not cookie_path.exists():
        return []

    raw = cookie_path.read_text(encoding="utf-8").strip()
    if not raw:
        return []

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return parse_cookie_string(raw)
    return normalize_cookies(data)


def save_cookie(cookies: CookieInput, path: Union[str, Path] = DEFAULT_COOKIE_FILE) -> None:
    """把 Cookie 静默保存到本地 JSON 文件。

    保存格式包含 updated_at 和 cookies，便于后续自动加载与刷新。
    """
    payload = {"updated_at": int(time.time()), "cookies": normalize_cookies(cookies)}
    cookie_path = Path(path)
    cookie_path.parent.mkdir(parents=True, exist_ok=True)
    cookie_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _new_session(cookies: CookieInput = None) -> requests.Session:
    """创建带微信读书请求头和 Cookie 的 requests.Session。"""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "accept": "application/json, text/plain, */*",
            "Content-Type": "application/json"
        }
    )
    for name, value in cookie_to_dict(cookies).items():
        session.cookies.set(name, value, domain=".weread.qq.com", path="/")
    return session


def _cookies_from_session(session: requests.Session) -> List[Cookie]:
    """从 requests.Session 中取出当前 Cookie。"""
    return [{"name": cookie.name, "value": cookie.value} for cookie in session.cookies]


def _response_set_cookie_headers(response: requests.Response) -> List[str]:
    """兼容不同 requests/urllib3 版本，提取响应里的所有 Set-Cookie 头。"""
    headers = response.raw.headers
    if hasattr(headers, "getlist"):
        return list(headers.getlist("Set-Cookie"))
    header_value = response.headers.get("Set-Cookie")
    return [header_value] if header_value else []


def merge_set_cookie(cookies: CookieInput, set_cookie_headers: Union[str, Iterable[str], None]) -> List[Cookie]:
    """把响应 Set-Cookie 合并到已有 Cookie。

    Args:
        cookies: 原有 Cookie。
        set_cookie_headers: 一个或多个 Set-Cookie 响应头。

    Returns:
        合并后的 Cookie 列表。
    """
    merged = cookie_to_dict(cookies)
    if set_cookie_headers is None:
        return normalize_cookies(merged)

    headers = [set_cookie_headers] if isinstance(set_cookie_headers, str) else list(set_cookie_headers)
    for header in headers:
        simple_cookie = SimpleCookie()
        simple_cookie.load(header)
        for name, morsel in simple_cookie.items():
            merged[name] = morsel.value
    return normalize_cookies(merged)


def verify_cookie(cookies: CookieInput, timeout: int = 30) -> bool:
    """静默验证 Cookie 是否仍然可用。

    通过 GET /api/user/notebook 判断登录状态。
    网络错误、401、登录过期都会返回 False，不向用户输出提示。
    """
    if not normalize_cookies(cookies):
        return False

    session = _new_session(cookies)
    try:
        response = session.get(f"{WEREAD_BASE_URL}/api/user/notebook", timeout=timeout)
        data = response.json()
    except (requests.RequestException, ValueError):
        return False

    if response.status_code == 401:
        return False
    if isinstance(data, dict) and data.get("errcode") == -2012:
        return False
    return isinstance(data, dict) and "books" in data


def refresh_cookie(cookies: CookieInput, timeout: int = 30) -> List[Cookie]:
    """静默刷新 Cookie。

    模拟插件逻辑，对 https://weread.qq.com 发 HEAD 请求，并合并服务端返回的 Set-Cookie。
    如果网络失败或没有新 Cookie，会返回原 Cookie。
    """
    current = normalize_cookies(cookies)
    if not current:
        return []

    session = _new_session(current)
    try:
        response = session.head(WEREAD_BASE_URL, timeout=timeout)
    except requests.RequestException:
        return current

    refreshed = _cookies_from_session(session)
    set_cookie_headers = _response_set_cookie_headers(response)
    if set_cookie_headers:
        refreshed = merge_set_cookie(refreshed, set_cookie_headers)
    return refreshed or current


def login_by_qrcode(
    output_path: Optional[Union[str, Path]] = None,
    timeout: int = 180,
    headless: bool = False
) -> List[Cookie]:
    """打开浏览器扫码登录微信读书，并自动提取 Cookie。

    这是本文件中唯一需要用户感知/操作的方法：用户需要在弹出的浏览器里扫码登录。

    Args:
        output_path: 可选，登录成功后把 Cookie 保存到该路径。
        timeout: 等待扫码登录的最长时间，单位秒。
        headless: 是否使用无头浏览器。扫码登录通常需要 False。

    Returns:
        登录成功后的 Cookie 列表。
    """
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise WeReadCookieError(
            "Playwright is required for QR login. Install it with: "
            "pip install playwright && playwright install chromium"
        ) from exc

    deadline = time.time() + timeout
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        page.goto(f"{WEREAD_BASE_URL}/#login", wait_until="domcontentloaded")

        try:
            while time.time() < deadline:
                cookies = [
                    {"name": item["name"], "value": item["value"]}
                    for item in context.cookies()
                    if item.get("domain", "").endswith("weread.qq.com")
                ]
                cookie_map = cookie_to_dict(cookies)
                has_identity = bool(cookie_map.get("wr_vid"))
                has_session = bool(cookie_map.get("wr_name") or cookie_map.get("wr_skey"))
                if has_identity and has_session and verify_cookie(cookies):
                    browser.close()
                    if output_path:
                        save_cookie(cookies, output_path)
                    return cookies
                page.wait_for_timeout(1000)
        except PlaywrightTimeoutError:
            pass
        finally:
            browser.close()

    raise WeReadCookieError("Timed out waiting for WeRead QR login.")


def ensure_valid_cookie(
    path: Union[str, Path] = DEFAULT_COOKIE_FILE,
    allow_login: bool = False,
    timeout: int = 30
) -> List[Cookie]:
    """确保本地 Cookie 可用，默认全程静默。

    流程：
    1. 从 path 加载 Cookie。
    2. 后台刷新 Cookie。
    3. 后台校验 Cookie。
    4. 成功则保存刷新后的 Cookie 并返回。
    5. 失败时，如果 allow_login=True，则触发扫码登录；否则返回空列表。
    """
    cookies = load_cookie(path)
    if cookies:
        refreshed = refresh_cookie(cookies, timeout=timeout)
        if verify_cookie(refreshed, timeout=timeout):
            save_cookie(refreshed, path)
            return refreshed

    if allow_login:
        return login_by_qrcode(output_path=path)

    return []


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage WeRead cookies silently by default.")
    parser.add_argument("command", choices=["login", "verify", "refresh", "ensure", "print"])
    parser.add_argument("--cookie")
    parser.add_argument("--cookie-file", default=str(DEFAULT_COOKIE_FILE))
    parser.add_argument("--output")
    parser.add_argument("--allow-login", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()

    output_path = args.output or args.cookie_file
    input_cookies = args.cookie if args.cookie else load_cookie(args.cookie_file)

    try:
        if args.command == "login":
            cookies = login_by_qrcode(output_path=output_path, timeout=args.timeout)
            if args.verbose:
                print(f"Saved {len(cookies)} cookies to {output_path}")
            return

        if args.command == "verify":
            is_valid = verify_cookie(input_cookies)
            if args.verbose:
                print("valid" if is_valid else "invalid")
            raise SystemExit(0 if is_valid else 1)

        if args.command == "refresh":
            cookies = refresh_cookie(input_cookies)
            save_cookie(cookies, output_path)
            if args.verbose:
                print(f"Saved {len(cookies)} refreshed cookies to {output_path}")
            return

        if args.command == "ensure":
            cookies = ensure_valid_cookie(
                path=args.cookie_file,
                allow_login=args.allow_login,
                timeout=args.timeout
            )
            if not cookies:
                raise SystemExit(1)
            if args.verbose:
                print(f"Cookie is valid: {args.cookie_file}")
            return

        if args.command == "print":
            print(cookie_to_string(input_cookies))
    except WeReadCookieError as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
