import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Union
from urllib.parse import unquote

import requests


WEREAD_BASE_URL = "https://weread.qq.com"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko)"
)

CookieInput = Union[str, Mapping[str, str], Iterable[Mapping[str, str]], None]


class WeReadError(Exception):
    """Base exception for WeRead API failures."""


class WeReadAuthError(WeReadError):
    """Raised when WeRead reports that the login cookie is invalid or expired."""


class WeReadRequestError(WeReadError):
    """Raised when a WeRead request fails for non-auth reasons."""


def parse_cookie_string(cookie_string: str) -> Dict[str, str]:
    cookies: Dict[str, str] = {}
    for part in cookie_string.split(";"):
        if "=" not in part:
            continue
        name, value = part.split("=", 1)
        name = unquote(name.strip())
        value = unquote(value.strip())
        if name:
            cookies[name] = value
    return cookies


def cookie_to_string(cookies: Mapping[str, str]) -> str:
    return "; ".join(f"{name}={value}" for name, value in cookies.items())


def normalize_cookies(cookie_input: CookieInput) -> Dict[str, str]:
    if cookie_input is None:
        return {}

    if isinstance(cookie_input, str):
        return parse_cookie_string(cookie_input)

    if isinstance(cookie_input, Mapping):
        if "cookies" in cookie_input and not isinstance(cookie_input.get("cookies"), str):
            return normalize_cookies(cookie_input["cookies"])  # type: ignore[index]
        if "name" in cookie_input and "value" in cookie_input:
            return {str(cookie_input["name"]): str(cookie_input["value"])}
        return {str(name): str(value) for name, value in cookie_input.items()}

    cookies: Dict[str, str] = {}
    for item in cookie_input:
        if "name" in item and "value" in item:
            cookies[str(item["name"])] = str(item["value"])
    return cookies


def load_cookie_file(path: Union[str, Path]) -> Dict[str, str]:
    raw = Path(path).read_text(encoding="utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return parse_cookie_string(raw)
    return normalize_cookies(data)


class WeReadApiClient:
    """微信读书 API 客户端。

    这个类只负责调用接口并返回微信读书的原始 JSON，不负责登录、不负责刷新 Cookie。
    调用方可以从 weread_cookie.py 获取 Cookie 后传进来。
    """

    def __init__(
        self,
        cookies: CookieInput = None,
        base_url: str = WEREAD_BASE_URL,
        timeout: int = 30,
        session: Optional[requests.Session] = None
    ) -> None:
        """初始化 API 客户端。

        Args:
            cookies: Cookie 字符串、dict，或 [{"name": "...", "value": "..."}] 列表。
            base_url: 微信读书 Web 站点地址，默认 https://weread.qq.com。
            timeout: 单次请求超时时间，单位秒。
            session: 可选 requests.Session，便于外部复用连接或自定义配置。
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "User-Agent": DEFAULT_USER_AGENT,
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "accept": "application/json, text/plain, */*",
                "Content-Type": "application/json"
            }
        )
        self.set_cookies(cookies)

    def set_cookies(self, cookies: CookieInput) -> None:
        """把 Cookie 写入当前 requests.Session。

        只更新客户端内存里的 Session，不读写本地文件。
        """
        for name, value in normalize_cookies(cookies).items():
            self.session.cookies.set(name, value, domain=".weread.qq.com", path="/")

    def get_cookie_string(self) -> str:
        """返回当前 Session 中的 Cookie 字符串，格式为 name=value; name2=value2。"""
        return cookie_to_string({cookie.name: cookie.value for cookie in self.session.cookies})

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        """发送底层 HTTP 请求并返回 JSON/文本结果。

        如果微信读书返回登录失效，会抛出 WeReadAuthError。
        如果 HTTP 请求失败或状态码异常，会抛出 WeReadRequestError。
        """
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        try:
            response = self.session.request(method, url, timeout=self.timeout, **kwargs)
        except requests.RequestException as exc:
            raise WeReadRequestError(f"WeRead request failed: {exc}") from exc

        data: Any
        try:
            data = response.json()
        except ValueError:
            data = response.text

        errcode = data.get("errcode") or data.get("errCode") if isinstance(data, dict) else None
        if response.status_code == 401 or errcode in {-2012, -2010}:
            raise WeReadAuthError("WeRead cookie is invalid or expired.")

        if response.status_code >= 400:
            raise WeReadRequestError(
                f"WeRead request failed with HTTP {response.status_code}: {data}"
            )

        return data

    def get_notebooks(self) -> Any:
        """获取当前账号的微信读书 Notebook 列表。

        对应插件里的 GET /api/user/notebook。
        返回值通常包含 books 字段，里面是有笔记/划线记录的书籍列表。
        """
        return self.request("GET", "/api/user/notebook")

    def get_book(self, book_id: str) -> Any:
        """获取单本书的详情信息。

        Args:
            book_id: 微信读书 bookId。

        返回书名、作者、封面、分类、出版社、ISBN、简介、评分等原始字段。
        """
        return self.request("GET", "/web/book/info", params={"bookId": book_id})

    def get_bookmark_list(self, book_id: str) -> Any:
        """获取单本书的划线/高亮列表。

        Args:
            book_id: 微信读书 bookId。

        返回值通常包含 updated、chapters、book 等字段。
        """
        return self.request("GET", "/web/book/bookmarklist", params={"bookId": book_id})

    def get_review_list(self, book_id: str) -> Any:
        """获取单本书的个人想法、页面笔记、章节笔记、书评等。

        Args:
            book_id: 微信读书 bookId。

        listType=11、mine=1、synckey=0 与 Obsidian 插件当前可用接口保持一致。
        """
        return self.request(
            "GET",
            "/web/review/list",
            params={"bookId": book_id, "listType": 11, "mine": 1, "synckey": 0}
        )

    def get_chapter_infos(self, book_id: str) -> Any:
        """获取单本书的章节目录信息。

        Args:
            book_id: 微信读书 bookId。

        使用 POST /web/book/chapterInfos，body 为 {"bookIds": [book_id]}。
        """
        return self.request("POST", "/web/book/chapterInfos", json={"bookIds": [book_id]})

    def get_progress(self, book_id: str) -> Any:
        """获取单本书的阅读进度。

        Args:
            book_id: 微信读书 bookId。

        返回值通常包含 book.progress、book.readingTime、book.startReadingTime、book.finishTime。
        """
        return self.request("GET", "/web/book/getProgress", params={"bookId": book_id})

    def get_read_info(self, book_id: str) -> Any:
        """获取旧版阅读信息接口返回值。

        Args:
            book_id: 微信读书 bookId。

        这是兼容方法；当前插件注释里也建议优先使用 get_progress()。
        """
        return self.request(
            "GET",
            "/web/book/readinfo",
            params={
                "bookId": book_id,
                "readingDetail": 1,
                "readingBookIndex": 1,
                "finishedDate": 1
            }
        )

    def get_full_book_data(self, book_id: str) -> Dict[str, Any]:
        """聚合获取单本书的核心数据。

        Args:
            book_id: 微信读书 bookId。

        Returns:
            dict，包含 book、bookmarks、reviews、chapters、progress 五个原始 JSON 返回值。
        """
        return {
            "book": self.get_book(book_id),
            "bookmarks": self.get_bookmark_list(book_id),
            "reviews": self.get_review_list(book_id),
            "chapters": self.get_chapter_infos(book_id),
            "progress": self.get_progress(book_id)
        }

    @staticmethod
    def transform_id(book_id: str) -> tuple[str, list[str]]:
        """把 bookId 转换成微信读书网页 reader URL 所需的中间编码。"""
        if book_id.isdigit():
            return "3", [format(int(book_id[i : min(i + 9, len(book_id))]), "x") for i in range(0, len(book_id), 9)]
        return "4", ["".join(format(ord(char), "x") for char in book_id)]

    @classmethod
    def calculate_book_str_id(cls, book_id: str) -> str:
        """计算微信读书 Web 阅读页 URL 中使用的加密书籍 ID。"""
        digest = hashlib.md5(book_id.encode("utf-8")).hexdigest()
        code, transformed_ids = cls.transform_id(book_id)
        result = digest[:3] + code + "2" + digest[-2:]

        for index, transformed_id in enumerate(transformed_ids):
            hex_length = format(len(transformed_id), "x").zfill(2)
            result += hex_length + transformed_id
            if index < len(transformed_ids) - 1:
                result += "g"

        if len(result) < 20:
            result += digest[: 20 - len(result)]

        return result + hashlib.md5(result.encode("utf-8")).hexdigest()[:3]

    @classmethod
    def get_reader_url(cls, book_id: str) -> str:
        """根据 bookId 生成微信读书网页阅读地址。"""
        return f"{WEREAD_BASE_URL}/web/reader/{cls.calculate_book_str_id(book_id)}"


def _print_json(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Call current weread.qq.com APIs.")
    parser.add_argument("command", choices=["notebooks", "book", "bookmarks", "reviews", "chapters", "progress", "readinfo", "full", "url"])
    parser.add_argument("--book-id")
    parser.add_argument("--cookie")
    parser.add_argument("--cookie-file")
    args = parser.parse_args()

    cookies = load_cookie_file(args.cookie_file) if args.cookie_file else args.cookie
    client = WeReadApiClient(cookies=cookies)

    if args.command == "notebooks":
        _print_json(client.get_notebooks())
        return

    if not args.book_id:
        parser.error("--book-id is required for this command")

    if args.command == "book":
        _print_json(client.get_book(args.book_id))
    elif args.command == "bookmarks":
        _print_json(client.get_bookmark_list(args.book_id))
    elif args.command == "reviews":
        _print_json(client.get_review_list(args.book_id))
    elif args.command == "chapters":
        _print_json(client.get_chapter_infos(args.book_id))
    elif args.command == "progress":
        _print_json(client.get_progress(args.book_id))
    elif args.command == "readinfo":
        _print_json(client.get_read_info(args.book_id))
    elif args.command == "full":
        _print_json(client.get_full_book_data(args.book_id))
    elif args.command == "url":
        print(client.get_reader_url(args.book_id))


if __name__ == "__main__":
    main()
