import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

SRC_ROOT = Path(__file__).resolve().parents[2]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from Script.Script_API.weread_api_client import WeReadApiClient, WeReadAuthError, WeReadRequestError
from Script.Script_API.weread_cookie import ensure_valid_cookie

DEFAULT_COOKIE_FILE = SRC_ROOT / "config" / "weread_cookie.json"
DATA_STAR_WEREAD_DIR = SRC_ROOT / "data" / "Data_Star" / "weread"
DATA_END_WEREAD_FILE = SRC_ROOT / "data" / "Data_End" / "weread_1.json"
BOOK_ALL_DATA_NAME = "book_allData"


def log(message: str) -> None:
    print(f"[WeRead] {message}")


def get_default_output_dir() -> Path:
    return DATA_STAR_WEREAD_DIR


def save_text(output_dir: Path, interface_name: str, data: Any) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{interface_name}.txt"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if isinstance(data, str):
        content = data
    else:
        content = json.dumps(data, ensure_ascii=False, indent=2)

    output_path.write_text(content, encoding="utf-8")
    return output_path


def save_book_all_data(data: Any) -> Path:
    DATA_END_WEREAD_FILE.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, str):
        content = data
    else:
        content = json.dumps(data, ensure_ascii=False, indent=2)

    DATA_END_WEREAD_FILE.write_text(content, encoding="utf-8")
    return DATA_END_WEREAD_FILE


def load_json_file(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def safe_path_name(value: Any) -> str:
    safe = "".join(char if char.isalnum() or char in "-_" else "_" for char in str(value))
    return safe or "unknown"


def format_timestamp(timestamp: Any) -> Optional[str]:
    if not timestamp:
        return None

    try:
        return datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
    except (OSError, TypeError, ValueError):
        return None


def format_duration(seconds: Any) -> Optional[str]:
    if seconds is None:
        return None

    try:
        total_seconds = int(seconds)
    except (TypeError, ValueError):
        return None

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    remaining_seconds = total_seconds % 60
    return f"{hours}:{minutes:02d}:{remaining_seconds:02d}"


def chapter_key(chapter_uid: Any) -> str:
    try:
        return f"{float(chapter_uid):.1f}"
    except (TypeError, ValueError):
        return str(chapter_uid)


def first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def sort_chapter_uid(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("inf")


def extract_first_book_id(notebooks: Any) -> Optional[str]:
    if not isinstance(notebooks, dict):
        return None

    books = notebooks.get("books")
    if not isinstance(books, list) or not books:
        return None

    first_book = books[0]
    if not isinstance(first_book, dict):
        return None

    nested_book = first_book.get("book")
    if isinstance(nested_book, dict):
        book_id = nested_book.get("bookId")
    else:
        book_id = first_book.get("bookId")

    return str(book_id) if book_id else None


def extract_book_ids(notebooks: Any) -> List[str]:
    if not isinstance(notebooks, dict):
        return []

    books = notebooks.get("books")
    if not isinstance(books, list):
        return []

    book_ids: List[str] = []
    seen = set()
    for item in books:
        if not isinstance(item, dict):
            continue

        nested_book = item.get("book")
        if isinstance(nested_book, dict):
            book_id = nested_book.get("bookId")
        else:
            book_id = item.get("bookId")

        if book_id is None:
            continue

        normalized_book_id = str(book_id)
        if normalized_book_id not in seen:
            book_ids.append(normalized_book_id)
            seen.add(normalized_book_id)

    return book_ids


def build_chapter_catalog(chapters: Any) -> Dict[str, str]:
    if not isinstance(chapters, dict):
        return {}

    data = chapters.get("data")
    if not isinstance(data, list) or not data:
        return {}

    updated = data[0].get("updated") if isinstance(data[0], dict) else None
    if not isinstance(updated, list):
        return {}

    catalog: Dict[str, str] = {}
    for chapter in updated:
        if not isinstance(chapter, dict):
            continue

        uid = chapter.get("chapterUid")
        title = chapter.get("title")
        if uid is not None and title is not None:
            catalog[chapter_key(uid)] = str(title)

    return catalog


def build_bookmark_chapter_titles(bookmarks: Any) -> Dict[str, str]:
    if not isinstance(bookmarks, dict) or not isinstance(bookmarks.get("chapters"), list):
        return {}

    titles: Dict[str, str] = {}
    for chapter in bookmarks["chapters"]:
        if not isinstance(chapter, dict):
            continue

        uid = chapter.get("chapterUid")
        title = chapter.get("title")
        if uid is not None and title is not None:
            titles[chapter_key(uid)] = str(title)

    return titles


def build_review_content_by_range(reviews: Any) -> Dict[str, str]:
    if not isinstance(reviews, dict) or not isinstance(reviews.get("reviews"), list):
        return {}

    content_by_range: Dict[str, str] = {}
    for review_item in reviews["reviews"]:
        if not isinstance(review_item, dict):
            continue

        review = review_item.get("review")
        if not isinstance(review, dict):
            continue

        review_range = review.get("range")
        if review.get("type") == 1 and review_range:
            content_by_range[str(review_range)] = str(review.get("content") or "")

    return content_by_range


def build_review_items(reviews: Any) -> List[Dict[str, Any]]:
    if not isinstance(reviews, dict) or not isinstance(reviews.get("reviews"), list):
        return []

    review_items: List[Dict[str, Any]] = []
    for review_item in reviews["reviews"]:
        if not isinstance(review_item, dict):
            continue

        review = review_item.get("review")
        if not isinstance(review, dict):
            continue

        review_items.append(review)

    return review_items


def build_mark_text(bookmarks: Any, reviews: Any, chapters: Any) -> List[Dict[str, Any]]:
    chapter_catalog = build_chapter_catalog(chapters)
    bookmark_chapter_titles = build_bookmark_chapter_titles(bookmarks)
    review_content_by_range = build_review_content_by_range(reviews)
    matched_review_ranges: Set[str] = set()

    mark_text: List[Dict[str, Any]] = []
    if isinstance(bookmarks, dict) and isinstance(bookmarks.get("updated"), list):
        for bookmark in bookmarks["updated"]:
            if not isinstance(bookmark, dict):
                continue

            uid = bookmark.get("chapterUid")
            uid_key = chapter_key(uid)
            bookmark_range = bookmark.get("range")
            range_key = str(bookmark_range) if bookmark_range else ""
            content = review_content_by_range.get(range_key, "") if range_key else ""
            if range_key and range_key in review_content_by_range:
                matched_review_ranges.add(range_key)

            mark_text.append(
                {
                    "chapterUid": uid,
                    "createTime": format_timestamp(bookmark.get("createTime")),
                    "markText": bookmark.get("markText") or "",
                    "content": content,
                    "chapterTitle": chapter_catalog.get(uid_key) or bookmark_chapter_titles.get(uid_key) or ""
                }
            )

    for review in build_review_items(reviews):
        review_range = review.get("range")
        range_key = str(review_range) if review_range else ""
        if range_key and range_key in matched_review_ranges:
            continue

        content = review.get("content") or review.get("mdContent") or ""
        if not content:
            continue

        uid = review.get("chapterUid")
        uid_key = chapter_key(uid)
        mark_text.append(
            {
                "chapterUid": uid,
                "createTime": format_timestamp(review.get("createTime")),
                "markText": review.get("abstract") or "",
                "content": content,
                "chapterTitle": review.get("chapterTitle")
                or review.get("chapterName")
                or chapter_catalog.get(uid_key)
                or bookmark_chapter_titles.get(uid_key)
                or ""
            }
        )

    return sorted(mark_text, key=lambda item: sort_chapter_uid(item.get("chapterUid")))


def get_classification(book: Any) -> Any:
    if not isinstance(book, dict):
        return None

    category = book.get("category")
    if category:
        return category

    categories = book.get("categories")
    if isinstance(categories, list) and categories:
        first_category = categories[0]
        if isinstance(first_category, dict):
            return first_category.get("title")

    return None


def build_book_all_data(results: Dict[str, Any]) -> Dict[str, Any]:
    book = results.get("book")
    bookmarks = results.get("bookmarks")
    reviews = results.get("reviews")
    chapters = results.get("chapters")
    progress = results.get("progress")
    readinfo = results.get("readinfo")
    reader_url = results.get("reader_url")

    book_data = book if isinstance(book, dict) else {}
    progress_book = progress.get("book", {}) if isinstance(progress, dict) else {}
    readinfo_data = readinfo if isinstance(readinfo, dict) else {}
    read_detail = readinfo_data.get("readDetail", {})
    if not isinstance(read_detail, dict):
        read_detail = {}

    progress_value = first_not_none(progress_book.get("progress"), readinfo_data.get("readingProgress"))
    title = str(book_data.get("title") or "未知书名")
    finish_reading = book_data.get("finishReading") == 1
    read_sign = "已读完" if finish_reading or progress_value == 100 else "在读"

    result = {
        title: {
            "bookId": book_data.get("bookId"),
            "title": book_data.get("title"),
            "classification": get_classification(book_data),
            "cover": book_data.get("cover"),
            "name": book_data.get("author"),
            "isbn": book_data.get("isbn"),
            "readSign": read_sign,
            "briefIntroduction": book_data.get("intro"),
            "Progress": progress_value,
            "ReadDay": first_not_none(
                readinfo_data.get("totalReadDay"), read_detail.get("totalReadDay")
            ),
            "ReadDayTime": format_duration(
                first_not_none(progress_book.get("readingTime"), readinfo_data.get("readingTime"))
            ),
            "StartDay": format_timestamp(
                first_not_none(
                    progress_book.get("startReadingTime"), read_detail.get("beginReadingDate")
                )
            ),
            "LastDay": format_timestamp(
                first_not_none(
                    progress_book.get("finishTime"),
                    readinfo_data.get("finishedDate"),
                    read_detail.get("lastReadingDate")
                )
            ),
            "LatestDay": format_timestamp(read_detail.get("longestReadingDate")),
            "ReadUrl": reader_url,
            "markText": build_mark_text(bookmarks, reviews, chapters),
            "1000000": build_chapter_catalog(chapters)
        }
    }

    return result


def merge_book_all_data(target: Dict[str, Any], source: Dict[str, Any]) -> None:
    for title, book_data in source.items():
        if title not in target:
            target[title] = book_data
            continue

        book_id = book_data.get("bookId") if isinstance(book_data, dict) else None
        unique_title = f"{title}_{book_id}" if book_id else title
        suffix = 2
        while unique_title in target:
            unique_title = f"{title}_{book_id}_{suffix}" if book_id else f"{title}_{suffix}"
            suffix += 1

        target[unique_title] = book_data


def build_book_key_by_id(book_all_data: Dict[str, Any]) -> Dict[str, str]:
    key_by_id: Dict[str, str] = {}
    for title_key, book_data in book_all_data.items():
        if not isinstance(book_data, dict):
            continue

        book_id = book_data.get("bookId")
        if book_id is not None:
            key_by_id[str(book_id)] = title_key

    return key_by_id


def mark_identity(mark: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        str(mark.get("chapterUid")),
        str(mark.get("createTime")),
        str(mark.get("markText"))
    )


def merge_mark_text_incremental(existing: Any, incoming: Any) -> List[Dict[str, Any]]:
    existing_marks = existing if isinstance(existing, list) else []
    incoming_marks = incoming if isinstance(incoming, list) else []
    merged: List[Dict[str, Any]] = []
    seen: Set[Tuple[str, str, str]] = set()

    for mark in existing_marks:
        if not isinstance(mark, dict):
            continue

        merged.append(mark)
        seen.add(mark_identity(mark))

    for mark in incoming_marks:
        if not isinstance(mark, dict):
            continue

        identity = mark_identity(mark)
        if identity in seen:
            continue

        merged.append(mark)
        seen.add(identity)

    return sorted(merged, key=lambda item: sort_chapter_uid(item.get("chapterUid")))


def merge_catalog_incremental(existing: Any, incoming: Any) -> Dict[str, str]:
    merged: Dict[str, str] = dict(existing) if isinstance(existing, dict) else {}
    if not isinstance(incoming, dict):
        return merged

    for chapter_uid, title in incoming.items():
        if chapter_uid not in merged:
            merged[chapter_uid] = title

    return merged


def merge_existing_book_incremental(existing_book: Dict[str, Any], incoming_book: Dict[str, Any]) -> None:
    for field_name, value in incoming_book.items():
        if field_name == "markText":
            existing_book["markText"] = merge_mark_text_incremental(
                existing_book.get("markText"), value
            )
            continue

        if field_name == "1000000":
            existing_book["1000000"] = merge_catalog_incremental(
                existing_book.get("1000000"), value
            )
            continue

        if field_name not in existing_book or existing_book[field_name] is None:
            existing_book[field_name] = value


def merge_book_all_data_incremental(target: Dict[str, Any], source: Dict[str, Any]) -> None:
    key_by_id = build_book_key_by_id(target)
    for title, incoming_book in source.items():
        if not isinstance(incoming_book, dict):
            continue

        book_id = incoming_book.get("bookId")
        existing_key = key_by_id.get(str(book_id)) if book_id is not None else None
        if existing_key and isinstance(target.get(existing_key), dict):
            merge_existing_book_incremental(target[existing_key], incoming_book)
            continue

        merge_book_all_data(target, {title: incoming_book})
        if book_id is not None:
            key_by_id[str(book_id)] = title


def run_book_api_calls(client: WeReadApiClient, book_id: str) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    target_book_id = str(book_id)
    results["reader_url"] = client.get_reader_url(target_book_id)
    results["book"] = client.get_book(target_book_id)
    book_title = results["book"].get("title") if isinstance(results["book"], dict) else None
    log(f"正在同步《{book_title or target_book_id}》的划线、笔记、目录和阅读进度。")
    results["bookmarks"] = client.get_bookmark_list(target_book_id)
    results["reviews"] = client.get_review_list(target_book_id)
    results["chapters"] = client.get_chapter_infos(target_book_id)
    results["progress"] = client.get_progress(target_book_id)
    results["readinfo"] = client.get_read_info(target_book_id)
    results["book_allData"] = build_book_all_data(results)
    log(f"《{book_title or target_book_id}》同步完成。")

    return results


def run_weread_api_calls(client: WeReadApiClient, book_id: Optional[str] = None) -> Dict[str, Any]:
    results: Dict[str, Any] = {}

    notebooks = client.get_notebooks()
    results["notebooks"] = notebooks

    if book_id:
        log(f"准备同步指定书籍：{book_id}。")
        results.update(run_book_api_calls(client, book_id))
        return results

    all_book_data: Dict[str, Any] = {}
    book_errors: Dict[str, str] = {}
    book_ids = extract_book_ids(notebooks)
    log(f"读取到 {len(book_ids)} 本有笔记或划线的书，开始逐本同步。")
    for index, current_book_id in enumerate(book_ids, start=1):
        log(f"进度 {index}/{len(book_ids)}，书籍 ID：{current_book_id}。")
        try:
            book_results = run_book_api_calls(client, current_book_id)
        except WeReadRequestError as exc:
            book_errors[current_book_id] = str(exc)
            log(f"书籍 ID {current_book_id} 同步失败：{exc}")
            continue

        results[f"raw/{safe_path_name(current_book_id)}/reader_url"] = book_results["reader_url"]
        results[f"raw/{safe_path_name(current_book_id)}/book"] = book_results["book"]
        results[f"raw/{safe_path_name(current_book_id)}/bookmarks"] = book_results["bookmarks"]
        results[f"raw/{safe_path_name(current_book_id)}/reviews"] = book_results["reviews"]
        results[f"raw/{safe_path_name(current_book_id)}/chapters"] = book_results["chapters"]
        results[f"raw/{safe_path_name(current_book_id)}/progress"] = book_results["progress"]
        results[f"raw/{safe_path_name(current_book_id)}/readinfo"] = book_results["readinfo"]
        merge_book_all_data(all_book_data, book_results["book_allData"])

    results["book_allData"] = all_book_data
    if book_errors:
        results["errors"] = book_errors

    return results


def save_results(output_dir: Path, results: Dict[str, Any]) -> Dict[str, Path]:
    saved_files: Dict[str, Path] = {}
    for interface_name, data in results.items():
        if interface_name == BOOK_ALL_DATA_NAME:
            saved_files[interface_name] = save_book_all_data(data)
        else:
            saved_files[interface_name] = save_text(output_dir, interface_name, data)
    return saved_files


def create_client(cookie_file: str) -> WeReadApiClient:
    log(f"正在检查微信读书 Cookie：{Path(cookie_file)}")
    cookies = ensure_valid_cookie(path=cookie_file, allow_login=True)
    if not cookies:
        log("登录未成功：Cookie 不可用，也没有完成扫码登录。")
        raise RuntimeError("Cookie 不可用，并且未能完成扫码登录。")

    log("登录状态确认成功，Cookie 可用。")
    return WeReadApiClient(cookies=cookies)


def full_sync(
    cookie_file: str = DEFAULT_COOKIE_FILE,
    output_dir: Optional[Path] = None,
    book_id: Optional[str] = None
) -> Dict[str, Path]:
    """
    全量模式：重新请求并重建所有数据。
    不传 book_id 时遍历 notebooks 中的全部 bookId。
    """
    target_output_dir = output_dir or get_default_output_dir()

    try:
        client = create_client(cookie_file)
        results = run_weread_api_calls(client, book_id=book_id)
        return save_results(target_output_dir, results)
    except WeReadAuthError as exc:
        raise RuntimeError("Cookie 已失效，请重新扫码登录。") from exc
    except WeReadRequestError as exc:
        raise RuntimeError(f"微信读书接口请求失败: {exc}") from exc


def incremental_sync(
    cookie_file: str = DEFAULT_COOKIE_FILE,
    output_dir: Optional[Path] = None
) -> Dict[str, Path]:
    """
    增量模式：只增加新书、新划线/笔记和新目录项。
    不删除已有书籍，不删除已有划线/笔记，也不覆盖已有字段值。
    """
    target_output_dir = output_dir or get_default_output_dir()
    existing_path = DATA_END_WEREAD_FILE
    existing_book_all_data: Dict[str, Any] = {}
    if existing_path.exists():
        loaded = load_json_file(existing_path)
        if isinstance(loaded, dict):
            existing_book_all_data = loaded

    try:
        client = create_client(cookie_file)
        notebooks = client.get_notebooks()
        book_ids = extract_book_ids(notebooks)
        log(f"读取到 {len(book_ids)} 本有笔记或划线的书，开始增量同步。")
        saved_files: Dict[str, Path] = {
            "notebooks": save_text(target_output_dir, "notebooks", notebooks)
        }

        book_errors: Dict[str, str] = {}
        for index, current_book_id in enumerate(book_ids, start=1):
            log(f"进度 {index}/{len(book_ids)}，书籍 ID：{current_book_id}。")
            try:
                book_results = run_book_api_calls(client, current_book_id)
            except WeReadRequestError as exc:
                book_errors[current_book_id] = str(exc)
                log(f"书籍 ID {current_book_id} 同步失败：{exc}")
                continue

            safe_book_id = safe_path_name(current_book_id)
            saved_files[f"raw/{safe_book_id}/reader_url"] = save_text(
                target_output_dir, f"raw/{safe_book_id}/reader_url", book_results["reader_url"]
            )
            saved_files[f"raw/{safe_book_id}/book"] = save_text(
                target_output_dir, f"raw/{safe_book_id}/book", book_results["book"]
            )
            saved_files[f"raw/{safe_book_id}/bookmarks"] = save_text(
                target_output_dir, f"raw/{safe_book_id}/bookmarks", book_results["bookmarks"]
            )
            saved_files[f"raw/{safe_book_id}/reviews"] = save_text(
                target_output_dir, f"raw/{safe_book_id}/reviews", book_results["reviews"]
            )
            saved_files[f"raw/{safe_book_id}/chapters"] = save_text(
                target_output_dir, f"raw/{safe_book_id}/chapters", book_results["chapters"]
            )
            saved_files[f"raw/{safe_book_id}/progress"] = save_text(
                target_output_dir, f"raw/{safe_book_id}/progress", book_results["progress"]
            )
            saved_files[f"raw/{safe_book_id}/readinfo"] = save_text(
                target_output_dir, f"raw/{safe_book_id}/readinfo", book_results["readinfo"]
            )
            merge_book_all_data_incremental(existing_book_all_data, book_results["book_allData"])

        saved_files[BOOK_ALL_DATA_NAME] = save_book_all_data(existing_book_all_data)
        if book_errors:
            saved_files["errors"] = save_text(target_output_dir, "errors", book_errors)

        return saved_files
    except WeReadAuthError as exc:
        raise RuntimeError("Cookie 已失效，请重新扫码登录。") from exc
    except WeReadRequestError as exc:
        raise RuntimeError(f"微信读书接口请求失败: {exc}") from exc


def main(
    cookie_file: str = DEFAULT_COOKIE_FILE,
    output_dir: Optional[Path] = None,
    book_id: Optional[str] = None
) -> Dict[str, Path]:
    return full_sync(cookie_file=cookie_file, output_dir=output_dir, book_id=book_id)


if __name__ == "__main__":
    files = main()
    print("接口返回值已保存：")
    for name, path in files.items():
        print(f"{name}: {path}")
