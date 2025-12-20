# gcores_radios_repository.py
from typing import Dict
from src.db import DB
import json


class GcoresRadiosRepository:
    def __init__(self, db: DB):
        self.db = db

    # ---------------------------------------------------
    # 创建表
    # ---------------------------------------------------
    def create_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS gcores_radios_data (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                duration INTEGER NOT NULL,
                cover TEXT,
                published_at TEXT NOT NULL,
                likes_count INTEGER NOT NULL,
                comments_count INTEGER NOT NULL,
                category INTEGER NOT NULL,
                userList TEXT,              -- JSON 字符串
                desc TEXT,
                bookmark_count INTEGER NOT NULL,
                content TEXT,
                url TEXT NOT NULL,
                updated_at TEXT
            )
        """)

    # ---------------------------------------------------
    # 插入或更新（SQLite Upsert）
    # ---------------------------------------------------
    def upsert(self, item: Dict):

        row = {
            "id": int(item["id"]),
            "title": item["title"],
            "duration": int(item["duration"]),
            "cover": item.get("cover"),
            "published_at": item["published_at"],
            "likes_count": int(item["likes_count"]),
            "comments_count": int(item["comments_count"]),
            "category": int((item.get("category") or {}).get("id", 99999)),
            "userList": json.dumps(item.get("userList", [])),
            "desc": item.get("desc"),
            "bookmark_count": int(item["bookmarks_count"]),
            "content": item.get("content"),
            "url": item["url"],
            "updated_at": item.get("published_at")
        }

        self.db.execute("""
            INSERT INTO gcores_radios_data (
                id, title, duration, cover, published_at,
                likes_count, comments_count, category, userList,
                desc, bookmark_count, content, url, updated_at
            ) VALUES (
                :id, :title, :duration, :cover, :published_at,
                :likes_count, :comments_count, :category, :userList,
                :desc, :bookmark_count, :content, :url, :updated_at
            )
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                duration = excluded.duration,
                cover = excluded.cover,
                published_at = excluded.published_at,
                likes_count = excluded.likes_count,
                comments_count = excluded.comments_count,
                category = excluded.category,
                userList = excluded.userList,
                desc = excluded.desc,
                bookmark_count = excluded.bookmark_count,
                content = excluded.content,
                url = excluded.url,
                updated_at = excluded.updated_at
        """, row)

    # ---------------------------------------------------
    # 查询全部
    # ---------------------------------------------------
    def get_all(self):
        return self.db.query("SELECT * FROM gcores_radios_data")

    # ---------------------------------------------------
    # 根据 ID 查询
    # ---------------------------------------------------
    def get_by_id(self, id: int):
        row = self.db.query_one("SELECT * FROM gcores_radios_data WHERE id=?", (id,))
        return dict(row) if row else None

    # ---------------------------------------------------
    # 删除
    # ---------------------------------------------------
    def delete(self, id: int):
        self.db.execute("DELETE FROM gcores_radios_data WHERE id=?", (id,))
