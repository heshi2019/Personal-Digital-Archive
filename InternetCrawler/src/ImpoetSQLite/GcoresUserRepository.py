# gcores_user_repository.py
from typing import Dict
from src.db import DB


class GcoresUserRepository:
    def __init__(self, db: DB):
        self.db = db

    # ---------------------------------------------------
    # 创建表
    # ---------------------------------------------------
    def create_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS gcores_user_data (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                image TEXT,
                url TEXT,
                location TEXT,
                intro TEXT,
                followersCount TEXT,
                followeesCount TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT
            )
        """)

    # ---------------------------------------------------
    # 插入 / 更新
    # ---------------------------------------------------
    def upsert(self, item: Dict):
        row = {
            "id": int(item["id"]),
            "name": item["name"],
            "image": item.get("image"),
            "url": item.get("url"),
            "location": item.get("location"),
            "intro": item.get("intro"),
            "followersCount": item.get("followersCount"),
            "followeesCount": item.get("followeesCount"),
            "created_at": item.get("created_at"),
            "updated_at": item.get("created_at")
        }

        self.db.execute("""
            INSERT INTO gcores_user_data (
                id, name, image, url, location, intro, followersCount, followeesCount, created_at, updated_at
            ) VALUES (
                :id, :name, :image, :url, :location, :intro, :followersCount, :followeesCount, :created_at, :updated_at
            )
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                image = excluded.image,
                url = excluded.url,
                location = excluded.location,
                intro = excluded.intro,
                followersCount = excluded.followersCount,
                followeesCount = excluded.followeesCount,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
        """, row)

    # ---------------------------------------------------
    # 查询所有
    # ---------------------------------------------------
    def get_all(self):
        return self.db.query("SELECT * FROM gcores_user_data")

    # ---------------------------------------------------
    # 根据 ID 查询
    # ---------------------------------------------------
    def get_by_id(self, id: int):
        row = self.db.query_one("SELECT * FROM gcores_user_data WHERE id=?", (id,))
        return dict(row) if row else None

    # ---------------------------------------------------
    # 删除
    # ---------------------------------------------------
    def delete(self, id: int):
        self.db.execute("DELETE FROM gcores_user_data WHERE id=?", (id,))
