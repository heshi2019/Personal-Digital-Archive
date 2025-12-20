# gcores_categories_repository.py
from typing import Dict
from src.db import DB


class GcoresCategoriesRepository:
    def __init__(self, db: DB):
        self.db = db

    # ---------------------------------------------------
    # 创建表
    # ---------------------------------------------------
    def create_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS gcores_categories_data (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                image TEXT,
                count INTEGER NOT NULL,
                url TEXT,
                updated_at TEXT
            )
        """)

    # ---------------------------------------------------
    # 插入或更新
    # ---------------------------------------------------
    def upsert(self, item: Dict):
        row = {
            "id": int(item["id"]),
            "name": item["name"],
            "image": item.get("image"),
            "count": item.get("count", 0),
            "url": item.get("url"),
            "updated_at": item.get("updated_at")
        }

        self.db.execute("""
            INSERT INTO gcores_categories_data (
                id, name, image, count, url, updated_at
            ) VALUES (
                :id, :name, :image, :count, :url, :updated_at
            )
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                image = excluded.image,
                count = excluded.count,
                url = excluded.url,
                updated_at = excluded.updated_at
        """, row)

    # ---------------------------------------------------
    # 查询全部
    # ---------------------------------------------------
    def get_all(self):
        return self.db.query("SELECT * FROM gcores_categories_data")

    # ---------------------------------------------------
    # 根据 ID 查询
    # ---------------------------------------------------
    def get_by_id(self, id: int):
        row = self.db.query_one("SELECT * FROM gcores_categories_data WHERE id=?", (id,))
        return dict(row) if row else None

    # ---------------------------------------------------
    # 删除
    # ---------------------------------------------------
    def delete(self, id: int):
        self.db.execute("DELETE FROM gcores_categories_data WHERE id=?", (id,))
