# flomo_repository.py
from src.db import DB
import json


class FlomoRepository:
    def __init__(self, db: DB):
        self.db = db

    # ---------------------------------------------------
    # 创建表
    # ---------------------------------------------------
    def create_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS flomo_data (
                dataTime TEXT PRIMARY KEY,
                text TEXT,
                images JSON,
                extend1 TEXT,
                extend2 TEXT,
                extend3 TEXT,
                updated_at TEXT
            )
        """)

    # ---------------------------------------------------
    # 插入或更新
    # ---------------------------------------------------
    def upsert(self, row: dict):
        self.db.execute("""
            INSERT INTO flomo_data (
                dataTime, text, images, extend1, extend2, extend3, updated_at
            ) VALUES (
                :dataTime, :text, :images, :extend1, :extend2, :extend3, :updated_at
            )
            ON CONFLICT(dataTime) DO UPDATE SET
                text=excluded.text,
                images=excluded.images,
                extend1=excluded.extend1,
                extend2=excluded.extend2,
                extend3=excluded.extend3,
                updated_at=excluded.updated_at
        """, row)
