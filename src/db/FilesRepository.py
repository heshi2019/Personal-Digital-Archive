# files_repository.py
from typing import Dict
from src.db import DB


class FilesRepository:
    def __init__(self, db: DB):
        self.db = db

    # ----------------------------------------------------------------------
    # 创建表
    # ----------------------------------------------------------------------
    def create_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                type TEXT,
                created_at TEXT,
                modified_at TEXT,
                hash TEXT,
                size INTEGER,
                thumbnail_path TEXT
            )
        """)

    # ----------------------------------------------------------------------
    # 插入或更新
    # ----------------------------------------------------------------------
    def upsert(self, file_meta: Dict):
        self.db.execute("""
            INSERT INTO files (id, path, type, created_at, modified_at, hash, size, thumbnail_path)
            VALUES (:id, :path, :type, :created_at, :modified_at, :hash, :size, :thumbnail_path)
            ON CONFLICT(id) DO UPDATE SET
                path=excluded.path,
                type=excluded.type,
                created_at=excluded.created_at,
                modified_at=excluded.modified_at,
                hash=excluded.hash,
                size=excluded.size,
                thumbnail_path=excluded.thumbnail_path
        """, file_meta)

    # ----------------------------------------------------------------------
    # 查询所有
    # ----------------------------------------------------------------------
    def get_all(self):
        return self.db.query("SELECT * FROM files")

    # ----------------------------------------------------------------------
    # 根据 ID 查询
    # ----------------------------------------------------------------------
    def get_by_id(self, id: str):
        row = self.db.query_one("SELECT * FROM files WHERE id=?", (id,))
        return dict(row) if row else None

    # ----------------------------------------------------------------------
    # 删除一条记录
    # ----------------------------------------------------------------------
    def delete(self, id: str):
        self.db.execute("DELETE FROM files WHERE id=?", (id,))
