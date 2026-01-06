# files_repository.py
from typing import Dict
from src.DB import SQLite_util


class FilesPhoneRepository:
    def __init__(self, db: SQLite_util):
        self.db = db

    # ----------------------------------------------------------------------
    # 创建表
    # ----------------------------------------------------------------------
    def create_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS files_phone (
                id TEXT PRIMARY KEY,        -- 文件ID，主键（hash值）
                file_name TEXT,             -- 文件名
                type TEXT,                  -- 文件类型
                path TEXT NOT NULL,         -- 文件真实路径
                hash TEXT,                  -- 文件哈希值
                size INTEGER,               -- 文件大小（字节）
                thumbnail_path TEXT,        -- 缩略图路径
                created_at TEXT,            -- 创建时间
                modified_at TEXT            -- 修改时间
            )
        """)

    # ----------------------------------------------------------------------
    # 插入或更新
    # ----------------------------------------------------------------------
    def upsert(self, file_meta: Dict):
        self.db.execute("""
            INSERT INTO files_phone (id,type,file_name, path,created_at, modified_at, hash, size, thumbnail_path)
            VALUES (:id,:type,:file_name, :path,:created_at, :modified_at, :hash, :size, :thumbnail_path)
            ON CONFLICT(id) DO UPDATE SET
                id=excluded.id,
                path=excluded.path,
                type=excluded.type,
                file_name=excluded.file_name,
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
        return self.db.query("SELECT * FROM files_phone")

    # ----------------------------------------------------------------------
    # 根据 ID 查询
    # ----------------------------------------------------------------------
    def get_by_id(self, id: str):
        row = self.db.query_one("SELECT * FROM files_phone WHERE id=?", (id,))
        return dict(row) if row else None

    # ----------------------------------------------------------------------
    # 删除一条记录
    # ----------------------------------------------------------------------
    def delete(self, id: str):
        self.db.execute("DELETE FROM files_phone WHERE id=?", (id,))
