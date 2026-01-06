# files_repository.py
from typing import Dict
from src.DB import SQLite_util


class FilesMdRepository:
    def __init__(self, db: SQLite_util):
        self.db = db

    # ----------------------------------------------------------------------
    # 创建表
    # ----------------------------------------------------------------------
    def create_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS files_md (
                id TEXT PRIMARY KEY,        -- 文件ID，主键（hash值）
                file_name TEXT,             -- 文件名
                type TEXT,                  -- 文件类型
                imageDict TEXT,              -- 图片列表，一个字符串，一般为图片，每个文件名之间用逗号隔开
                path TEXT NOT NULL,         -- 文件真实路径
                hash TEXT,                  -- 文件哈希值
                size INTEGER,               -- 文件大小（字节）
                summary TEXT,               -- 摘要
                created_at TEXT,            -- 创建时间
                modified_at TEXT,           -- 修改时间
                Extend1 TEXT,               -- 扩展字段1
                Extend2 TEXT,               -- 扩展字段2
                Extend3 TEXT                -- 扩展字段3
            )
        """)

    # ----------------------------------------------------------------------
    # 插入或更新
    # ----------------------------------------------------------------------
    def insert_upsert(self, file_meta: Dict):
        self.db.execute("""
            INSERT INTO files_md (id, file_name, type, imageDict, path, hash, size, summary, created_at, modified_at, Extend1, Extend2, Extend3)
            VALUES (:id, :file_name, :type, :imageDict, :path, :hash, :size, :summary, :created_at, :modified_at, :Extend1, :Extend2, :Extend3)
            ON CONFLICT(id) DO UPDATE SET
                path=excluded.path,
                type=excluded.type,
                file_name=excluded.file_name,
                imageDict=excluded.imageDict,
                hash=excluded.hash,
                size=excluded.size,
                summary=excluded.summary,
                created_at=excluded.created_at,
                modified_at=excluded.modified_at,
                Extend1=excluded.Extend1,
                Extend2=excluded.Extend2,
                Extend3=excluded.Extend3
        """, file_meta)

    def upsert(self, file_meta: Dict):
        # 1. 检查是否传入了 id，没有 id 无法定位记录
        if "id" not in file_meta:
            raise ValueError("必须提供 'id' 才能进行更新操作")

        # 2. 提取除 id 以外的所有需要更新的键
        # 假设传入 {"id": 1, "summary": "xxx"}，keys 逻辑会处理成 ["summary = :summary"]
        update_keys = [f"{k} = :{k}" for k in file_meta.keys() if k != 'id']

        # 3. 如果字典里只有 id，没有其他字段，则无需更新
        if not update_keys:
            return

            # 4. 拼接 SQL 语句
        set_clause = ", ".join(update_keys)
        sql = f"UPDATE files_md SET {set_clause} WHERE id = :id"

        # 5. 执行更新
        # 这里的 file_meta 字典直接作为命名参数传入
        self.db.execute(sql, file_meta)



    # ----------------------------------------------------------------------
    # 查询所有
    # ----------------------------------------------------------------------
    def get_all(self):
        return self.db.query("SELECT * FROM files_md")

