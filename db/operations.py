import sqlite3
from typing import List, Dict
import os
from . import models

class DB:
    def __init__(self, path: str):
        # 创建目录，exist_ok=True如已经存在则不抛出异常，dirname()提取一个包含路径和文件名中的路径部分
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # 创建连接，check_same_thread=False：允许跨线程使用连接（默认只允许创建连接的线程访问）
        self.conn = sqlite3.connect(path, check_same_thread=False)
        # row_factory：设置查询结果的返回格式
        # sqlite3.Row：使查询结果以 Row 对象形式返回
        self.conn.row_factory = sqlite3.Row

        # 创建表
        models.create_tables(self.conn)

    # 插入或更新文件元数据
    def insert_or_update_file(self, file_meta: Dict):
        self.conn.execute('''
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
        ''', file_meta)
        self.conn.commit()

    # 获取所有文件记录
    def get_files(self):
        return self.conn.execute("SELECT * FROM files").fetchall()

    # 根据ID获取单个文件记录
    def get_file_by_id(self, id: str):
        row = self.conn.execute("SELECT * FROM files WHERE id=?", (id,)).fetchone()
        return dict(row) if row else None
