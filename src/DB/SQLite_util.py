# db.py
import sqlite3
import os
from contextlib import contextmanager


class SQLite_util:
    """SQLite 连接工具类"""

    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)

        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA encoding = 'UTF-8'")  # 强制SQLite使用UTF-8
        self.conn.row_factory = sqlite3.Row

    def execute(self, sql: str, params=None):
        if params is None:
            params = []
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur

    def query(self, sql: str, params=None):
        if params is None:
            params = []
        return self.conn.execute(sql, params).fetchall()

    def query_one(self, sql: str, params=None):
        if params is None:
            params = []
        return self.conn.execute(sql, params).fetchone()


    # 事务支持
    @contextmanager
    def transaction(self):
        """
        提供一个事务上下文管理器。
        在这个 'with' 块中的所有操作将作为一个整体被提交或回滚。
        """
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()
        try:
            yield cursor  # 将游标对象传递给 with 块
            self.conn.commit()  # 如果 with 块成功，则提交
        except Exception as e:
            self.conn.rollback()  # 如果 with 块中出现异常，则回滚
            raise e

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __del__(self):
        """对象销毁时自动关闭连接"""
        self.close()

    # 上下文管理器支持，用于 with 语句
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """with 语句结束时自动关闭连接"""
        self.close()