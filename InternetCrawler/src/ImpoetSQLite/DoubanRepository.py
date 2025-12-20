# douban_repository.py
from src.db import DB
import json


class DoubanRepository:
    def __init__(self, db: DB):
        self.db = db

    # ---------------------------------------------------
    # 创建表
    # ---------------------------------------------------
    def create_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS douban_data (
                id INTEGER PRIMARY KEY,
                title TEXT,
                type TEXT,
                directors JSON,
                scriptwriter JSON,
                actors JSON,
                count TEXT,
                genres JSON,
                countNum INTEGER,
                countIne REAL,
                pubdate TEXT,
                url TEXT,
                vendor_names JSON,
                cover_url TEXT,
                honor_infos JSON,
                comment_time TEXT,
                comment TEXT,
                comment_score INTEGER,
                updated_at TEXT
            )
        """)

    # ---------------------------------------------------
    # 插入或更新
    # ---------------------------------------------------
    def upsert(self, row: dict):
        self.db.execute("""
            INSERT INTO douban_data (
                id, title, type, directors, scriptwriter, actors, count, genres, 
                countNum, countIne, pubdate, url, vendor_names, cover_url, honor_infos,
                comment_time, comment, comment_score, updated_at
            ) VALUES (
                :id, :title, :type, :directors, :scriptwriter, :actors, :count, :genres, 
                :countNum, :countIne, :pubdate, :url, :vendor_names, :cover_url, :honor_infos,
                :comment_time, :comment, :comment_score, :updated_at
            )
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title,
                type=excluded.type,
                directors=excluded.directors,
                scriptwriter=excluded.scriptwriter,
                actors=excluded.actors,
                count=excluded.count,
                genres=excluded.genres,
                countNum=excluded.countNum,
                countIne=excluded.countIne,
                pubdate=excluded.pubdate,
                url=excluded.url,
                vendor_names=excluded.vendor_names,
                cover_url=excluded.cover_url,
                honor_infos=excluded.honor_infos,
                comment_time=excluded.comment_time,
                comment=excluded.comment,
                comment_score=excluded.comment_score,
                updated_at=excluded.updated_at
        """, row)
