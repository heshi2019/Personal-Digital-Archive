# flyme_repository.py
from typing import Dict
from src.db import DB
import json


class FlymeRepository:
    def __init__(self, db: DB):
        self.db = db

    # ---------------------------------------------------
    # 创建表
    # ---------------------------------------------------
    def create_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS flyme_data (
                uuid TEXT PRIMARY KEY,
                `group` TEXT,
                title TEXT,
                body TEXT,
                topdate TEXT,
                firstImg TEXT,
                firstImgSrc TEXT,
                fileList TEXT,
                files TEXT,            -- JSON 字符串
                lastUpdate TEXT,
                createTime TEXT,
                modifyTime TEXT,
                Extend1 TEXT,
                Extend2 TEXT,
                Extend3 TEXT,
                updated_at TEXT
            )
        """)

    # ---------------------------------------------------
    # 插入或更新（SQLite 写法）
    # ---------------------------------------------------
    def upsert(self, item: Dict):
        row = {
            "uuid": item.get("uuid"),
            "group": item.get("groupStatus"),
            "title": item.get("title"),
            "body": item.get("body"),
            "topdate": item.get("topdate"),
            "firstImg": item.get("firstImg"),
            "firstImgSrc": item.get("firstImgSrc"),
            "fileList": item.get("fileList"),
            "files": json.dumps(item.get("files", [])),
            "lastUpdate": item.get("lastUpdate"),
            "createTime": item.get("createTime"),
            "modifyTime": item.get("modifyTime"),
            "Extend1": None,
            "Extend2": None,
            "Extend3": None,
            "updated_at": item.get("lastUpdate")
        }

        self.db.execute("""
            INSERT INTO flyme_data (
                uuid, `group`, title, body, topdate, firstImg, firstImgSrc,
                fileList, files, lastUpdate, createTime, modifyTime,
                Extend1, Extend2, Extend3, updated_at
            ) VALUES (
                :uuid, :group, :title, :body, :topdate, :firstImg, :firstImgSrc,
                :fileList, :files, :lastUpdate, :createTime, :modifyTime,
                :Extend1, :Extend2, :Extend3, :updated_at
            )
            ON CONFLICT(uuid) DO UPDATE SET
                `group` = excluded.`group`,
                title = excluded.title,
                body = excluded.body,
                topdate = excluded.topdate,
                firstImg = excluded.firstImg,
                firstImgSrc = excluded.firstImgSrc,
                fileList = excluded.fileList,
                files = excluded.files,
                lastUpdate = excluded.lastUpdate,
                createTime = excluded.createTime,
                modifyTime = excluded.modifyTime,
                Extend1 = excluded.Extend1,
                Extend2 = excluded.Extend2,
                Extend3 = excluded.Extend3,
                updated_at = excluded.updated_at
        """, row)

    # ---------------------------------------------------
    # 查询全部
    # ---------------------------------------------------
    def get_all(self):
        return self.db.query("SELECT * FROM flyme_data")

    # ---------------------------------------------------
    # 根据 uuid 查询
    # ---------------------------------------------------
    def get_by_uuid(self, uuid: str):
        row = self.db.query_one("SELECT * FROM flyme_data WHERE uuid=?", (uuid,))
        return dict(row) if row else None

    # ---------------------------------------------------
    # 删除
    # ---------------------------------------------------
    def delete(self, uuid: str):
        self.db.execute("DELETE FROM flyme_data WHERE uuid=?", (uuid,))
