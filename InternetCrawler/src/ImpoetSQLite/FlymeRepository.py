# flyme_repository.py
import os
from datetime import datetime
from typing import Dict

from src.config.config import load_config
from src.db.DB import DB
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

        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_flyme_data_createTime ON flyme_data(createTime);
        """)

    # ---------------------------------------------------
    # 插入或更新（SQLite 写法）
    # ---------------------------------------------------
    def upsert(self, row: Dict):

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


    def import_flyme_SQLite(self):
        print("开始导入 flyme 数据 --> SQLite")

        self.create_table()

        # 动态生成正确路径
        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_path = os.path.join(os.path.dirname(current_dir), 'Data_End', 'flyme.json')  # 改为平级目录

        # 读取JSON文件
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        read_num = 0

        # 遍历外层数组
        for item in data:

            # 构建插入数据
            insert_data = (
                item["uuid"],
                item["groupStatus"],
                item["title"],
                item["body"],
                item["topdate"],
                item["firstImg"],
                item["firstImgSrc"],
                item["fileList"],
                json.dumps(item["files"]),
                item["lastUpdate"],
                item["createTime"],
                item["modifyTime"],
                None,
                None,
                None,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )

            read_num +=1

            print(f"已导入{read_num}条")
            self.upsert(insert_data)


# --- 【全新】用于测试“单次运动记录”功能的测试用例 ---
if __name__ == '__main__':
    config = load_config()
    db_instance = DB(config.database.path)

    repository = FlymeRepository(db_instance)
    repository.import_flyme_SQLite()

