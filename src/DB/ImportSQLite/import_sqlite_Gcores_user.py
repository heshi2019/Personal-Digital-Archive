# gcores_user_repository.py
import json
import os
from datetime import datetime
from typing import Dict

from src.DB.SQLite_util import SQLite_util
from src.config.configClass import app_config


class GcoresUserRepository:
    def __init__(self, db: SQLite_util):
        self.db = db

    # ---------------------------------------------------
    # 创建表
    # ---------------------------------------------------
    def create_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS gcores_user_data (
                id INTEGER PRIMARY KEY,     -- 用户ID，这个ID在机核的库中是递增的
                name TEXT NOT NULL,         -- 用户名称
                image TEXT,                 -- 用户头像URL
                url TEXT,                   -- 用户URL
                location TEXT,              -- 用户地点
                intro TEXT,                 -- 用户个人签名
                followersCount INTEGER,        -- 被关注数
                followeesCount INTEGER,        -- 关注数
                created_at TEXT NOT NULL,   -- 注册时间
                updated_at TEXT             -- 更新时间
            )
        """)
        self.db.execute("""
             CREATE INDEX IF NOT EXISTS idx_gcores_user_data_id ON gcores_user_data(id);
         """)

        self.db.execute("""
             CREATE INDEX IF NOT EXISTS idx_gcores_user_data_creatTime ON gcores_user_data(created_at);
         """)

    # ---------------------------------------------------
    # 插入 / 更新
    # ---------------------------------------------------
    def upsert(self, row: Dict):

        self.db.execute("""
            INSERT INTO gcores_user_data (
                id, name, image, url, location, intro, followersCount, followeesCount, created_at, updated_at
            ) VALUES (
                :id, :name, :image, :url, :location, :intro, :followersCount, :followeesCount, :created_at, :updated_at
            )
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                image = excluded.image,
                url = excluded.url,
                location = excluded.location,
                intro = excluded.intro,
                followersCount = excluded.followersCount,
                followeesCount = excluded.followeesCount,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
        """, row)

    # ---------------------------------------------------
    # 查询所有
    # ---------------------------------------------------
    def get_all(self):
        return self.db.query("SELECT * FROM gcores_user_data")

    # ---------------------------------------------------
    # 根据 ID 查询
    # ---------------------------------------------------
    def get_by_id(self, id: int):
        row = self.db.query_one("SELECT * FROM gcores_user_data WHERE id=?", (id,))
        return dict(row) if row else None

    # ---------------------------------------------------
    # 删除
    # ---------------------------------------------------
    def delete(self, id: int):
        self.db.execute("DELETE FROM gcores_user_data WHERE id=?", (id,))





    def import_GcoresUser_SQLite(self):
        print("开始导入 机核用户 数据 --> SQLite")

        self.create_table()

        data_path = os.path.join(app_config.Data_End, 'Gcores_User.json')

        # 读取JSON文件
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        read_num = 0
        # 遍历外层数组
        for key, value in data.items():
            # 构建插入数据
            insert_data = (
                int(key),
                value['nickname'],
                value['images'],
                value['url'],
                value['location'],
                value['intro'],
                value['followers-count'],
                value['followees-count'],
                value['created-at'],
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )

            read_num +=1

            print(f"已导入{read_num}条")
            self.upsert(insert_data)


# --- 【全新】用于测试“单次运动记录”功能的测试用例 ---
if __name__ == '__main__':
    db_instance = SQLite_util(app_config.SQLitePath)

    repository = GcoresUserRepository(db_instance)
    repository.import_GcoresUser_SQLite()

