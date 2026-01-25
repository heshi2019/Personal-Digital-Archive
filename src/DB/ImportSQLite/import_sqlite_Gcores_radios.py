# gcores_radios_repository.py
import os
from datetime import datetime
from typing import Dict

import json

from src.DB.SQLite_util import SQLite_util
from src.config.configClass import app_config


class GcoresRadiosRepository:
    def __init__(self, db: SQLite_util):
        self.db = db

    # ---------------------------------------------------
    # 创建表
    # ---------------------------------------------------
    def create_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS gcores_radios_data (
                id INTEGER PRIMARY KEY,             -- 播客ID
                title TEXT NOT NULL,                -- 播客标题        
                duration INTEGER NOT NULL,          -- 播客时长，秒
                cover TEXT,                         -- 播客封面URL
                published_at TEXT NOT NULL,         -- 发布时间
                likes_count INTEGER NOT NULL,       -- 点赞数
                comments_count INTEGER NOT NULL,    -- 评论数
                bookmark_count INTEGER NOT NULL,    -- 收藏数
                plays INTEGER NOT NULL,             -- 播放量
                category INTEGER NOT NULL,          -- 专题ID
                userList TEXT,                      -- 参与节目用户ID列表
                desc TEXT,                          -- 小标题

                content TEXT,                       -- 播客内容描述
                url TEXT NOT NULL,                  -- 播客URL
                updated_at TEXT                     -- 更新时间
            )
        """)

        self.db.execute("""
             CREATE INDEX IF NOT EXISTS idx_gcores_radios_data_title ON gcores_radios_data(title);
         """)

        self.db.execute("""
             CREATE INDEX IF NOT EXISTS idx_gcores_radios_data_time ON gcores_radios_data(published_at);
         """)

    # ---------------------------------------------------
    # 插入或更新（SQLite Upsert）
    # ---------------------------------------------------
    def upsert(self, row: Dict):

        self.db.execute("""
            INSERT INTO gcores_radios_data (
                id, title, duration, cover, published_at,
                likes_count, comments_count, category, userList,
                desc, bookmark_count, content, url, updated_at,plays
            ) VALUES (
                :id, :title, :duration, :cover, :published_at,
                :likes_count, :comments_count, :category, :userList,
                :desc, :bookmark_count, :content, :url, :updated_at, :plays
            )
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                duration = excluded.duration,
                cover = excluded.cover,
                published_at = excluded.published_at,
                likes_count = excluded.likes_count,
                comments_count = excluded.comments_count,
                category = excluded.category,
                userList = excluded.userList,
                desc = excluded.desc,
                bookmark_count = excluded.bookmark_count,
                content = excluded.content,
                url = excluded.url,
                updated_at = excluded.updated_at,
                plays = excluded.plays
        """, row)

    # ---------------------------------------------------
    # 查询全部
    # ---------------------------------------------------
    def get_all(self):
        return self.db.query("SELECT * FROM gcores_radios_data")

    # ---------------------------------------------------
    # 根据 ID 查询
    # ---------------------------------------------------
    def get_by_id(self, id: int):
        row = self.db.query_one("SELECT * FROM gcores_radios_data WHERE id=?", (id,))
        return dict(row) if row else None

    # ---------------------------------------------------
    # 删除
    # ---------------------------------------------------
    def delete(self, id: int):
        self.db.execute("DELETE FROM gcores_radios_data WHERE id=?", (id,))



    def import_GcoresRadios_SQLite(self):
        print("开始导入 机核电台 数据 --> SQLite")

        self.create_table()

        data_path = os.path.join(app_config.Data_End, 'Gcores_Radios.json')

        # 读取JSON文件
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        read_num = 0
        # 遍历外层数组
        for value in data:
            # 构建插入数据
            insert_data = (
                int(value["id"]),
                value['title'],
                int(value['duration']),
                value["cover"],
                value['published_at'],
                int(value['likes_count']),
                int(value['comments_count']),
                int((value.get("category") or {}).get("id",99999)),
                json.dumps(value['userList']),
                value['desc'],
                int(value['bookmarks_count']),
                value['content'],
                value['url'],
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                value['plays']
            )
            read_num +=1

            print(f"已导入{read_num}条")
            self.upsert(insert_data)


# --- 【全新】用于测试“单次运动记录”功能的测试用例 ---
if __name__ == '__main__':
    db_instance = SQLite_util(app_config.SQLitePath)

    repository = GcoresRadiosRepository(db_instance)
    repository.import_GcoresRadios_SQLite()

