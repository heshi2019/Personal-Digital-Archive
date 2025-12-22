# douban_repository.py
import os
from datetime import datetime

from src.config.config import load_config
from src.db.DB import DB
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
                
                -- myComment 评论字段
                myComment_create_time TEXT,
                myComment_comment TEXT,
                myComment_MyValue INTEGER,
                
                updated TEXT
            )
        """)

        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_douban_data_lastDay ON douban_data(title,myComment_create_time);
        """)

    # ---------------------------------------------------
    # 插入或更新
    # ---------------------------------------------------
    def upsert(self, row: dict):
        self.db.execute("""
            INSERT INTO douban_data (
                id, title, type, directors, scriptwriter, actors, count, genres, 
                countNum, countIne, pubdate, url, vendor_names, cover_url, honor_infos,
                myComment_create_time, myComment_comment, myComment_MyValue, updated
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
                myComment_create_time=excluded.myComment_create_time,
                myComment_comment=excluded.myComment_comment,
                myComment_MyValue=excluded.myComment_MyValue,
                updated=excluded.updated
        """, row)

    def import_douban_SQLite(self):
        print("开始导入 douban 数据 --> SQLite")

        self.create_table()

        # 动态生成正确路径
        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_path = os.path.join(os.path.dirname(current_dir), 'Data_End', 'douban.json')  # 改为平级目录

        # 读取JSON文件
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)


            # 遍历外层数组
        for category in data:
            # 每个字典只有一个键值对，如 "mark": [...]
            category_name, items = next(iter(category.items()))

            # 遍历每个分类下的条目
            for item in items:
                movie = item['movieOne']
                comment = item.get('myComment', {})

                # 构建插入数据
                insert_data = (
                    movie['id'],
                    movie['title'],
                    category_name,
                    json.dumps(movie['directors'], ensure_ascii=False),  # 转为JSON数组
                    json.dumps(movie['Scriptwriter'], ensure_ascii=False),
                    json.dumps(movie['actors'], ensure_ascii=False),
                    movie['count'],
                    json.dumps(movie['genres'], ensure_ascii=False),
                    movie['countNum'],
                    movie['countIne'],
                    movie['pubdate'][0] if movie['pubdate'] else None,  # 取第一个日期
                    movie['url'],
                    json.dumps(movie['vendor_names'], ensure_ascii=False),
                    movie['cover_url'],
                    json.dumps(movie['honor_infos'], ensure_ascii=False),
                    comment.get('create_time'),
                    comment.get('comment') or None,  # 空字符串转NULL
                    int(comment['MyValue']) if comment.get('MyValue', '未评分') != '未评分' else None,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                )

                print(f"导入({category_name}): {movie['title']}")
                self.upsert(insert_data)


# --- 【全新】用于测试“单次运动记录”功能的测试用例 ---
if __name__ == '__main__':
    config = load_config()
    db_instance = DB(config.database.path)

    repository = DoubanRepository(db_instance)
    repository.import_douban_SQLite()

