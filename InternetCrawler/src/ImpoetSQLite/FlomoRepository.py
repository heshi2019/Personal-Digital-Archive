# flomo_repository.py
import os
from datetime import datetime

from src.config.config import load_config
from src.db.DB import DB
import json


class FlomoRepository:
    def __init__(self, db: DB):
        self.db = db

    # ---------------------------------------------------
    # 创建表
    # ---------------------------------------------------
    def create_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS flomo_data (
                dataTime TEXT PRIMARY KEY,
                text TEXT,
                images JSON,
                extend1 TEXT,
                extend2 TEXT,
                extend3 TEXT,
                updated_at TEXT
            )
        """)

        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_flomo_data_time ON flomo_data(dataTime);
        """)

    # ---------------------------------------------------
    # 插入或更新
    # ---------------------------------------------------
    def upsert(self, row: dict):
        self.db.execute("""
            INSERT INTO flomo_data (
                dataTime, text, images, extend1, extend2, extend3, updated_at
            ) VALUES (
                :dataTime, :text, :images, :extend1, :extend2, :extend3, :updated_at
            )
            ON CONFLICT(dataTime) DO UPDATE SET
                text=excluded.text,
                images=excluded.images,
                extend1=excluded.extend1,
                extend2=excluded.extend2,
                extend3=excluded.extend3,
                updated_at=excluded.updated_at
        """, row)



    def import_flomo_SQLite(self):
        print("开始导入 flomo 数据 --> SQLite")

        self.create_table()

        # 动态生成正确路径
        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_path = os.path.join(os.path.dirname(current_dir), 'Data_End', 'flomo.json')  # 改为平级目录

        # 读取JSON文件
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        read_num = 0
        # 遍历外层数组
        for item in data:
            # 构建插入数据
            insert_data = (
                item["time"],
                item["content"],
                json.dumps(item["files"]),
                None,
                None,
                None,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            read_num +=1

            print(f"导入{read_num}条: {item["time"]}")
            self.upsert(insert_data)


# --- 【全新】用于测试“单次运动记录”功能的测试用例 ---
if __name__ == '__main__':
    config = load_config()
    db_instance = DB(config.database.path)

    repository = FlomoRepository(db_instance)
    repository.import_flomo_SQLite()

