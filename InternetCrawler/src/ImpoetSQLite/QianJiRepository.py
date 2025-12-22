# qianji_repository.py
import json
import os
from datetime import datetime
from typing import Dict, Any, List, Optional

from src.config.config import load_config
from src.db.DB import DB


class QianjiRepository:
    """
    Repository for qianji_data (钱迹) table, SQLite version.
    Methods:
      - create_table()
      - upsert(item: Dict)
      - get_all() -> List[Dict]
      - get_by_key(key: str) -> Optional[Dict]
      - delete(key: str)
    """

    def __init__(self, db: DB):
        self.db = db

    def create_table(self):
        # use double quotes for potentially reserved names (e.g. "key", "from")
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS qianji_data (
                "key" TEXT PRIMARY KEY,
                date TEXT,
                category TEXT,
                type TEXT,
                money REAL,
                currency TEXT,
                "from" TEXT,
                target TEXT,
                asset TEXT,
                remark TEXT,
                hasbx INTEGER,
                username TEXT,
                billflag TEXT,
                sourceid TEXT,
                updated_at TEXT
            )
        """)

        self.db.execute("""
             CREATE INDEX IF NOT EXISTS idx_qianji_data_key ON qianji_data(key);
         """)

        self.db.execute("""
             CREATE INDEX IF NOT EXISTS idx_qianji_data_time ON qianji_data(date);
         """)


    def upsert(self, row: Dict[str, Any]):

        self.db.execute("""
            INSERT INTO qianji_data (
                "key", date, category, type, money, currency,
                "from", target, asset, remark,
                hasbx, username, billflag, sourceid, updated_at
            ) VALUES (
                :key, :date, :category, :type, :money, :currency,
                :from, :target, :asset, :remark,
                :hasbx, :username, :billflag, :sourceid, :updated_at
            )
            ON CONFLICT("key") DO UPDATE SET
                date = excluded.date,
                category = excluded.category,
                type = excluded.type,
                money = excluded.money,
                currency = excluded.currency,
                "from" = excluded."from",
                target = excluded.target,
                asset = excluded.asset,
                remark = excluded.remark,
                hasbx = excluded.hasbx,
                username = excluded.username,
                billflag = excluded.billflag,
                sourceid = excluded.sourceid,
                updated_at = excluded.updated_at
        """, row)

    def get_all(self) -> List[Dict[str, Any]]:
        rows = self.db.query('SELECT * FROM qianji_data')
        # convert sqlite3.Row to dict
        return [dict(r) for r in rows]

    def get_by_key(self, key: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one('SELECT * FROM qianji_data WHERE "key" = ?', (key,))
        return dict(row) if row else None

    def delete(self, key: str):
        self.db.execute('DELETE FROM qianji_data WHERE "key" = ?', (key,))




    def import_Qianji_SQLite(self):
        print("开始导入 钱迹 数据 --> SQLite")

        self.create_table()

        # 动态生成正确路径
        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_path = os.path.join(os.path.dirname(current_dir), 'Data_End', 'qianji.json')  # 改为平级目录

        # 读取JSON文件
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        read_num = 0
        # 遍历外层数组
        for item in data:

            # 构建插入数据
            insert_data = (
                item['key'],
                item['date'],
                item['category'],
                item['type'],
                float(item['money']),
                item['currency'],
            )

            if item['type'] == "收入" or item["type"] == "支出":
                insert_data += (
                    None,
                    None,
                    item.get("asset", ""),
                    item.get("remark", ""),

                )
            elif item["type"] == "转账":
                insert_data += (
                    item['from'],
                    item['target'],
                    None,
                    None,

                )
            elif "债务" in item["type"]:
                insert_data += (
                    item['from'],
                    None,
                    item.get("asset", ""),
                    item.get("remark", ""),
                )
            else:
                print(f"未分类数据：{item}")

            insert_data += (
                int(item['hasbx']),
                item['username'],
                item['billflag'],
                item['sourceid'],
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )


            read_num +=1

            print(f"已导入{read_num}条")
            self.upsert(insert_data)




# --- 【全新】用于测试“单次运动记录”功能的测试用例 ---
if __name__ == '__main__':
    config = load_config()
    db_instance = DB(config.database.path)

    repository = QianjiRepository(db_instance)
    repository.import_Qianji_SQLite()

