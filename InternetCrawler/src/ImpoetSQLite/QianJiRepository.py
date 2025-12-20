# qianji_repository.py
from typing import Dict, Any, List, Optional
from src.db import DB


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

    def upsert(self, item: Dict[str, Any]):
        """
        Insert or update a qianji record.
        Accepts item dict with keys corresponding to table columns.
        Handles basic normalization (types).
        """
        # Normalize / ensure keys exist
        row = {
            "key": str(item.get("key") or item.get("id") or ""),
            "date": item.get("date"),
            "category": item.get("category"),
            "type": item.get("type"),
            "money": float(item.get("money")) if item.get("money") not in (None, "") else None,
            "currency": item.get("currency"),
            "from": item.get("from"),       # keep original field name
            "target": item.get("target"),
            "asset": item.get("asset"),
            "remark": item.get("remark"),
            "hasbx": int(item.get("hasbx")) if item.get("hasbx") not in (None, "") else 0,
            "username": item.get("username"),
            "billflag": item.get("billflag"),
            "sourceid": item.get("sourceid"),
            "updated_at": item.get("updated_at") or item.get("date")
        }

        # Use named params to bind values
        # Note: double quotes around "key" and "from" in SQL to avoid keyword issues
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
