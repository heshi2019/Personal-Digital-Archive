# Basic timeline builder - can be expanded
# from db.operations import DB
from src.collections import defaultdict

# 生成时间线
class Timeline:
    def __init__(self, db: DB):
        self.db = db

    def build_by_day(self, limit=100, offset=0):
        rows = self.db.conn.execute('SELECT * FROM files ORDER BY created_at DESC LIMIT ? OFFSET ?', (limit, offset)).fetchall()
        groups = defaultdict(list)
        for r in rows:
            d = r['created_at'][:10]
            groups[d].append(dict(r))
        return groups
