# Placeholder for collections (manual groupings)
from db.operations import DB

# 逻辑目录管理
class CollectionManager:
    def __init__(self, db: DB):
        self.db = db

    def create_collection(self, name: str):
        cur = self.db.conn.execute('INSERT INTO collections (name) VALUES (?)', (name,))
        self.db.conn.commit()
        return cur.lastrowid

    def add_to_collection(self, collection_id: int, file_id: str):
        self.db.conn.execute('INSERT INTO collection_items (collection_id, file_id) VALUES (?, ?)', (collection_id, file_id))
        self.db.conn.commit()
