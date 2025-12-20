# Placeholder for tag management. Expand as needed.
# from db.operations import DB

# 标签管理
class TagManager:
    def __init__(self, db: DB):
        self.db = db

    def add_tag(self, file_id: str, tag: str):
        self.db.conn.execute('INSERT INTO tags (file_id, tag) VALUES (?, ?)', (file_id, tag))
        self.db.conn.commit()

    def list_tags(self, file_id: str):
        rows = self.db.conn.execute('SELECT tag FROM tags WHERE file_id=?', (file_id,)).fetchall()
        return [r[0] for r in rows]
