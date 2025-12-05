import sqlite3

def create_tables(conn: sqlite3.Connection):
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS files (
        id TEXT PRIMARY KEY,
        path TEXT NOT NULL,
        type TEXT,
        created_at TEXT,
        modified_at TEXT,
        hash TEXT,
        size INTEGER,
        thumbnail_path TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id TEXT,
        tag TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS collections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS collection_items (
        collection_id INTEGER,
        file_id TEXT
    )''')

    conn.commit()
