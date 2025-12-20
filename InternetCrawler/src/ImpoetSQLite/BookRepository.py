# book_repository.py
import glob
import os
import time
from typing import Dict, List

from src.config.config import load_config
from src.db import DB
import json


class BookRepository:
    def __init__(self, db: DB):
        self.db = db

    # ---------------------------------------------------
    # 创建主表 + 章节表
    # ---------------------------------------------------
    def create_tables(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS book_data (
                type TEXT,
                bookId TEXT,
                title TEXT PRIMARY KEY,
                classification TEXT,
                market JSON,
                cover TEXT,
                name TEXT,
                isbn TEXT,
                readSign TEXT,
                description TEXT,
                publisher TEXT,
                progress INTEGER,
                readDay INTEGER,
                readDayTime TEXT,
                startDay TEXT,
                lastDay TEXT,
                latestDay TEXT,
                readUrl TEXT,
                wordCount INTEGER,
                deviceName TEXT,
                notenum INTEGER,
                updated_at TEXT
            )
        """)

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS book_market_data (
                title TEXT NOT NULL,
                chapterUid TEXT,
                chapterTitle TEXT,
                markText TEXT,
                content TEXT,
                createDate TEXT,
                updateDate TEXT,
                UNIQUE(markText),
                UNIQUE(title, chapterUid, chapterTitle),
                
                -- 定义外键约束，并设置级联删除
                FOREIGN KEY (markText) 
                    REFERENCES book_data(title) 
                    ON DELETE CASCADE -- 当主运动记录被删除时，其所有分段详情也会被自动删除
                    
            )
        """)

    # ---------------------------------------------------
    # 插入或更新书籍主表
    # ---------------------------------------------------
    def upsert_book(self, row: Dict):
        self.db.execute("""
            INSERT INTO book_data (
                type, bookId, title, classification, market, cover, name, isbn,
                readSign, description, publisher, progress, readDay, readDayTime,
                startDay, lastDay, latestDay, readUrl, wordCount, deviceName, notenum, updated_at
            ) VALUES (
                :type, :bookId, :title, :classification, :market, :cover, :name, :isbn,
                :readSign, :description, :publisher, :progress, :readDay, :readDayTime,
                :startDay, :lastDay, :latestDay, :readUrl, :wordCount, :deviceName, :notenum, :updated_at
            )
            ON CONFLICT(title) DO UPDATE SET
                type=excluded.type,
                bookId=excluded.bookId,
                title=excluded.title,
                classification=excluded.classification,
                market=excluded.market,
                cover=excluded.cover,
                name=excluded.name,
                isbn=excluded.isbn,
                readSign=excluded.readSign,
                description=excluded.description,
                publisher=excluded.publisher,
                progress=excluded.progress,
                readDay=excluded.readDay,
                readDayTime=excluded.readDayTime,
                startDay=excluded.startDay,
                lastDay=excluded.lastDay,
                latestDay=excluded.latestDay,
                readUrl=excluded.readUrl,
                wordCount=excluded.wordCount,
                deviceName=excluded.deviceName,
                notenum=excluded.notenum,
                updated_at=excluded.updated_at
        """, row)


    # ---------------------------------------------------
    # 插入或更新标注记录
    # ---------------------------------------------------
    def upsert_mark(self, mark_row: Dict):

        self.db.execute("""
            INSERT INTO book_market_data (
                title, chapterUid, chapterTitle, markText, content, createDate, updateDate
            ) VALUES (
                :title, :chapterUid, :chapterTitle, :markText, :content, :createDate, :updateDate
            )
            ON CONFLICT(title,chapterUid,chapterTitle) DO UPDATE SET
                markText=excluded.markText,
                content=excluded.content,
                createDate=excluded.createDate,
                updateDate=excluded.updateDate
        """, mark_row)


    def insert_update_function(self, row: List):
        """
        批量保存多条记录，所有操作在一个事务中完成，提高处理大量数据时的性能。

        Args:
                    [
                        (main_tuple,list[ext_tuple])
                    ]
                     包含(main_tuple, list[ext_tuple])元组的列表
        """

        if not row:
            return

        with self.db.transaction() as cursor:
            try:
                for main_tuple, ext_tuple_list in row:
                    self.upsert_book(main_tuple)

                    for ext_tuple in ext_tuple_list:
                        self.upsert_mark(ext_tuple)

            except Exception as e:
                print(f"警告：数据写入/更新时发生未知错误：{e}，已跳过。\n"
                      f"main_tuple：{main_tuple}\n"
                      f"ext_tuple_list：{ext_tuple_list}")


# --- 【全新】用于测试“单次运动记录”功能的测试用例 ---
if __name__ == '__main__':

    start = time.perf_counter()

    config = load_config()
    db_instance = DB(config.database.path)
    try:
        repo = BookRepository(db_instance)
        repo.create_tables()

        print("开始导入 book 数据 --> SQLite")


        # 动态生成正确路径
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # 微信读书
        data_path = os.path.join(os.path.dirname(current_dir), 'Data_End', 'weread.json')  # 改为平级目录

        # 读取JSON文件
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        weread_list = []

        # 遍历外层数组
        for key, value in data.items():

            # 构建插入数据
            insert_data = (
                type,
                value["bookId"],
                value["title"],
                value["classification"],
                json.dumps(value["1000000"]),
                value["cover"],
                value["name"],
                value["isbn"],
                value["readSign"],
                value["briefIntroduction"],
                None,
                value["Progress"],
                value["ReadDay"],
                value["ReadDayTime"],
                value["StartDay"],
                value["LastDay"],
                value["LatestDay"],
                value["ReadUrl"],
                None,
                None,
                None
            )

            ext_list = []
            for value1 in value["markText"]:
                insert_market_data = (
                    value["title"],
                    value1["chapterUid"],
                    value1["chapterTitle"],
                    value1["markText"],
                    value1["content"],
                    value1["createTime"],
                    None,
                )

            weread_list.append((insert_data,ext_list))

        repo.insert_update_function(weread_list)



        # 网易蜗牛
        data_path = os.path.join(os.path.dirname(current_dir), 'Data_End', 'zhangyue.json')  # 改为平级目录

        du_list = []

        # 遍历外层数组
        for key, value in data.items():

            # 构建插入数据
            insert_data = (
                type,
                value["bookId"],
                value["title"],
                value["category"],
                json.dumps(value["1000000"]),
                value["imageUrl"],
                str(value["authors"]),
                value["isbn"],
                value["ReadStatus"],
                value["description"],
                value["publisher"],
                None,
                None,
                None,
                value["publishTime"],
                value["LastReadTime"],
                None,
                None,
                value["wordCount"],
                None,
                None
            )

            ext_list = []
            for value1 in value["markText"]:
                insert_market_data = (
                    value["title"],
                    value1["articleId"],
                    value1["title"],
                    value1["markText"],
                    value1["remark"],
                    value1["createTime"],
                    value1["uploadTime"]
                )

            du_list.append((insert_data, ext_list))

        repo.insert_update_function(du_list)

        # 掌阅
        zhangyue_list = []

        # 遍历外层数组
        for key, value in data.items():

            # 构建插入数据
            insert_data = (
                type,
                None,
                value["title"],
                None,
                None,
                value["imageUrl"],
                str(value["authors"]),
                None,
                None,
                None,
                None,
                value["readpercent"],
                None,
                None,
                None,
                value["updateTime"],
                None,
                None,
                None,
                value["deviceName"],
                value["notenum"]
            )

            ext_list = []
            for value1 in value["markText"]:
                insert_market_data = (
                    value["title"],
                    None,
                    None,
                    value1["original_text"],
                    value1["thoughts"],
                    value1["date"],
                    None,
                )

            zhangyue_list.append((insert_data, ext_list))

        repo.insert_update_function(zhangyue_list)



        if not files_found:
            print(f"错误：在文件夹 '{DATA_FOLDER_PATH}' 中没有找到匹配 '{CSV_FILENAME_PATTERN}' 的文件。")
        else:
            source_file = files_found[0]
            print(f"成功找到源文件：'{source_file}'，开始处理...")

            # 使用批处理，每次处理10000条记录，在连续处理35万条记录后速度大幅下降
            BATCH_SIZE = 1  # 批处理大小，可以根据系统性能调整
            batch = []

            with open(source_file, 'r', encoding='utf-8') as file:
                html_content = file.read()

            with (open(source_file, mode='r', encoding='utf-8') as csvfile):

                total_rows = 0
                processed_rows = 0
