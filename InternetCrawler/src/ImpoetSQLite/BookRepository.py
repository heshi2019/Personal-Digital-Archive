import glob
import os
import time
import traceback
from datetime import datetime

from typing import Dict, List

from src.config.config import load_config
from src.db.DB import DB
import json

'''
upsert_book和upsert_mark函数很奇特，这两个sql执行函数，形参需要的是dict，
但在import_book_SQLite函数中给他传递实参的时候，一个是tuple，一个是list，但最后程序还是能执行。

下面是AI解释的原因
1.类型注解的性质：Python的类型注解（如row: Dict）只是静态提示，不是运行时强制检查。Python解释器在运行时会忽略这些类型注解，所以即使传递元组也不会直接导致类型错误。
2.SQLite参数处理机制：DB类的execute方法直接将参数传递给SQLite的execute方法。SQLite的Python驱动程序能够灵活处理不同类型的参数：
    当SQL语句使用位置占位符（如?）时，期望接收元组/列表参数
    当SQL语句使用命名占位符（如:title）时，通常期望接收字典参数
3.驱动程序的灵活性：有趣的是，即使SQL语句使用了命名占位符，SQLite的Python驱动程序仍然能够处理元组参数。它会按照元组中元素的顺序，依次将值分配给SQL语句中的占位符位置，而不管占位符的名称是什么。
4.参数数量匹配：从代码中可以看到，元组的元素数量与SQL语句中占位符的数量完全匹配（例如，book表有22个字段，元组就有22个元素），这使得按位置匹配成为可能。

import_book_SQLite函数中这样写的原因是，import_SQLite这个函数其实修改import_MYSQL而来的，


'''


class BookRepository:
    def __init__(self, db: DB):
        self.db = db

    # ---------------------------------------------------
    # 创建主表 + 章节表
    # ---------------------------------------------------
    def create_tables(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS book_data (
                type TEXT,                  -- 阅读平台
                -- 书籍核心字段
                bookId TEXT,               
                title TEXT PRIMARY KEY,     -- 书名，不为空，但可能重复
                classification TEXT,        -- 书籍类型，如小说，科幻等
                market JSON,                -- 本书章节
                cover TEXT,                 -- 封面连接
                name TEXT,                  -- 作者，可能有多个，也就是现在面临的多对多问题
                isbn TEXT,
                readSign TEXT,              -- 阅读状态，可能为空（已读完，未读，在读）
                description TEXT,           -- 书本简介，可能为空
                publisher TEXT,             -- 出版社，可能为空
                progress INTEGER,           -- 阅读进度，可能为空	
                readDay INTEGER,            -- 阅读天数，可能为空
                readDayTime TEXT,           -- 阅读时长，可能为空
                startDay TEXT,              -- 开始阅读时间，可能为空
                lastDay TEXT,               -- 最后阅读时间，可能为空
                latestDay TEXT,             -- 最晚阅读时间，可能为空    
                readUrl TEXT,               -- 阅读链接，可能为空
                wordCount INTEGER,          -- 书籍字数，可能为空
                deviceName TEXT,            -- 阅读设备，可能为空
                notenum INTEGER,            -- 笔记数   
                updated_at TEXT             -- 更新时间字段，SQLite中不支持自动更新ON UPDATE CURRENT_TIMESTAMP
            )
        """)

        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_book_data_lastDay ON book_data(title,lastDay);
        """)

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS book_market_data (
                title TEXT NOT NULL,            -- 书名
                chapterUid TEXT,                -- 章节id
                chapterTitle TEXT,              -- 章节名
                markText TEXT NOT NULL,         -- 划线内容
                content TEXT,                   -- 笔记内容
                createDate TEXT,                -- 创建时间
                updateDate TEXT,                -- 更新时间 
                UNIQUE(markText),               -- UNIQUE 唯一性强制（值为NULL时，不算重复，且允许插入NULL），自动创建索引
                UNIQUE(title, chapterUid, chapterTitle)
                                                -- UNIQUE可写在列后（列级约束，简单快速），也可以像这样写在建表语句后（表级约束，更灵活），
                
          
            )
        """)

    # ---------------------------------------------------
    # 插入或更新书籍主表
    # ---------------------------------------------------
    def upsert_book(self, row: Dict):

        # 下面的sql可以接受两种参数，一种是dict，一种是tuple
        # 字典的时候，写法就是:type,会去形参传递的字典中找key=type的值赋值给insert/update的sql中的type
        # tuple/list写法的时候，占位符使用？，这种情况下依赖形参中的参数顺序，程序会将参数按顺序赋值，并且是全表插入
        #
        # 但其实就算使用的字典形式，传入的是tuple/list，程序也能正常执行，这是因为SQLite的Python驱动程序能够灵活处理不同类型的参数。
        # 他会按照参数顺序，将元组中的元素赋值给sql中的占位符，而不管占位符的名称是什么。
        #
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

    def reader_file(self, file_path: str):

        # 动态生成正确路径
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # 微信读书
        data_path = os.path.join(os.path.dirname(current_dir), 'Data_End', file_path)  # 改为平级目录

        files_found = glob.glob(data_path)

        if not files_found:
            print(f"错误：在文件夹 '{current_dir}' 中没有找到匹配 '{file_path}' 的文件。")
        else:
            source_file = files_found[0]
            print(f"成功找到源文件：'{source_file}'，开始处理...")

        data = None

        # 读取JSON文件
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if data:
            return data
        else:
            print(f"错误：读取文件 '{file_path}' 时数据为空。")
            return None

    def import_book_SQLite(self):
        start = time.perf_counter()

        try:
            self.create_tables()

            print("开始导入 book 数据 --> SQLite")

            # 微信读书
            data = self.reader_file('weread.json')

            weread_list = []

            # 遍历外层数组
            for key, value in data.items():

                # 构建插入数据
                insert_data = (
                    'weread',
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
                    None,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
                    ext_list.append(insert_market_data)

                weread_list.append((insert_data, ext_list))

            self.insert_update_function(weread_list)

            # 网易蜗牛
            data = self.reader_file('du.json')
            du_list = []

            # 遍历外层数组
            for key, value in data.items():

                # 构建插入数据
                insert_data = (
                    'du',
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
                    None,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                )

                ext_list = []
                for value1 in value["market"]:
                    insert_market_data = (
                        value["title"],
                        value1["articleId"],
                        value1["title"],
                        value1["markText"],
                        value1["remark"],
                        value1["createTime"],
                        value1["uploadTime"]
                    )
                    ext_list.append(insert_market_data)

                du_list.append((insert_data, ext_list))

            self.insert_update_function(du_list)

            # 掌阅
            zhangyue_list = []
            # 掌阅是一次性入库
            data = self.reader_file('zhangyue.json')

            # 遍历外层数组
            for value in data:

                # 构建插入数据
                insert_data = (
                    'zhangyue',
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
                    value["notenum"],
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                )

                ext_list = []
                for value1 in value["annotations"]:
                    insert_market_data = (
                        value["title"],
                        None,
                        None,
                        value1["original_text"],
                        value1["thoughts"],
                        value1["date"],
                        None,
                    )
                    ext_list.append(insert_market_data)

                zhangyue_list.append((insert_data, ext_list))

            self.insert_update_function(zhangyue_list)

        except Exception as e:
            print(f"警告：遍历数据时发生未知错误：{e}，已跳过。\n")
            print(f"错误位置：{traceback.format_exc()}")

        end = time.perf_counter()
        print(f"导入完成，耗时: {end - start:.2f}秒")

# --- 【全新】用于测试“单次运动记录”功能的测试用例 ---
if __name__ == '__main__':
    config = load_config()
    db_instance = DB(config.database.path)

    repository = BookRepository(db_instance)
    repository.import_book_SQLite()
