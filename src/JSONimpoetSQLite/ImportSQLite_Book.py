import json
import os
from datetime import datetime
from pathlib import Path

from anyio import current_time

from InternetCrawler.src.ImpoetMySQL import import_book
from InternetCrawler.src.ImpoetSQLite.BookRepository import BookRepository
from src.config.config import load_config
from src.db.DB import DB

class BookJson:
    def __init__(self):
        self.config = load_config()

        # 初始化数据库，并创建对象
        self.db = DB(self.config.database.path)

    def import_book(self,json_path,type):

        # 换新的数据库管理方式
        filesTable = BookRepository(self.db)
        filesTable.create_tables()

        # 读取JSON文件
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)


        if type == "weread":
            print("微信读书入库开始")
            # 遍历外层数组
            for key, value in data.items():
                # 获取当前时间字符串
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # 构建插入数据 - 使用字典类型
                insert_data = {
                    "type": type,
                    "bookId": value["bookId"],
                    "title": value["title"],
                    "classification": value["classification"],
                    "market": json.dumps(value["1000000"]),
                    "cover": value["cover"],
                    "name": value["name"],
                    "isbn": value["isbn"],
                    "readSign": value["readSign"],
                    "description": value["briefIntroduction"],
                    "publisher": None,
                    "progress": value["Progress"],
                    "readDay": value["ReadDay"],
                    "readDayTime": value["ReadDayTime"],
                    "startDay": value["StartDay"],
                    "lastDay": value["LastDay"],
                    "latestDay": value["LatestDay"],
                    "readUrl": value["ReadUrl"],
                    "wordCount": None,
                    "deviceName": None,
                    "notenum": None,
                    "updated_at": current_time
                }

                for value1 in value["markText"]:
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    insert_market_data = {
                        "title": value["title"],
                        "chapterUid": value1["chapterUid"],
                        "chapterTitle": value1["chapterTitle"],
                        "markText": value1["markText"],
                        "content": value1["content"],
                        "createDate": value1["createTime"],
                        "updateDate": current_time
                    }
                    filesTable.upsert_mark(insert_market_data)
                filesTable.upsert_book(insert_data)
                print(f"入库完成  {value["title"]}")

        elif type == "du":
            print("网易蜗牛读书入库开始")
            # 遍历外层数组
            for key, value in data.items():

                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 构建插入数据 - 使用字典类型
                insert_data = {
                    "type": type,
                    "bookId": value["bookId"],
                    "title": value["title"],
                    "classification": value["category"],
                    "market": json.dumps(value["1000000"]),
                    "cover": value["imageUrl"],
                    "name": str(value["authors"]),
                    "isbn": value["isbn"],
                    "readSign": value["ReadStatus"],
                    "description": value["description"],
                    "publisher": value["publisher"],
                    "progress": None,
                    "readDay": None,
                    "readDayTime": None,
                    "startDay": value["publishTime"],
                    "lastDay": value["LastReadTime"],
                    "latestDay": None,
                    "readUrl": None,
                    "wordCount": value["wordCount"],
                    "deviceName": None,
                    "notenum": None,
                    "updated_at": current_time
                }

                for value1 in value["market"]:
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    insert_market_data = {
                        "title": value["title"],
                        "chapterUid": value1["articleId"],
                        "chapterTitle": value1["title"],
                        "markText": value1["markText"],
                        "content": value1["remark"],
                        "createDate": value1["createTime"],
                        "updateDate": value1["uploadTime"],
                        "updated_at": current_time
                    }
                    filesTable.upsert_mark(insert_market_data)
                filesTable.upsert_book(insert_data)
                print(f"入库完成  {value["title"]}")

        elif type == "zhangyue":
            print("掌阅入库开始")
            # 遍历外层数组
            for value in data:
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # 构建插入数据 - 使用字典类型
                insert_data = {
                    "type": type,
                    "bookId": None,
                    "title": value["title"],
                    "classification": None,
                    "market": None,
                    "cover": value["imageUrl"],
                    "name": str(value["authors"]),
                    "isbn": None,
                    "readSign": None,
                    "description": None,
                    "publisher": None,
                    "progress": value["readpercent"],
                    "readDay": None,
                    "readDayTime": None,
                    "startDay": None,
                    "lastDay": value["updateTime"],
                    "latestDay": None,
                    "readUrl": None,
                    "wordCount": None,
                    "deviceName": value["deviceName"],
                    "notenum": value["notenum"],
                    "updated_at": current_time
                }

                for value1 in value["annotations"]:
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    insert_market_data = {
                        "title": value["title"],
                        "chapterUid": None,
                        "chapterTitle": None,
                        "markText": value1["original_text"],
                        "content": value1["thoughts"],
                        "createDate": value1["date"],
                        "updateDate": None,
                        "updated_at": current_time
                    }
                    filesTable.upsert_mark(insert_market_data)
                filesTable.upsert_book(insert_data)
                print(f"入库完成  {value["title"]}")


if __name__ == "__main__":
    print("开始导入 book 数据 --> SQLite")

    # 动态生成正确路径
    current_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent
    data_path = os.path.join(os.path.dirname(current_dir),'InternetCrawler\src', 'Data_End', 'weread.json')  # 改为平级目录

    bookJson = BookJson()
    bookJson.import_book(data_path,"weread")