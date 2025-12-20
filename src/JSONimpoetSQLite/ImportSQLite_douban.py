import json

from InternetCrawler.src.ImpoetSQLite.DoubanRepository import DoubanRepository
from src.config.config import load_config
from src.db.DB import DB


class ImportSQLite_douban:
    def __init__(self):
        self.config = load_config()

        # 初始化数据库，并创建对象
        self.db = DB(self.config.database.path)

    def import_douban(self,json_path,type):

        # 换新的数据库管理方式
        filesTable = DoubanRepository(self.db)
        filesTable.create_table()

        # 读取JSON文件
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 遍历外层数组
        for category in data:
            # 每个字典只有一个键值对，如 "mark": [...]
            category_name, items = next(iter(category.items()))

            # 遍历每个分类下的条目
            for item in items:
                movie = item['movieOne']
                comment = item.get('myComment', {})

                # 构建插入数据
                insert_data = (
                    movie['id'],
                    movie['title'],
                    category_name,
                    json.dumps(movie['directors'], ensure_ascii=False),  # 转为JSON数组
                    json.dumps(movie['Scriptwriter'], ensure_ascii=False),
                    json.dumps(movie['actors'], ensure_ascii=False),
                    movie['count'],
                    json.dumps(movie['genres'], ensure_ascii=False),
                    movie['countNum'],
                    movie['countIne'],
                    movie['pubdate'][0] if movie['pubdate'] else None,  # 取第一个日期
                    movie['url'],
                    json.dumps(movie['vendor_names'], ensure_ascii=False),
                    movie['cover_url'],
                    json.dumps(movie['honor_infos'], ensure_ascii=False),
                    comment.get('create_time'),
                    comment.get('comment') or None,  # 空字符串转NULL
                    int(comment['MyValue']) if comment.get('MyValue', '未评分') != '未评分' else None
                )


