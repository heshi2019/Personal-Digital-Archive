import json
import os  # 新增导入os模块

from src.DB.Mysql_util import Mysql_util
from src.config.configClass import app_config


def parse_and_insert(json_path):
    print(f"正在尝试访问文件路径：{os.path.abspath(json_path)}")

    # 读取JSON文件
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)


    mysql = Mysql_util()
    # 连接到MySQL数据库
    conn,cursor = mysql.connect_to_mysql()

    # 新增：检查表是否存在
    cursor.execute("""
          SELECT COUNT(*)
          FROM information_schema.tables 
          WHERE table_name = 'douban_data'
      """)

# 现在存表的时候，会将导演，编剧，演员，类型，直接作为json格式存入表
    if cursor.fetchone()[0] == 0:
        # 创建表结构
        create_table_sql = """
             CREATE TABLE douban_data (
                -- movieOne 核心字段
                id BIGINT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                `type` VARCHAR(255),
                directors JSON NOT NULL,
                Scriptwriter JSON NOT NULL,
                actors JSON NOT NULL,
                `count` VARCHAR(255),
                genres JSON NOT NULL,
                countNum INT,
                countIne FLOAT,
                pubdate VARCHAR(512),
                url VARCHAR(512),
                vendor_names JSON,
                cover_url VARCHAR(512),cj
                honor_infos JSON,
                
                -- myComment 评论字段
                myComment_create_time DATETIME,
                myComment_comment TEXT, 
                myComment_MyValue VARCHAR(50),
                -- 新增更新时间字段
                `update` TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
                     ON UPDATE CURRENT_TIMESTAMP,
                -- 索引配置
                INDEX idx_title (title),
                INDEX idx_countIne (countIne)

            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
              
          """
        print("\n执行建表 SQL:\n" + create_table_sql.strip())
        cursor.execute(create_table_sql)

        print("已创建新表 douban_data")

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

            # 生成INSERT语句
            sql = '''
                INSERT INTO douban_data (
                    id, title,`type`, directors, Scriptwriter, actors, `count`, genres,
                    countNum, countIne, pubdate , url, vendor_names, cover_url,
                    honor_infos, myComment_create_time, myComment_comment, myComment_MyValue
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                `type` = VALUES(`type`),
                directors = VALUES(directors),
                Scriptwriter = VALUES(Scriptwriter),
                actors = VALUES(actors),
                `count` = VALUES(`count`),
                genres = VALUES(genres),
                countNum = VALUES(countNum),
                countIne = VALUES(countIne),
                pubdate = VALUES(pubdate),
                url = VALUES(url),
                vendor_names = VALUES(vendor_names),
                cover_url = VALUES(cover_url),
                honor_infos = VALUES(honor_infos),
                myComment_create_time = VALUES(myComment_create_time),
                myComment_comment = VALUES(myComment_comment),
                myComment_MyValue = VALUES(myComment_MyValue)
            '''

            # 添加 SQL 打印
            print("\n执行插入 SQL:")
            print(sql.strip())
            print("参数:", insert_data)

            # 执行插入
            cursor.execute(sql, insert_data)
    
    # 提交事务
    conn.commit()
    cursor.close()
    conn.close()


def import_douban_Mysql():
    print("开始导入 douban 数据 --> mysql")

    data_path = os.path.join(app_config.Data_End, 'douban.json')

    # 调用函数
    parse_and_insert(data_path)

    print("数据导入完成")

if __name__ == "__main__":
    import_douban_Mysql()
