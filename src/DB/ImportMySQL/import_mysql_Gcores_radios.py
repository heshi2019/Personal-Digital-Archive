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
          WHERE table_name = 'gcores_radios_data'
      """)

    if cursor.fetchone()[0] == 0:
        # 创建表结构
        create_table_sql = """
             CREATE TABLE gcores_radios_data (
                id BIGINT PRIMARY KEY,           -- id
                title VARCHAR(255) NOT NULL,     -- 标题
                duration BIGINT NOT NULL,        -- 时长
                cover VARCHAR(255),             -- 封面url
                published_at varchar(255) NOT NULL,-- 发布时间
                likes_count BIGINT NOT NULL,        -- 点赞量
                comments_count BIGINT NOT NULL,     -- 评论量
                category BIGINT NOT NULL,           -- 专题id
                userList TEXT NOT NULL,     -- 参与节目用户id列表，已修改为TEXT类型
                `desc` TEXT ,                       -- 小标题
                bookmark_count BIGINT NOT NULL,     -- 收藏数
                plays BIGINT NOT NULL,              -- 播放量
                content TEXT,                       -- 博客内容描述
                url VARCHAR(255) NOT NULL,          -- url
             
                -- 新增更新时间字段
                `update` TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
                     ON UPDATE CURRENT_TIMESTAMP,
                -- 索引配置
                INDEX idx_id (id),
                INDEX idx_name (title),
                INDEX idx_creatDate (published_at)

            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

          """
        print("\n执行建表 SQL:\n" + create_table_sql.strip())
        cursor.execute(create_table_sql)

        print("已创建新表 gcores_radios_data")  # 修复表名打印错误

    # 遍历外层数组
    for value in data:
        # 构建插入数据
        insert_data = (
            int(value["id"]),
            value['title'],
            int(value['duration']),
            value["cover"],
            value['published_at'],
            int(value['likes_count']),
            int(value['comments_count']),
            int((value.get("category") or {}).get("id",99999)),
            json.dumps(value['userList']),
            value['desc'],
            int(value['bookmarks_count']),
            value['content'],
            value['url'],
            value['plays'],
        )

        # 生成INSERT语句
        sql = '''
            INSERT INTO gcores_radios_data (
                id, title, duration, cover, published_at, likes_count, comments_count, category, userList, `desc`, bookmark_count, content, url ,plays  
                
            ) VALUES (
                %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s
            )ON DUPLICATE KEY UPDATE
            id = VALUES(id),
            title = VALUES(title),
            duration = VALUES(duration),
            cover = VALUES(cover),
            published_at = VALUES(published_at),
            likes_count = VALUES(likes_count),
            comments_count = VALUES(comments_count),
            category = VALUES(category),
            userList = VALUES(userList),
            `desc` = VALUES(`desc`),
            bookmark_count = VALUES(bookmark_count),
            content = VALUES(content),
            url = VALUES(url),
            plays = VALUES(plays)
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


def import_GcoresRadios_Mysql():
    print("开始导入 Gcores博客 数据 --> mysql")

    data_path = os.path.join(app_config.Data_End, 'Gcores_Radios.json')

    # 调用函数
    parse_and_insert(data_path)

    print("数据导入完成")


if __name__ == "__main__":
    import_GcoresRadios_Mysql()