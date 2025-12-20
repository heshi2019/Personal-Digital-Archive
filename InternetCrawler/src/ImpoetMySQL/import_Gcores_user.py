import json
import os  # 新增导入os模块
from InternetCrawler.src.Config.mysql_conn import mysql_conn


def parse_and_insert(json_path):
    print(f"正在尝试访问文件路径：{os.path.abspath(json_path)}")

    # 读取JSON文件
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    connect_to_mysql = mysql_conn()
    # 连接到MySQL数据库
    conn,cursor = connect_to_mysql.connect_to_mysql()

    # 新增：检查表是否存在
    cursor.execute("""
          SELECT COUNT(*)
          FROM information_schema.tables 
          WHERE table_name = 'gcores_user_data'
      """)


    if cursor.fetchone()[0] == 0:
        # 创建表结构
        create_table_sql = """
             CREATE TABLE gcores_user_data (
                id BIGINT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                image VARCHAR(255),
                url VARCHAR(255),
                location VARCHAR(255),
                intro VARCHAR(255),
                followersCount VARCHAR(255),
                followeesCount VARCHAR(255),
                created_at datetime NOT NULL,
             
                -- 新增更新时间字段
                `update` TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
                     ON UPDATE CURRENT_TIMESTAMP,
                -- 索引配置
                INDEX idx_id (id),
                INDEX idx_created (created_at)

            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

          """
        print("\n执行建表 SQL:\n" + create_table_sql.strip())
        cursor.execute(create_table_sql)

        print("已创建新表 gcores_user_data")

    # 遍历外层数组
    for key,value in data.items():

        # 构建插入数据
        insert_data = (
            int(key),
            value['nickname'],
            value['images'],
            value['url'],
            value['location'],
            value['intro'],
            value['followers-count'],
            value['followees-count'],
            value['created-at']
        )

        # 生成INSERT语句
        sql = '''
            INSERT INTO gcores_user_data (
                id, name, image, url, location, intro, followersCount, followeesCount, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            image = VALUES(image),
            url = VALUES(url),
            location = VALUES(location),
            intro = VALUES(intro),
            followersCount = VALUES(followersCount),
            followeesCount = VALUES(followeesCount),
            created_at = VALUES(created_at)
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


def main():
    print("开始导入 Gcores用户 数据 --> mysql")

    # 动态生成正确路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(os.path.dirname(current_dir), 'Data_End', 'Gcores_User.json')  # 改为平级目录

    # 调用函数
    parse_and_insert(data_path)

    print("数据导入完成")


if __name__ == "__main__":
    main()