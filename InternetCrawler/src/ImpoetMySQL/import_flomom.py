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
          WHERE table_name = 'flomo_data'
      """)

    if cursor.fetchone()[0] == 0:
        # 创建表结构
        create_table_sql = """
             CREATE TABLE flomo_data (
                `DataTime` datetime PRIMARY KEY,
                Text TEXT,
                Images JSON,
                Extend1 Varchar(50),
                Extend2 Varchar(50),
                Extend3 Varchar(50),

                -- 新增更新时间字段
                `update` TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
                     ON UPDATE CURRENT_TIMESTAMP,
                -- 索引配置
                INDEX idx_title (`DataTime`)

            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

          """
        print("\n执行建表 SQL:\n" + create_table_sql.strip())
        cursor.execute(create_table_sql)

        print("已创建新表 flomo_data")

    # 遍历外层数组
    for item in data:

        # 构建插入数据
        insert_data = (
            item["time"],
            item["content"],
            json.dumps(item["files"]),
            None,
            None,
            None
        )

        # 生成INSERT语句
        sql = '''
            INSERT INTO flomo_data (
                DataTime, Text, Images, Extend1, Extend2, Extend3
            ) VALUES (
                %s, %s, %s, %s, %s, %s
            )ON DUPLICATE KEY UPDATE
            Text = VALUES(Text),
            Images = VALUES(Images),
            Extend1 = VALUES(Extend1),
            Extend2 = VALUES(Extend2),
            Extend3 = VALUES(Extend3)
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
    print("开始导入 flomo 数据 --> mysql")

    # 动态生成正确路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(os.path.dirname(current_dir), 'Data_End', 'flomo.json')  # 改为平级目录

    # 调用函数
    parse_and_insert(data_path)

    print("数据导入完成")


if __name__ == "__main__":
    main()