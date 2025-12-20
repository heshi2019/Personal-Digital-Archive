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
          WHERE table_name = 'qianji_data'
      """)

    if cursor.fetchone()[0] == 0:
        # 创建表结构
        create_table_sql = """
             CREATE TABLE qianji_data (
                `key` VARCHAR(255) PRIMARY KEY,
                date datetime,
                category VARCHAR(255),
                type VARCHAR(255),
                money float,
                currency VARCHAR(255),
                `from` VARCHAR(255),
                target VARCHAR(255),
                asset VARCHAR(255),
                remark VARCHAR(255),
                hasbx int,
                username VARCHAR(255),
                billflag VARCHAR(255),
                sourceid VARCHAR(255),
         
                -- 新增更新时间字段
                `update` TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
                     ON UPDATE CURRENT_TIMESTAMP,
                -- 索引配置
                INDEX idx_title (`key`),
                INDEX idx_date (date)

            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

          """
        print("\n执行建表 SQL:\n" + create_table_sql.strip())
        cursor.execute(create_table_sql)

        print("已创建新表 qianji_data")

    # 遍历外层数组
    for item in data:


        # 构建插入数据
        insert_data = (
            item['key'],
            item['date'],
            item['category'],
            item['type'],
            float(item['money']),
            item['currency'],
        )

        if item['type'] == "收入" or item["type"] == "支出":
            insert_data += (
                None,
                None,
                item.get("asset",""),
                item.get("remark",""),

            )
        elif item["type"] =="转账":
            insert_data += (
                item['from'],
                item['target'],
                None,
                None,

            )
        elif "债务" in item["type"]:
            insert_data += (
                item['from'],
                None,
                item.get("asset",""),
                item.get("remark",""),
            )
        else:
            print(f"未分类数据：{item}")

        insert_data += (
            int(item['hasbx']),
            item['username'],
            item['billflag'],
            item['sourceid']
        )

        # 生成INSERT语句
        sql = '''
            INSERT INTO qianji_data (
                `key`,date,category,type,money,currency,
                `from`,target,asset,remark,hasbx,username,billflag,sourceid
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )ON DUPLICATE KEY UPDATE
            `key` = VALUES(`key`),
            date = VALUES(date),
            category = VALUES(category),
            type = VALUES(type),
            money = VALUES(money),
            currency = VALUES(currency),
            `from` = VALUES(`from`),
            target = VALUES(target),
            asset = VALUES(asset),
            remark = VALUES(remark),
            hasbx = VALUES(hasbx),
            username = VALUES(username),
            billflag = VALUES(billflag),
            sourceid = VALUES(sourceid)
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
    print("开始导入 钱迹 数据 --> mysql")

    # 动态生成正确路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(os.path.dirname(current_dir), 'Data_End', 'qianji.json')  # 改为平级目录

    # 调用函数
    parse_and_insert(data_path)

    print("数据导入完成")


if __name__ == "__main__":
    main()