import json
import os  # 新增导入os模块
from InternetCrawler.src.Config.mysql_conn import mysql_conn

def parse_and_insert(json_path,type):
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
          WHERE table_name = 'book_data'
      """)

    if cursor.fetchone()[0] == 0:
        # 创建表结构
        create_table_sql = """
             CREATE TABLE book_data (
                -- 阅读平台
				`type` VARCHAR(255),       
                -- 书籍核心字段
                
                bookId VARCHAR(255) ,   
                -- 书名，不为空，但可能重复
                title VARCHAR(255) PRIMARY KEY,  
                -- 类型
                classification VARCHAR(255), 
                -- 本书章节 
                market json,
                -- 封面连接
                cover VARCHAR(255) ,    
                -- 作者，可能有多个，也就是现在面临的多对多问题
                `name` VARCHAR(255),   

				isbn VARCHAR(255),		
                -- 阅读状态，可能为空						
                readSign VARCHAR(255),		
                -- 书本简介，可能为空
                description TEXT,	
                -- 出版社，可能为空
                publisher VARCHAR(255),
                -- 阅读进度，可能为空			
                Progress BIGINT,									
                -- 阅读天数，可能为空
                ReadDay BIGINT,									
                -- 阅读时长，可能为空
                ReadDayTime VARCHAR(255),				
                -- 开始阅读时间，可能为空
                StartDay datetime,								
                -- 最后阅读时间，可能为空
                LastDay datetime,								
                -- 最晚阅读时间，可能为空
                LatestDay VARCHAR(255),					
                -- 阅读链接，可能为空
                ReadUrl VARCHAR(255),						
                -- 书籍字数，可能为空
                wordCount BIGINT,								
                -- 阅读设备，可能为空
                deviceName VARCHAR(255),					
                -- 笔记数，
                notenum BIGINT,									
								
                -- 新增更新时间字段
                `update` TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
                     ON UPDATE CURRENT_TIMESTAMP,
                -- 索引配置
                INDEX idx_title (title),
                INDEX idx_type (type)

            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

          """
        print("\n执行 book_data 建表 SQL:\n" + create_table_sql.strip())
        cursor.execute(create_table_sql)

        print("已创建新表 book_data")


    # 新增：检查表是否存在
    cursor.execute("""
              SELECT COUNT(*)
              FROM information_schema.tables 
              WHERE table_name = 'book_market_data'
          """)

    if cursor.fetchone()[0] == 0:
        # 创建表结构
        create_table_sql = """
                 CREATE TABLE book_market_data (
                    -- 书名
                    title VARCHAR(255) NOT NULL,
                    -- 章节id
                    chapterUid VARCHAR(255),
                    -- 章节名
                    chapterTitle VARCHAR(255),
                    -- 划线内容
                    markText TEXT NOT NULL,
                    -- 笔记内容
                    content TEXT,
                    -- 创建时间
                    createDate datetime,
                    -- 更新时间
                    updateDate datetime,
                        

                    -- 新增更新时间字段
                    `update` TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
                         ON UPDATE CURRENT_TIMESTAMP,
                    -- 索引配置
                    INDEX idx_title (title),
                    UNIQUE idx_markText (markText(255))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

              """
        print("\n执行 book_market_data 建表 SQL:\n" + create_table_sql.strip())
        cursor.execute(create_table_sql)

        print("已创建新表 book_market_data")

    if type == "weread" :

        # 遍历外层数组
        for key,value in data.items():


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

                # 生成INSERT语句（保留原有字段结构）
                sql = '''
                   INSERT INTO book_market_data (
                       title,chapterUid,chapterTitle,markText,content,createDate,updateDate
                   ) VALUES (
                       %s, %s, %s, %s, %s, %s, %s
                   ) ON DUPLICATE KEY UPDATE
                       title = VALUES(title),
                       chapterUid = VALUES(chapterUid),
                       chapterTitle = VALUES(chapterTitle),
                       markText = VALUES(markText),
                       content = VALUES(content),
                       createDate = VALUES(createDate),
                       updateDate = VALUES(updateDate)
                    '''

                # 添加 SQL 打印
                print("\n执行插入 SQL:")
                print(sql.strip())
                print("参数:", insert_market_data)

                # 执行插入
                cursor.execute(sql, insert_market_data)


            # 生成INSERT语句（保留原有字段结构）
            sql = '''
                INSERT INTO book_data (
                    `type`,bookId, title, classification, market,cover, name, isbn, readSign,
                    description,publisher, Progress, ReadDay, ReadDayTime, StartDay, LastDay,
                    LatestDay, ReadUrl, wordCount, deviceName, notenum
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON DUPLICATE KEY UPDATE
                    bookId = VALUES(bookId),
                    title = VALUES(title),
                    classification = VALUES(classification),
                    cover = VALUES(cover),
                    name = VALUES(name),
                    isbn = VALUES(isbn),
                    readSign = VALUES(readSign),
                    description = VALUES(description),
                    publisher = VALUES(publisher),
                    Progress = VALUES(Progress),
                    ReadDay = VALUES(ReadDay),
                    ReadDayTime = VALUES(ReadDayTime),
                    StartDay = VALUES(StartDay),
                    LastDay = VALUES(LastDay),
                    LatestDay = VALUES(LatestDay),
                    ReadUrl = VALUES(ReadUrl),
                    wordCount = VALUES(wordCount),
                    deviceName = VALUES(deviceName),
                    notenum = VALUES(notenum)
            '''

            # 添加 SQL 打印
            print("\n执行插入 SQL:")
            print(sql.strip())
            print("参数:", insert_data)

            # 执行插入
            cursor.execute(sql, insert_data)
    elif type == "du":
        # 遍历外层数组
        for key,value in data.items():
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

            for value1 in value["market"]:
                insert_market_data = (
                    value["title"],
                    value1["articleId"],
                    value1["title"],
                    value1["markText"],
                    value1["remark"],
                    value1["createTime"],
                    value1["uploadTime"],
                )

                # 生成INSERT语句（保留原有字段结构）
                sql = '''
                        INSERT INTO book_market_data (
                            title,chapterUid,chapterTitle,markText,content,createDate,updateDate
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s
                        ) ON DUPLICATE KEY UPDATE
                            title = VALUES(title),
                            chapterUid = VALUES(chapterUid),
                            chapterTitle = VALUES(chapterTitle),
                            markText = VALUES(markText),
                            content = VALUES(content),
                            createDate = VALUES(createDate),
                            updateDate = VALUES(updateDate)
                         '''

                # 添加 SQL 打印
                print("\n执行插入 SQL:")
                print(sql.strip())
                print("参数:", insert_market_data)

                # 执行插入
                cursor.execute(sql, insert_market_data)

            # 生成INSERT语句（保留原有字段结构）
            sql = '''
                     INSERT INTO book_data (
                         `type`,bookId, title, classification, market,cover, name, isbn, readSign,
                         description,publisher, Progress, ReadDay, ReadDayTime, StartDay, LastDay,
                         LatestDay, ReadUrl, wordCount, deviceName, notenum
                     ) VALUES (
                         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                     ) ON DUPLICATE KEY UPDATE
                         bookId = VALUES(bookId),
                         title = VALUES(title),
                         classification = VALUES(classification),
                         cover = VALUES(cover),
                         name = VALUES(name),
                         isbn = VALUES(isbn),
                         readSign = VALUES(readSign),
                         description = VALUES(description),
                         publisher = VALUES(publisher),
                         Progress = VALUES(Progress),
                         ReadDay = VALUES(ReadDay),
                         ReadDayTime = VALUES(ReadDayTime),
                         StartDay = VALUES(StartDay),
                         LastDay = VALUES(LastDay),
                         LatestDay = VALUES(LatestDay),
                         ReadUrl = VALUES(ReadUrl),
                         wordCount = VALUES(wordCount),
                         deviceName = VALUES(deviceName),
                         notenum = VALUES(notenum)
                 '''

            # 添加 SQL 打印
            print("\n执行插入 SQL:")
            print(sql.strip())
            print("参数:", insert_data)

            # 执行插入
            cursor.execute(sql, insert_data)

    elif type == "zhangyue":
        # 遍历外层数组
        for value in data:
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

                # 生成INSERT语句（保留原有字段结构）
                sql = '''
                               INSERT INTO book_market_data (
                                   title,chapterUid,chapterTitle,markText,content,createDate,updateDate
                               ) VALUES (
                                   %s, %s, %s, %s, %s, %s, %s
                               ) ON DUPLICATE KEY UPDATE
                                   title = VALUES(title),
                                   chapterUid = VALUES(chapterUid),
                                   chapterTitle = VALUES(chapterTitle),
                                   markText = VALUES(markText),
                                   content = VALUES(content),
                                   createDate = VALUES(createDate),
                                   updateDate = VALUES(updateDate)
                                '''

                # 添加 SQL 打印
                print("\n执行插入 SQL:")
                print(sql.strip())
                print("参数:", insert_market_data)

                # 执行插入
                cursor.execute(sql, insert_market_data)

            # 生成INSERT语句（保留原有字段结构）
            sql = '''
                            INSERT INTO book_data (
                                `type`,bookId, title, classification, market,cover, name, isbn, readSign,
                                description,publisher, Progress, ReadDay, ReadDayTime, StartDay, LastDay,
                                LatestDay, ReadUrl, wordCount, deviceName, notenum
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            ) ON DUPLICATE KEY UPDATE
                                bookId = VALUES(bookId),
                                title = VALUES(title),
                                classification = VALUES(classification),
                                cover = VALUES(cover),
                                name = VALUES(name),
                                isbn = VALUES(isbn),
                                readSign = VALUES(readSign),
                                description = VALUES(description),
                                publisher = VALUES(publisher),
                                Progress = VALUES(Progress),
                                ReadDay = VALUES(ReadDay),
                                ReadDayTime = VALUES(ReadDayTime),
                                StartDay = VALUES(StartDay),
                                LastDay = VALUES(LastDay),
                                LatestDay = VALUES(LatestDay),
                                ReadUrl = VALUES(ReadUrl),
                                wordCount = VALUES(wordCount),
                                deviceName = VALUES(deviceName),
                                notenum = VALUES(notenum)
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
    print("开始导入 book 数据 --> mysql")

    # 动态生成正确路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(os.path.dirname(current_dir), 'Data_End', 'weread.json')  # 改为平级目录

    # 微信读书
    parse_and_insert(data_path,"weread")

    data_path = os.path.join(os.path.dirname(current_dir), 'Data_End', 'du.json')  # 改为平级目录

    # 网易蜗牛
    parse_and_insert(data_path, "du")

    data_path = os.path.join(os.path.dirname(current_dir), 'Data_End', 'zhangyue.json')  # 改为平级目录

    # 掌阅
    parse_and_insert(data_path, "zhangyue")

    print("数据导入完成")


if __name__ == "__main__":
    main()
