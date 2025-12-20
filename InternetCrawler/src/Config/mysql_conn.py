# import mysql.connector

from InternetCrawler.src.Config.config_manager import config

class mysql_conn:
    def connect_to_mysql(self):
        host = config.get_key("MYSQL", "host")
        user = config.get_key("MYSQL", "user")
        password = config.get_key("MYSQL", "password")
        database = config.get_key("MYSQL", "database")

        # 连接数据库
        # conn = mysql.connector.connect(
        #     host=host,
        #     user=user,
        #     password=password,
        #     database=database
        # )
        # cursor = conn.cursor()
        # return conn, cursor

