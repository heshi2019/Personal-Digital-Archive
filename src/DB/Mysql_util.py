import mysql.connector

from src.config.configClass import app_config

class Mysql_util:
    def connect_to_mysql(self):
        host = app_config.MySQL.host
        user = app_config.MySQL.user
        password = app_config.MySQL.password
        database = app_config.MySQL.database

        # 连接数据库
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()
        return conn, cursor

