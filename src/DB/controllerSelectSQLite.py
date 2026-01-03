from src.DB.SQLite_util import SQLite_util


class controllerSelectSQLite:
    def __init__(self, db: SQLite_util):
        self.db = db


    def select_douban_data(self):
        """
        从指定表中查询所有数据
        :param table_name: 表名
        :return: 查询结果列表
        """
        sql = f"select title,type,myComment_comment,myComment_MyValue,myComment_create_time from douban_data order by myComment_create_time"
        return self.db.query(sql)

    def select_files_data(self):
        sql = f"select type,path,created_at from files order by created_at"
        return self.db.query(sql)