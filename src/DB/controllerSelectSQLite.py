from src.DB.SQLite_util import SQLite_util
from src.config.configClass import app_config


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

    def select_gcores_radios(self, page=1, page_size=20, sort=None):
        """
        查询Gcores radios数据，支持分页和排序
        :param page: 页码
        :param page_size: 每页显示数量
        :param sort: 排序规则，格式为[{"field": "field_name", "order": "asc|desc"}]
        :return: 查询结果列表
        """
        # 构建排序语句
        order_by = []
        if sort:
            for item in sort:
                field = item.get('field')
                order = item.get('order', 'asc')
                if field:
                    order_by.append(f"{field} {order}")
        order_by_clause = "order by " + ", ".join(order_by) if order_by else ""
        
        # 构建分页语句
        offset = (page - 1) * page_size
        limit = page_size
        
        # 构建SQL语句，使用正确的表名
        sql = f"select * from gcores_radios_data {order_by_clause} limit {limit} offset {offset}"
        return self.db.query(sql)

    def get_daily_program_count(self):
        """
        统计每个日期发布的节目数量
        :return: 查询结果列表
        """
        sql = """
             SELECT
                DATE(published_at) AS publish_date,
                COUNT(*) AS daily_program_count,
                GROUP_CONCAT(title, ' <|> ') AS titles
                FROM gcores_radios_data
                WHERE category != 93
                GROUP BY DATE(published_at)
                ORDER BY publish_date;
               """
        return self.db.query(sql)

    def get_daily_program_count_exclude_resident(self):
        """
        统计每个日期发布的节目数量（去掉入驻博客）
        :return: 查询结果列表
        """
        sql = """
        SELECT DATE(published_at) AS publish_date, 
               COUNT(*) AS daily_program_count,
               GROUP_CONCAT(title, ' <|> ') AS titles  
        FROM gcores_radios_data 
        where category != 93
        GROUP BY publish_date
        ORDER BY publish_date;
        """
        return self.db.query(sql)
    
    def get_program_user_count(self):
        """
        计算每个节目参与的用户个数，并展示URL（去掉入驻博客）
        :return: 查询结果列表
        """
        sql = """
        SELECT id, title, url, published_at, 
               LENGTH(userList) - LENGTH(REPLACE(userList, ',', '')) + 1 AS user_count
        FROM gcores_radios_data 
        where category != 93;
        """
        return self.db.query(sql)
    
    def get_program_duration_by_time(self):
        """
        按发布时间时间线展示每个节目的时长（去掉入驻博客）
        :return: 查询结果列表
        """
        sql = """
        SELECT
          id,
          title,
          published_at,
          duration
        FROM gcores_radios_data 
        WHERE category != 93
        ORDER BY published_at;
        """
        return self.db.query(sql)
    
    def get_user_program_count(self):
        """
        统计每个用户参加节目的次数（按次数降序排列）
        :return: 查询结果列表
        """
        sql = """
        SELECT
          u.id AS user_id,
          u.name,
          u.url AS user_url,
          COUNT(DISTINCT g.id) AS programs_joined
        FROM gcores_radios_data AS g
        JOIN json_each(g.userList) AS je
        JOIN gcores_user_data AS u
          ON u.id = CAST(je.value AS INTEGER)
        WHERE g.category != 93
        GROUP BY u.id, u.name, u.url
        ORDER BY programs_joined DESC, u.id;
        """
        return self.db.query(sql)
    
    def get_daily_registered_users(self):
        """
        按日期统计每天注册用户数，并给出累计用户总数
        :return: 查询结果列表
        """
        sql = """
        SELECT
          register_date,
          daily_registered_users,
          SUM(daily_registered_users) OVER (ORDER BY register_date) AS total_users_to_date
        FROM (
          SELECT
            DATE(created_at) AS register_date,
            COUNT(*) AS daily_registered_users
          FROM gcores_user_data 
          GROUP BY DATE(created_at)
        )
        ORDER BY register_date;
        """
        return self.db.query(sql)

if __name__ == "__main__":
    db = SQLite_util(app_config.SQLitePath)
    controllerSelectSQLite = controllerSelectSQLite(db)

    print(controllerSelectSQLite.select_gcores_radios())
    print(controllerSelectSQLite.get_daily_program_count())
