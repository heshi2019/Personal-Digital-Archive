import time
from datetime import datetime
from InternetCrawler.src.Config.config_manager import config
from InternetCrawler.src.flomo_api import FlomoApi
from InternetCrawler.src.ImpoetMySQL import import_flomom

# 2025.12.08
# 坏消息是，这个自动化脚本没法用
# 好消息是，我有一个脚本用来整理从flomo导出的html文件（BasicData/flomo/flomo_index_analysis.py）

# 2025.12.22
# 脚本可用，需要从浏览器拿authorization签名而不是cookie
# 同时，还有一个脚本用来整理从flomo导出的html文件（BasicData/flomo/flomo_index_analysis.py）




# 30天对应的Unix毫秒值（固定值）
THIRTY_DAYS_MS = 2592000000

'''
        Args: type = all        全量获取数据
              type = increment  增量获取数据
                默认全量获取
'''
def get_flomo_list(model=None):

    print("开始获取flomo数据")

    flomo_api = FlomoApi()
    data = flomo_api.get_memo_list()

    # 解析返回数据
    dataList = []
    for item in data:
        created = item.get("created_at", None)

        # 全量获取
        if model == "all" or model is None:
            pass
        # 增量获取，只获取最近30天内更新的书籍
        elif model == "increment":
            # 使用strptime函数，将字符串解析为datetime对象，timestamp()方法将datetime对象转换为时间戳（秒级）
            LastReadTime = int(datetime.strptime(created, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
            nowTime = int(time.time() * 1000)
            if LastReadTime >= nowTime - THIRTY_DAYS_MS:
                pass
                # 如果数据的最后阅读时间大于当前时间，则增量获取数据
            else:
                continue


        content = item.get("content", None)
        file = item.get("files", [])
        files = []

        for file in file:
            files.append(file.get("path", None))

        dataList.append({
            "time": created,
            "content": content,
            "files": files
        })

    # 按日期时间降序排序
    dataList.sort(
        key=lambda x: datetime.strptime(x['time'], "%Y-%m-%d %H:%M:%S"),
        reverse=True
    )

    config.save("Data_End", "flomo.json", dataList, "txt")



if __name__ == "__main__":
    get_flomo_list()
