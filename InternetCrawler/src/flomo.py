from datetime import datetime
from InternetCrawler.src.Config.config_manager import config
from InternetCrawler.src.flomo_api import FlomoApi
from InternetCrawler.src.ImpoetMySQL import import_flomom

# 2025.12.08
# 坏消息是，这个自动化脚本没法用
# 好消息是，我有一个脚本用来整理从flomo导出的html文件（BasicData/flomo/flomo_index_analysis.py）

def main():
    print("开始获取flomo数据")

    flomo_api = FlomoApi()
    data = flomo_api.get_memo_list()

    # 解析返回数据
    dataList = []
    for item in data:
        created = item.get("created_at", None)
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
    main()
