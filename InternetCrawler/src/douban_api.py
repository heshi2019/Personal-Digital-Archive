import os
import requests
from retrying import retry
from InternetCrawler.src.Config.config_manager import config

DOUBAN_API_HOST = os.getenv("DOUBAN_API_HOST", "frodo.douban.com")
DOUBAN_API_KEY = os.getenv("DOUBAN_API_KEY", "0ac44ae016490db2204ce0a042db2916")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")

class DouBanApi:
    def __init__(self):
        pass

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def fetch_subjects(self,status):

        user = config.get_key("DOUBAN", "user")
        if user is None:
            user = os.getenv("DOUBAN_USERNAME")

        headers = {
            "host": DOUBAN_API_HOST,
            "authorization": f"Bearer {AUTH_TOKEN}" if AUTH_TOKEN else "",
            "user-agent": "User-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 15_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.16(0x18001023) NetType/WIFI Language/zh_CN",
            "referer": "https://servicewechat.com/wx2f9b06c1de1ccfca/84/page-frame.html",
        }

        offset = 0
        page = 0
        results = []
        url = f"https://{DOUBAN_API_HOST}/api/v2/user/{user}/interests"

        while True:
            params = {
                "type": "movie",
                "count": 50,
                "status": status,
                "start": offset,
                "apiKey": DOUBAN_API_KEY,
            }

            response = requests.get(url, headers=headers, params=params)
            if response.ok:
                response = response.json()
                interests = response.get("interests")
                if len(interests) == 0:
                    break
                results.extend(interests)
                page += 1
                offset = page * 50
        # 将笔记文字部分输出到json文件
        config.save("Data_Star", "douban_"+status+".json", results, "txt")

        return results

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def getErr_id(self,id):
        headers = {
            "referer":"https://m.douban.com/",
            "user-agent": "User-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 15_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.16(0x18001023) NetType/WIFI Language/zh_CN",
        }

        url = f"https://movie.douban.com/subject/"+id+"/"

        response = requests.get(url, headers=headers)

        if response.ok:
            # 保存数据
            config.save("Data_Star","douban_"+id+".json", response.text.strip(),"json")

            return response.text.strip()