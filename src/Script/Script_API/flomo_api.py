import hashlib
import time
import requests

from src.config.configClass import app_config
from src.utils.utils import app_Utils

FLOMO_DOMAIN = "https://flomoapp.com"
MEMO_LIST_URL = FLOMO_DOMAIN + "/api/v1/memo/updated/"

HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'origin': 'https://v.flomoapp.com',
    'priority': 'u=1, i',
    'referer': 'https://v.flomoapp.com/',
    'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
}


class FlomoApi:

    def get_memo_list(self):
        # 获取当前时间

        latest_updated_at = 1

        user_authorization = app_config.flomo_authorization

        memo_list = []
        while True:
            current_timestamp = int(time.time())

            # 构造参数
            params = {
                'limit': '200',
                'latest_updated_at': latest_updated_at,
                'tz': '8:0',
                'timestamp': current_timestamp,
                'api_key': 'flomo_web',
                'app_version': '4.0',
                'platform': 'web',
                'webp': '1'
            }

            # 获取签名
            params['sign'] = self.getSign(params)
            HEADERS['authorization'] = f'Bearer {user_authorization}'

            response = requests.get(MEMO_LIST_URL, headers=HEADERS, params=params)

            if response.status_code != 200:
                # 网络或者服务器错误
                print(f"网络或者服务器错误: {response.text}")
                return

            response_json = response.json()
            if response_json['code'] != 0:
                print(f"flomo返回数据错误: {response_json['message']}")
                return

            if not response_json['data']:
                print(f"flomo无返回数据: {response_json}")
                break

            memo_list.extend(response_json['data'])

            latest_updated_at = str(
                int(time.mktime(time.strptime(response_json['data'][-1]['updated_at'], "%Y-%m-%d %H:%M:%S"))))

            if len(response_json['data']) < 200:
                print(f"flomo数据返回结束，目前步长: {latest_updated_at}")
                break

        app_Utils.save(app_config.Data_Star, "Flomo_Data.json", memo_list, "txt")

        return memo_list

    def getSign(self,e):
        e = dict(sorted(e.items()))

        t = ""
        for i in e:
            o = e[i]
            if o is not None and (o or o == 0):
                if isinstance(o, list):
                    o.sort(key=lambda x: x if x else '')
                    for item in o:
                        t += f"{i}[]={item}&"
                else:
                    t += f"{i}={o}&"
        t = t[:-1]

        t = t + "dbbc3dd73364b4084c3a69346e0ce2b2"
        sign_str = hashlib.md5(t.encode('utf-8')).hexdigest()

        return sign_str

