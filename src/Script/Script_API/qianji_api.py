import requests
from retrying import retry

from src.config.configClass import app_config
from src.utils.utils import app_Utils

QIANJI_URL = "https://api.qianjiapp.com/syncv2/pull"
QIANJI_CATEGORIES = "https://api.qianjiapp.com/asset/list"


# 请求头 tok和reqidv2为动态计算

class QianJiApi:

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def get_catefories(self):
        cookie = app_config.qianji_cookie
        headers = {
            "devid": "5bdc9946fec22964881cb62b2cd3f697",
            "os": "1",
            "ctrl": "asset",
            "cregion": "CN",
            "pkg": "com.mutangtech.qianji",
            "vsn": "4.2.2",
            "tok": "",
            "act": "list",
            "devbrand": "XIAOMI",
            "clang": "zh",
            "osvs": "32",
            "timezoneoffset": "28800",
            "utoken": cookie,
            "devname": "24031PN0DC",
            "reqidv2": "e27ff8d40a1aa6b7d9ebe3b5d7c591b3",
            "vs": "994",
            "mk": "xiaomi",
            "htoken": "1",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 12; 24031PN0DC Build/c086e2e.0)",
            "Host": "api.qianjiapp.com",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip",
            "Content-Length": "60"
        }

        r = requests.post(QIANJI_CATEGORIES,headers=headers, verify=False)

        if r.ok:
            data = r.json()
            # 筛选数据
            app_Utils.save(app_config.data_star, "qianji_catefories_data.json",data.get("data",""), "txt")

        else:
            raise Exception(f"获取钱迹数据失败： {r}")
        return data.get("data","")


    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def get_data(self):
        cookie = app_config.qianji_cookie
        headers = {
          "devid": "5309ed2dcd6ce907ed7a6222583cd64c",
          "os": "1",
          "ctrl": "syncv2",
          "cregion": "CN",
          "pkg": "com.mutangtech.qianji",
          "vsn": "4.2.2",
          "tok": "afb72a21186cc6c4b7ad861c06380d7e",
          "act": "pull",
          "devbrand": "XIAOMI",
          "clang": "zh",
          "osvs": "32",
          "utoken": cookie,
          "devname": "24031PN0DC",
          "reqidv2": "8e871dd1346d9c4240656cef8010d7cb",
          "vs": "994",
          "mk": "xiaomi",
          "htoken": "1",
          "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
          "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 12; 24031PN0DC Build/c086e2e.0)",
          "Host": "api.qianjiapp.com",
          "Connection": "Keep-Alive",
          "Accept-Encoding": "gzip",
          "Content-Length": "127"
        }

        r = requests.post(QIANJI_URL,headers=headers)

        re = r.json()
        code = re.get("ec")

        if r.ok:
            data = r.json()
            # 筛选数据
            app_Utils.save(app_config.data_star, "qianji_data.json",data.get("data",""), "txt")

        else:
            raise Exception(f"获取钱迹数据失败： {r}")
        return data.get("data","")


