import json
import re
import os
import time
from datetime import datetime

import requests
from requests.utils import cookiejar_from_dict
from retrying import retry

from src.config.configClass import app_config
from src.utils.utils import app_Utils

# 播客
Gcores_Radios_URL = "https://www.gcores.com/gapi/v1/latest-radios"

# 首页
Gcores_URL = "https://www.gcores.com"

# 用户id
Gcores_USER_URL = "https://www.gcores.com/gapi/v1/users/"

# 某个类型的电台
# https://www.gcores.com/gapi/v1/categories/45/originals

# 专题页
# Gcores_Subject_URL = "https://www.gcores.com/gapi/v1/collections"

# 获取连载博客
# Gcores_Albums_URL = "https://www.gcores.com/gapi/v1/albums/152/published-audiobooks"

# 某一期播客
Gcores_One_Radios_URL = "https://www.gcores.com/gapi/v1/radios/"

class GcoresApi:
    def __init__(self):
        self.cookie = self.get_cookie()
        self.session = requests.Session()
        self.session.cookies = self.parse_cookie_string()
        # self.session.verify = False  # 禁用SSL验证
        # requests.packages.urllib3.disable_warnings()  # 禁用SSL警告

    def get_cookie(self):

        cookie = app_config.Grcores
        if cookie is None:
            cookie = os.getenv("GRCORES_COOKIE")

        if not cookie or not cookie.strip():
            raise Exception("没有找到cookie，请按照文档填写cookie")
        return cookie

    def parse_cookie_string(self):
        cookies_dict = {}

        # 使用正则表达式解析 cookie 字符串
        pattern = re.compile(r'([^=]+)=([^;]+);?\s*')
        matches = pattern.findall(self.cookie)

        for key, value in matches:
            cookies_dict[key] = value.encode('unicode_escape').decode('ascii')
        # 直接使用 cookies_dict 创建 cookiejar
        cookiejar = cookiejar_from_dict(cookies_dict)

        return cookiejar

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def get_HomePage(self):

        headers = {
            "Host": "www.gcores.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        }
        params = {
            "start": 0,
            "length": 1000,
            "groupUuid": -1
        }
        self.session.get(Gcores_URL, headers=headers, params=params)

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def get_Radios(self):
        radiosList = []
        categoriesList = {}
        usersList = {}
        albumsList = {}

        """获取播客列表"""
        self.get_HomePage()

        # 添加重试标记位
        max_retries = 3
        retry_count = 0

        i = 0  # 改为 while 循环以便手动控制迭代

        while i < 100000:

            params = {
                "page[limit]": "20",
                "page[offset]": i,
                "include": "radio.category,radio.djs,album",
                "fields[radios]": "title,duration,cover,thumb,published-at,is-published,is-verified,likes-count,comments-count,category,albums,djs",
                "fields[categories]": "name,logo,subscriptions-count",
                "fields[albums]": "title,cover,is-require-privilege,content-type,is-free,owner-type",
                "fields[users]": "nickname,thumb",
                "meta[radios]": ",",
                "meta[categories]": ",",
                "meta[albums]": ",",
                "meta[users]": ",",
                # "Cookie": "navAppDownload=2; navJizuFlag=1; navDiscussionFlag=5; navGfusionFlag=5; navBooom2024Flag=5; p_h5_u=733EF180-4BCE-487C-A9E0-ACA552155DC2; acw_tc=0bdd344e17439250001023874e8ecb3d8fcbcddb4fcf2a780b25d050010821; wechatTicket=kgt8ON7yVITDhtdwci0qeVh-oVHCGueploMGrzaCyq87pBNJf7LpDz5vK90E0VmCz1voJbZYIVOZV8zr4tcC1Q; sensorable=true; __snaker__id=Y5P5rCZPWWdq3d1h; gdxidpyhxdE=DyNx9HDU5tTOie1LRrg%2FqbWld2MCMxArkis9GfQy2jUzzf3wTEnIbAXg6P6vf%2Bg7%2FaCtvSmXXnCvpV994qhZTBkJD%5CjzsjJlCW9%5CozwCb5kAsTkOit1JZA6mt0sizBiXEKz3%2FRKZJR3wz9O75rCVWU0XpDxr7dypvwnxHOCG4xaUx08c%3A1743925905823; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22193263a1135581-0796947914ffee4-26011951-1821369-193263a11362921%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_landing_page%22%3A%22https%3A%2F%2Fwww.gcores.com%2F%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTkzMjYzYTExMzU1ODEtMDc5Njk0NzkxNGZmZWU0LTI2MDExOTUxLTE4MjEzNjktMTkzMjYzYTExMzYyOTIxIn0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%22193263a1135581-0796947914ffee4-26011951-1821369-193263a11362921%22%7D",
            }

            headers = {
                "Host": "www.gcores.com",
                "Connection": "keep-alive",
                "sec-ch-ua-platform": "Windows",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
                "Content-Type": "application/vnd.api+json",
                "sec-ch-ua-mobile": "?0",
                "Accept": "*/*",
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Dest": "empty",
                "Referer": "https://www.gcores.com/radios",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            }
            print(f"正在同步第{i/20}页，已同步{i}条数据")

            # 请注意，get_saveRadiosData函数的任何异常和报错，会向外层传递，因此会导致最外层的本函数报错，
            # 而如果没有try-except捕获异常，会直接触发本函数的异常机制，也就是请求重试

            # 说实话，在请求这里不应该调用其他函数，因为会导致异常扩散
            try:

                r = self.session.get(Gcores_Radios_URL, headers=headers, params=params)
                if r.json().get("data",[]) == []:
                    break
                if r.ok:
                    data = r.json().get("included")
                    # 筛选数据
                    self.get_saveRadiosData(data,
                                            radiosList,categoriesList,usersList,albumsList)
                    i += 20  # 只有成功时才递增
                    retry_count = 0  # 重置重试计数器
                else:
                    raise Exception(f"获取机核博客失败： {r}")

            except Exception as e:
                if retry_count < max_retries:
                    print(f"第{i}条数据请求失败，正在重试 ({retry_count + 1}/{max_retries})")
                    retry_count += 1
                    time.sleep(5)
                else:
                    print(f"达到最大重试次数，跳过第{i}条数据")
                    i += 20  # 强制前进以避免死循环
                    retry_count = 0

        app_Utils.save(app_config.Data_Star, "Gcores_Radios.json", radiosList, "txt")
        app_Utils.save(app_config.Data_End, "Gcores_Categories.json", categoriesList, "txt")
        app_Utils.save(app_config.Data_Star, "Gcores_User.json", usersList, "txt")
        app_Utils.save(app_config.Data_Star, "Gcores_albums.json", albumsList, "txt")

        return radiosList,categoriesList,usersList,albumsList
    # 数据筛选
    def get_saveRadiosData(self,radios,
                           radiosList,categoriesList,usersList,albumsList):

        for item in radios:
            # 博客
            if item.get("type") == "radios":

                id = item.get("id", "0")

                # 获取一个播客详情
                temp = self.get_One_Radios(id)

                # 副标题
                desc = temp.get("attributes", {}).get("desc", "")
                # 收藏数
                bookmarks_count = temp.get("attributes", {}).get("bookmarks-count", "")

                content = ""
                # 节目介绍
                # 注意字典的get方法，只有在key不存在时，才会返回默认值，如果key存在但值为None，不会返回默认值。
                contentList = json.loads(temp.get("attributes", {}).get("content","{}") or "{}")
                for value in contentList.get("blocks",[]):
                    content = content + value.get("text", "") + "。\n"

                title = item.get("attributes", {}).get("title", "")
                # 博客时长，秒
                duration = item.get("attributes", {}).get("duration", "")
                # 节目封面图，只是图片名称
                cover = item.get("attributes", {}).get("cover", "")
                # 发布时间
                published_at = item.get("attributes", {}).get("published-at", "")
                published_at = datetime.fromisoformat(published_at).strftime('%Y-%m-%d %H:%M:%S')
                # 点赞数
                likes_count = item.get("attributes", {}).get("likes-count", "")
                # 评论数
                comments_count = item.get("attributes", {}).get("comments-count", "")
                # 节目分类，字典
                category = item.get("relationships", {}).get("category", {}).get("data", {})
                # 参与节目的用户，列表
                userList = []
                user = item.get("relationships", {}).get("djs", {}).get("data", [])
                for value in user:
                    userList.append(value.get("id"))
                # 节目播放连接
                url = "https://www.gcores.com/radios/" + str(id)
                radiosList.append({"id": id, "title": title,"desc":desc,"content":content,
                                   "duration": duration, "cover": cover,
                                   "published_at": published_at, "likes_count": likes_count,
                                   "comments_count": comments_count,"bookmarks_count":bookmarks_count,
                                   "category": category,"userList": userList,"url":url})
            # 分类
            elif item.get("type") == "categories":
                if categoriesList.get("id", None) is None:
                    # 分类id
                    id = item.get("id", "0")
                    # 专题名称
                    name = item.get("attributes", {}).get("name", "")
                    # 专题图片
                    categorieImages = item.get("attributes", {}).get("logo", "")
                    # 订阅数
                    subscriptions_count = item.get("attributes", {}).get("subscriptions-count", 0)
                    # 分类链接
                    url = "https://www.gcores.com/categories/"+str(id)

                    categoriesList[id] = {"name": name, "images": categorieImages,
                                          "subscriptions_count": subscriptions_count,
                                          "url":url}
            # 参与节目用户
            elif item.get("type") == "users":
                if usersList.get("id", None) is None:
                    # 用户id
                    id = item.get("id", "0")
                    # 用户昵称
                    nickname = item.get("attributes", {}).get("nickname", "")
                    # 用户头像
                    nicknameImages = item.get("attributes", {}).get("thumb", "")
                    # 用户连接
                    url = "https://www.gcores.com/users/"+str(id)+"/talks"

                    usersList[id] = {"nickname": nickname, "images": nicknameImages,"url":url}
            # 连载专题
            elif item.get("type") == "albums":
                if albumsList.get("id", None) is None:
                    id = item.get("id")
                    title = item.get("attributes",{}).get("title","")
                    # 封面
                    cover = item.get("attributes",{}).get("cover","")
                    # 是否需要会员
                    is_require_privilege = item.get("attributes",{}).get("is-require-privilege","")
                    # 是否免费，单独付费内容
                    is_free = item.get("attributes",{}).get("is-free","")
                    # 专题链接
                    url = "https://www.gcores.com/albums/"+str(id)

                    albumsList[id] = {"title":title,"cover":cover,"is_require_privilege":is_require_privilege,
                                      "is_free":is_free,"url":url}

            else:
                print(f"未分类数据：{item}")

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def get_Albums(self,albumsIds):

        for key,value in albumsIds.items():

            Gcores_Albums_URL = "https://www.gcores.com/gapi/v1/albums/"+str(key)+"/published-audiobooks"
            print(f"正在同步连载播客:{value.get("title")}")
            for i in range(0, 200, 20):
                params = {
                    "page[limit]": "20",
                    "page[offset]": i,
                    "include": "media,category,albums",
                    "fields[categories]": "name",
                    "fields[radios]": "title,is-free,is-require-privilege,is-limited-free,published-at,duration,comments-count,cover,media,category,albums"
                }

                r = self.session.get(Gcores_Albums_URL,params=params)
                if r.json().get("data",[]) == []:
                    break
                if r.ok:
                    data = r.json()
                    # 筛选数据
                    self.get_saveAlbumsData(data,key,albumsIds)
                else:
                    raise Exception(f"获取机核博客失败： {r}")
        return albumsIds

    # 添加专题所属节目与介绍，直接处理操作比较优雅，但处理数据嵌套太麻烦了，我再也不想碰了
    def get_saveAlbumsData(self,albums,id,albumsList):

        temp = []
        for item in albums.get("data",[]):
            temp.append(item.get("id"))

        # albumsList是一个列表，列表中的字典才是要新增数据的地方

        # 专题节目列表
        if albumsList.get(id,{}).get("RadiosList",None) is None:
            albumsList[id]["RadiosList"] = temp
        else:
            albumsList[id]["RadiosList"] = albumsList[id]["RadiosList"] + temp

        # 增加专题介绍
        included = albums.get("included", [])
        if albumsList.get(id).get("description",None) is None:
            description = included[len(included)-1].get("attributes",{}).get("description","")
            albumsList[id]["description"] = description

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def get_One_Radios(self,id):

        params = {
            "include": "category,user,djs,media,albums.album-bundles,user.role,tags,entities,entries,similarities.user,similarities.djs,similarities.category,collections,operational-events.giveaways.winners,operational-events.public-candidates,portfolios"
        }

        headers = {
            "Host": "www.gcores.com",
            "Connection": "keep-alive",
            "sec-ch-ua-platform": "Windows",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Content-Type": "application/vnd.api+json",
            "sec-ch-ua-mobile": "?0",
            "Accept": "*/*",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://www.gcores.com/radios",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
        print(f"正在同步播客详情:{id}")
        r = self.session.get(Gcores_One_Radios_URL+str(id), headers=headers, params=params)

        if r.ok:
            data = r.json().get("data")
            return data
        else:
            raise Exception(f"获取机核播客详情失败： {r}")

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def get_User(self,id):

        params ={
            "include": "entities.involvements.entry,latest-editorial-article",
            "ctrlfields[users]": "+editorial-articles-count"
        }

        headers ={
            "Host": "www.gcores.com",
            "Connection": "keep-alive",
            "sec-ch-ua-platform": "Windows",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "sec-ch-ua": "Google Chrome;v=135,Not-A.Brand;v=8, Chromium;v=135",
            "Content-Type": "application/vnd.api+json",
            "sec-ch-ua-mobile": "?0",
            "Accept": "*/*",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://www.gcores.com/users/"+str(id),
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

        print(f"正在同步:{id} 用户")

        try:

            r = self.session.get(Gcores_USER_URL+str(id), headers=headers, params=params)

            if r.ok:
                data = r.json().get("data")
                return data
            else:
                raise Exception(f"获取机核用户详情失败： {r}")
        except Exception as e:
            print(f"获取机核用户详情失败： {e}\n"
                  f"用户ID：{id}\n"
                  f"返回值：{r.text}")
