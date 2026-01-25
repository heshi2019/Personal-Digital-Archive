import json
import os
from datetime import datetime

from src.Script.Script_API.gcores_api import GcoresApi
from src.config.configClass import app_config
from src.utils.utils import app_Utils

# 30天对应的Unix毫秒值（固定值）
THIRTY_DAYS_MS = 2592000000

'''
        Args: type = all        全量获取数据
              type = increment  增量获取数据
                默认全量获取
        End文件夹中的Gcores_Categories.json在gcores_api.py中保存
'''
def get_gcores_list(model=None):

    gcores_api = GcoresApi()

    print("开始获取机核电台信息")
    # 初始化播客列表，初步分类
    radiosList,categoriesList,usersList,albumsList = gcores_api.get_Radios(model)

    # 丰富用户信息
    for key,value in usersList.items():
        user =gcores_api.get_User(key)
        # 地点
        location = user.get("attributes",{}).get("location","")
        # 个人签名
        intro = user.get("attributes",{}).get("intro","")
        # 被关注数
        followers_count = user.get("attributes",{}).get("followers-count","")
        # 关注数
        followees_count = user.get("attributes",{}).get("followees-count","")
        # 注册时间
        created_at = user.get("attributes",{}).get("created-at","")
        created_at = datetime.fromisoformat(created_at).strftime('%Y-%m-%d %H:%M:%S')

        usersList[key]["location"] = location
        usersList[key]["intro"] = intro
        usersList[key]["followers-count"] = followers_count
        usersList[key]["followees-count"] = followees_count
        usersList[key]["created-at"] = created_at

    usersList = updateArr(model, "Gcores_User.json", usersList)
    app_Utils.save(app_config.Data_End, "Gcores_User.json", usersList, "txt")

    # 获取每一个专题的具体节目信息
    albumsList = gcores_api.get_Albums(albumsList)

    albumsList = updateArr(model,"Gcores_albums.json",albumsList)
    app_Utils.save(app_config.Data_End, "Gcores_albums.json", albumsList, "txt")

    # 将专题节目信息添加到播客列表中
    for key,value in albumsList.items():
        tempList = albumsList.get(key,{}).get("RadiosList",[])
        for temp in tempList:

            # 获取专辑的每一个节目信息，并添加到播客列表中
            albumsList = gcores_api.get_One_Radios(temp)

            id = albumsList.get("id", "0")

            title = albumsList.get("attributes", {}).get("title", "")
            # 博客时长，秒
            duration = albumsList.get("attributes", {}).get("duration", "")
            # 节目封面图，只是图片名称
            cover = albumsList.get("attributes", {}).get("cover", "")
            # 发布时间
            published_at = albumsList.get("attributes", {}).get("published-at", "")
            published_at = datetime.fromisoformat(published_at).strftime('%Y-%m-%d %H:%M:%S')
            # 点赞数
            likes_count = albumsList.get("attributes", {}).get("likes-count", "")
            # 评论数
            comments_count = albumsList.get("attributes", {}).get("comments-count", "")
            # 节目分类，字典
            category = albumsList.get("relationships", {}).get("category", {}).get("data", {})
            # 参与节目的用户，列表
            userList = []
            user = albumsList.get("relationships", {}).get("djs", {}).get("data", [])
            for value in user:
                userList.append(value.get("id"))

            # 副标题
            desc = albumsList.get("attributes", {}).get("desc", "")
            # 收藏数
            bookmarks_count = albumsList.get("attributes", {}).get("bookmarks-count", "")

            content = ""
            # 节目介绍
            contentList = json.loads(albumsList.get("attributes", {}).get("content", "{}") or "{}")
            for value in contentList.get("blocks", []):
                content = content + value.get("text", "") + "。\n"

            # 节目播放连接
            url ="https://www.gcores.com/radios/"+str(id)

            # 播放量
            plays = albumsList.get("attributes", {}).get("plays", 0)

            radiosList.append({"id": id, "title": title,"desc":desc,"content":content,
                               "duration": duration, "cover": cover,
                               "published_at": published_at, "likes_count": likes_count,
                               "comments_count": comments_count,"bookmarks_count":bookmarks_count,
                               "category": category,"userList": userList,"url":url,"plays":plays})

    radiosList = updateArr(model,"Gcores_Radios.json",radiosList)
    app_Utils.save(app_config.Data_End, "Gcores_Radios.json", radiosList, "txt")

def updateArr(model, fileName,Arr):
    if model is not None:
        data_path = os.path.join(app_config.Data_End, fileName)
        # 读取JSON文件
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        Arr.append(data)

        return Arr
    return Arr

if __name__ == "__main__":
    get_gcores_list()
