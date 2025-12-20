import json
from InternetCrawler.src.Config.config_manager import config
from InternetCrawler.src.gcores_api import GcoresApi

def main():

    gcores_api = GcoresApi()

    print("开始获取机核电台信息")
    # 初始化播客列表，初步分类
    radiosList,categoriesList,usersList,albumsList = gcores_api.get_Radios()

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

        usersList[key]["location"] = location
        usersList[key]["intro"] = intro
        usersList[key]["followers-count"] = followers_count
        usersList[key]["followees-count"] = followees_count
        usersList[key]["created-at"] = created_at

    config.save("Data_End", "Gcores_User.json", usersList, "txt")

    # 获取每一个专题的具体节目信息
    albumsList = gcores_api.get_Albums(albumsList)

    config.save("Data_End", "Gcores_albums.json", albumsList, "txt")

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

            radiosList.append({"id": id, "title": title,"desc":desc,"content":content,
                               "duration": duration, "cover": cover,
                               "published_at": published_at, "likes_count": likes_count,
                               "comments_count": comments_count,"bookmarks_count":bookmarks_count,
                               "category": category,"userList": userList,"url":url})

    config.save("Data_End", "Gcores_Radios.json", radiosList, "txt")


if __name__ == "__main__":
    main()
