from datetime import datetime
from InternetCrawler.src.Config.config_manager import config
from InternetCrawler.src.flyme_api import FlymeApi
from InternetCrawler.src.ImpoetMySQL import import_flyme

def main():
    print("开始获取魅族便签笔记")
    flyme_api = FlymeApi()

    # 获取分类，这个分类中还有每个分类所包含的笔记个数
    Classification = flyme_api.get_Classification()

    # 将分类的id和名称整合为一个字典
    ClassificationDict = {}
    for item in Classification:
        ClassificationDict[item.get("id")] = item.get("name")

    # 获取笔记数据
    data = flyme_api.get_Flyme_data()
    # 格式转换
    flyme_list_data = []
    for index,item in enumerate(data):

        # unix时间戳转换为可视格式
        lastUpdate = datetime.fromtimestamp(item.get("lastUpdate") / 1000).strftime(
            '%Y-%m-%d %H:%M:%S') if item.get("lastUpdate") else None
        createTime = datetime.fromtimestamp(item.get("createTime") / 1000).strftime(
            '%Y-%m-%d %H:%M:%S') if item.get("createTime") else None
        modifyTime = datetime.fromtimestamp(item.get("modifyTime") / 1000).strftime(
            '%Y-%m-%d %H:%M:%S') if item.get("modifyTime") else None
        topdate = datetime.fromtimestamp(item.get("topdate") / 1000).strftime(
            '%Y-%m-%d %H:%M:%S') if item.get("topdate") else None

        print(f"正在同步魅族便签笔记,一共{len(data)}条，当前是第{index + 1}条，创建时间：{createTime}")

        # body中，state为0和1的是文字，state为3和4的是图片
        temp = {"uuid": item.get("uuid"),"lastUpdate":lastUpdate,
                "createTime":createTime,"modifyTime":modifyTime,
                "body":item.get("body"),"title":item.get("title"),"firstImg":item.get("firstImg"),
                "fileList":item.get("fileList"),"topdate":topdate,"files":item.get("files"),
                "firstImgSrc":item.get("firstImgSrc"),
                "groupStatus":ClassificationDict.get(item.get("groupStatus"))}
        flyme_list_data.append(temp)

    # 将笔记文字部分输出到json文件
    config.save("Data_End", "flyme.json", flyme_list_data, "txt")

    #整理图片链接 部分，其实这里可以直接通过API下载图片，但为了知道总图片和目前下载图片个数，选择了先整理，再下载

    imgList = {}
    for item in flyme_list_data:
        for key,value in item["files"].items():
            imgList[key] = value

    # 图片下载部分
    for index,(key,value) in enumerate(imgList.items()):
        print(f"开始下载图片，一共{len(imgList)},当前是第{index+1}张")
        flyme_api.get_FirstImg(imgList.get(key))


if __name__ == "__main__":
    main()





