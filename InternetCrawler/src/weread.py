import time

import pendulum
from InternetCrawler.src.Config.config_manager import config
from InternetCrawler.src.weread_api import WeReadApi

# 所有数据信息，全局变量
book_message = None

# 30天对应的Unix毫秒值（固定值）
THIRTY_DAYS_MS = 2592000000
'''

        Args: type = all        全量获取数据
              type = increment  增量获取数据
'''

    # 2025.12.08
    # 坏消息是，这个接口已经不能用
    # 好消息是，我也不用微信读书很久了
def get_weread_book_list(model=None):
    # 清空
    global book_message
    book_message = {}

    we_read_api = WeReadApi()

    # 这个函数实际上是使用了WEREAD_HISTORY_URL这个接口，每日阅读历史，数据暂不处理
    # api_data = we_read_api.get_api_data()
    # print(f"we_read_api.get_api_data接口返回了什么数据，输出格式如下：{api_data}")
    # 返回目前书架，数据暂不处理
    # bookshelf_books = we_read_api.get_bookshelf()
    # print(f"we_read_api.get_bookshelf接口返回了什么数据，输出格式如下：{bookshelf_books}")


    #微信读书API，获取基本数据
    books = we_read_api.get_notebooklist()

    if books != None:
        for index, book in enumerate(books):


            title = book.get("book").get("title")

            print(f"正在同步《{title}》,一共{len(books)}本，当前是第{index + 1}本。")

            # 书籍id
            bookId = book.get("bookId")
            # 书名
            title = book.get("book").get("title")
            # 作者
            name = book.get("book").get("author")
            # 封面
            cover = book.get("book").get("cover")


            # 书籍信息
            get_bookinfo = we_read_api.get_bookinfo(bookId)
            # 简介
            briefIntroduction = get_bookinfo.get("intro")
            # 书籍分类获取
            classification1 = None
            classification2 = None
            classification3 = None

            print(f"{book}")

            if book.get("categories"):
                classification1 = book.get("categories", {}).get("title",None)
                classification2 = book.get("book", {}).get("categories",[])[0].get("title",None)

            classification3 = get_bookinfo.get("category",None)
            # 书籍分类
            classification = classification1 or classification2 or classification3

            # 章节阅读信息
            readInfo = we_read_api.get_read_info(bookId)


            # 全量获取
            if model == "all" or model is None:
                pass

            # 增量获取，只获取最近30天内更新的书籍
            elif model == "increment":
                LastReadTime = readInfo.get("readDetail").get("lastReadingDate")
                nowTime = int(time.time() * 1000)
                if LastReadTime >= nowTime - THIRTY_DAYS_MS:
                    pass
                    # 如果数据的最后阅读时间大于当前时间，则增量获取数据
                else:
                    continue



            # 阅读状态
            readSign = readInfo.get("markedStatus")
            if readSign == 4:
                readSign = "已读完"
            elif readSign == 1:
                readSign = "未读"
            else:
                readSign = "在读"

            # isbn
            isbn = readInfo.get("isbn")

            # 阅读进度  百分比
            Progress = readInfo.get("readingProgress")
            # 阅读时间 格式时分秒
            ReadTime = readInfo.get("readingTime")
            if ReadTime:
                seconds = int(ReadTime)
                hours = seconds // 3600
                minutes = (seconds % 3600) // 60
                seconds = seconds % 60
                ReadDayTime = f"{hours}:{minutes}:{seconds}"
            # 阅读天数
            ReadDay = readInfo.get("totalReadDay")
            # 开始阅读时间
            StartDay = pendulum.from_timestamp(
                readInfo.get("readDetail").get("beginReadingDate")).to_datetime_string() \
                if readInfo.get(
                "readDetail", {}).get("beginReadingDate",None) else None
            # 最后阅读时间
            LastDay = pendulum.from_timestamp(
                readInfo.get("readDetail").get("lastReadingDate")).to_datetime_string() if readInfo.get(
                "readDetail", {}).get("lastReadingDate",None) else None
            # 最晚阅读时间
            LatestDay = pendulum.from_timestamp(
                readInfo.get("readDetail").get("deepestNightReadTime")).to_datetime_string() if readInfo.get(
                "readDetail", {}).get("deepestNightReadTime",None) else None
            # 书籍阅读链接
            url = we_read_api.get_url(bookId)

            # 书籍信息整合
            BookInformation = {"bookId":bookId,"title":str(title),"classification":classification,"cover":cover,"name":name,
                               "isbn":isbn,"readSign":readSign,"briefIntroduction":briefIntroduction,"Progress":Progress,
                               "ReadDay":ReadDay,"ReadDayTime":ReadDayTime,"StartDay":StartDay,"LastDay":LastDay,
                               "LatestDay":LatestDay,"ReadUrl":url}

            # 章节信息
            chapter = we_read_api.get_chapter_info(bookId)
            # 获取bookid这本书的划线信息
            bookmark_list = we_read_api.get_bookmark_list(bookId)
            # 获取一本书的笔记信息
            reviews = we_read_api.get_review_list(bookId)

            # 提取章节及划线，笔记信息
            MyExtendList,MyExtendList1=MyExtend(chapter,bookmark_list,reviews,model)

            # 章节，划线，笔记信息整合
            assemble_BookMessage(MyExtendList,MyExtendList1,BookInformation)
        print(f"all：{book_message}")

        config.save("Data_End", "weread.json", book_message, "txt")
        print("微信读书数据解析完毕")

# 参数信息 1.章节信息；2.划线信息；3.笔记信息
def MyExtend(get_chapter_info, get_bookmark_list, get_review_list, model=None):

    # 尝试重新遍历，在每次遍历的时候，使用两个数字控制章节id a+b，a为整数位，b为小数位，如遇到了anchors锚点，
    # 则整数位+1，开始递增小数位，直到下次遇见anchors锚点，整数位+1，小数位重置为0.0,。如未遇到anchors锚点
    # 则一直递增整数位，if外层使用标志位来控制for循环是递增整数还是小数

    # 划线
    # 将对应章节的划线放在一个字典中，key为chapterUid，value为一个列表，列表中存放的是该章节的所有划线
    get_bookmark_list_my = {}
    for value in get_bookmark_list:

        chapterUid = value.get("chapterUid")
        createTime = value.get("createTime")
        createTime = pendulum.from_timestamp(createTime).to_datetime_string() if createTime else None
        markText = value.get("markText")

        if chapterUid not in get_bookmark_list_my:
            get_bookmark_list_my[chapterUid] = [{"chapterUid": chapterUid, "createTime": createTime, "markText": markText, "content": ""}]
        else:
            get_bookmark_list_my[chapterUid].append({"chapterUid": chapterUid, "createTime": createTime, "markText": markText, "content": ""})

    # 笔记
    # 将对应章节的划线放在一个字典中，key为chapterUid，value为一个列表，列表中存放的是该章节的所有笔记
    get_review_list_my = {}
    for value in get_review_list:
        chapterUid = value.get("chapterUid")
        abstract = value.get("abstract")
        content = value.get("content")
        createTime = value.get("createTime")
        createTime = pendulum.from_timestamp(createTime).to_datetime_string() if createTime else None

        if chapterUid not in get_review_list_my:
            get_review_list_my[chapterUid] = [
                {"chapterUid": chapterUid, "createTime": createTime, "markText": abstract, "content": content}]
        else:
            get_review_list_my[chapterUid].append(
                {"chapterUid": chapterUid, "createTime": createTime, "markText": abstract, "content": content})

    # 标志位及数字的整数小数部分
    sign = 0
    i = 0
    i_x = 0.0

    # 章节字典
    chapter_translations1 = {}
    get_mark_my = []
    for key, value in get_chapter_info.items():

        # 如果遇见锚点，存储锚点及对应的锚点数据，并进行下次循环
        if "anchors" in value:
            sign = 1
            i_x = 0.0
            i += 1

            chapter_translations1[round(i + i_x,3)] = value["title"]
            i_x = 0.1
            tile = value.get("anchors")[0].get("title")
            chapter_translations1[round(i + i_x,3)] = tile

            value["title"] = tile
            del value["anchors"]
            continue

        # 使用标志位来控制，现在的章节是否为大标题下的小标题
        if sign == 0:
            i += 1

        elif sign == 1:
            i_x += 0.1

        for temp in get_bookmark_list_my.get(key, []):
            temp["chapterUid"] = round(i + i_x,3)
            temp["chapterTitle"] = value.get("title","")
            get_mark_my.append(temp)

        for temp1 in get_review_list_my.get(key, []):
            temp1["chapterUid"] = round(i + i_x,3)
            temp1["chapterTitle"] = value.get("title","")
            get_mark_my.append(temp1)

        chapter_translations1[round(i + i_x,3)] = value["title"]

    return get_mark_my,chapter_translations1

def assemble_BookMessage(MyExtendList,MyExtendList1,BookInformation):

    BookInformation['markText'] = MyExtendList
    BookInformation['1000000'] = MyExtendList1
    title = BookInformation['title']

    global book_message
    book_message[title] = BookInformation

if __name__ == "__main__":
    main()
