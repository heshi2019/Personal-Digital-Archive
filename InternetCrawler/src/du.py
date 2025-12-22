import time
from datetime import datetime
from InternetCrawler.src.Config.config_manager import config
from InternetCrawler.src.du_api import DUApi


# 30天对应的Unix毫秒值（固定值）
THIRTY_DAYS_MS = 2592000000
'''

        Args: type = all        全量获取数据
              type = increment  增量获取数据
'''
def get_du_book_list(model=None):

    du_api = DUApi()
    print("开始获取网易蜗牛读书信息")

    #网易蜗牛读书API，获取基本数据
    books = du_api.get_book_list().get("bookWrappers")

    bookIdStr = ""
    BookList = {}

    if books!= None:
        for index, book in enumerate(books):


            # 全量获取
            if model == "all" or model is None:
                pass

            # 增量获取，只获取最近30天内更新的书籍
            elif model == "increment":
                LastReadTime = book.get("book").get("updateTime")
                nowTime = int(time.time() * 1000)
                if LastReadTime >= nowTime - THIRTY_DAYS_MS:
                    pass
                    # 如果数据的最后阅读时间大于当前时间，则增量获取数据
                else:
                    continue


            # 书名
            title = book.get("book").get("title")
            # if "刺杀骑士团长" not in title:
            #     continue  # 跳过不符合条件的书籍

            # 书籍id
            bookId = book.get("book").get("bookId")
            bookIdStr = bookIdStr + str(bookId) + ","

            print(f"正在同步《{title}》,一共{len(books)}本，当前是第{index + 1}本。")

            # 简介
            description = book.get("book").get("description")
            # 封面连接
            imageUrl = book.get("book").get("imageUrl")
            # 阅读状态，-1为读完
            ReadStatus = book.get("book").get("status")
            if ReadStatus == -1:
                ReadStatus = "已读完"
            else:
                ReadStatus = "在读"
            # 字数
            wordCount = book.get("book").get("wordCount")
            # isbn
            isbn = book.get("book").get("isbn")
            # 出版时间 unix时间戳 毫秒
            publishTime = book.get("book").get("publishTime")
            publishTime = datetime.fromtimestamp(publishTime / 1000).strftime(
                '%Y-%m-%d %H:%M:%S') if publishTime else None

            # 出版社
            publisher = book.get("book").get("publisher")
            # 最后阅读时间 unix时间戳 毫秒
            LastReadTime = book.get("book").get("updateTime")
            LastReadTime = datetime.fromtimestamp(LastReadTime / 1000).strftime('%Y-%m-%d %H:%M:%S') if LastReadTime else None
            # 分类
            category = book.get("category").get("name")
            # 作者,列表
            authors = []
            for author in book.get("authors"):
                authors.append(author.get("name"))

            # 书籍信息整合
            BookInformation = {"bookId":bookId,"title":str(title),"description":description,"imageUrl":imageUrl,
                               "ReadStatus":ReadStatus,"wordCount":wordCount,"isbn":isbn,"publishTime":publishTime,
                               "publisher":publisher,"LastReadTime":LastReadTime,"category":category,"authors":authors
                               }
            BookList[title] = BookInformation

    # 获取对应数据id章节
    Chapter = du_api.get_Chapter(bookIdStr)
    # 获取划线信息
    Annotations = du_api.get_Annotations()
    # 整合书籍，划线，章节信息
    temp = MyExtend(BookList,Annotations, Chapter,model)

    # 保存数据
    config.save("Data_End", "du.json", temp, "txt")


def MyExtend(BookList,Annotations, Chapter,model=None):

    # 划线信息
    BookAn = {}

    for BookInformation in Annotations.get("updated"):

        # 增量获取，只获取最近30天内更新的书籍
        if model == "increment":
            LastReadTime = BookInformation.get("bookNote").get("uploadTime")
            nowTime = int(time.time() * 1000)
            if LastReadTime >= nowTime - THIRTY_DAYS_MS:
                pass
                # 如果数据的最后阅读时间大于当前时间，则增量获取数据
            else:
                continue



        # 这两个数字需要转换为字符串，否则再次提取时会报错
        bookId = str(BookInformation.get("bookNote").get("bookId"))
        # 章节id
        articleId = str(BookInformation.get("bookNote").get("articleId"))
        
        # 如果temp不初始化，会报错
        if BookAn.get(bookId,{}).get(articleId,{}) != {}:
            temp = BookAn[bookId].get(articleId)
        else:
            temp = []
        # 划线
        markText = BookInformation.get("bookNote").get("markText")
        # 笔记
        remark = BookInformation.get("bookNote").get("remark")
        # 创建时间
        createTime = BookInformation.get("bookNote").get("createTime")
        createTime = datetime.fromtimestamp(createTime / 1000).strftime('%Y-%m-%d %H:%M:%S') if createTime else None

        # 更新时间
        uploadTime = BookInformation.get("bookNote").get("uploadTime")
        uploadTime = datetime.fromtimestamp(uploadTime / 1000).strftime('%Y-%m-%d %H:%M:%S') if uploadTime else None

        #拼接
        temp.append({"markText":markText,"remark":remark,"createTime":createTime,"uploadTime":uploadTime})
        
        # 还是BookAn需要初始化，否则报错。并且分不同的赋值方式，否则会被覆盖
        if BookAn.get(bookId, {}) == {}:
            BookAn[bookId]= {articleId: temp}
        else:
            BookAn[bookId][articleId] = temp

    # 我讨厌数组
    # 章节信息
    BookList_OneBooke = {}

    for mes in Chapter.get("catalogs"):

        num = 1.0
        # 全章节信息
        BookCh_temp = {}

        # 章节对应划线
        chat_temp_one = []
        for mes_1 in mes.get("catalog"):

            if mes_1.get("children") == []:
                BookCh_temp[num] = mes_1.get("title")

                temp = BookAn.get(str(mes.get("bookId")),{}).get(str(mes_1.get("articleId")),{})
                if (temp!= []):
                    for book in temp:
                        book["title"] = mes_1.get("title")
                        book["articleId"] = num
                        chat_temp_one.append(book)

                num = num + 1

            elif mes_1.get("children") != []:
                # 这里的小节，0.01则每章节最大为99小节，且最后写入时需要指定保留两位小数，否则会导致会导致数字自动扩展为10位左右
                numF = 0.01
                BookCh_temp[num] = mes_1.get("title")

                temp = BookAn.get(str(mes.get("bookId")),{}).get(str(mes_1.get("articleId")),{})
                if (temp!= []):
                    for book in temp:
                        book["title"] = mes_1.get("title")
                        book["articleId"] = num
                        chat_temp_one.append(book)

                for mes_2 in mes_1.get("children"):
                    BookCh_temp[round(num + numF, 2)] = mes_2.get("title")

                    temp = BookAn.get(str(mes.get("bookId")), {}).get(str(mes_2.get("articleId")), {})
                    if (temp != []):
                        for book in temp:
                            book["title"] = mes_2.get("title")
                            book["articleId"] = num
                            chat_temp_one.append(book)

                    numF = numF + 0.01
                num = num + 1

        # 章节和笔记
        if str(mes.get("bookId")) not in BookList_OneBooke.keys():
            BookList_OneBooke[str(mes.get("bookId"))] = {"market":chat_temp_one,"1000000":BookCh_temp}
        else:
            BookList_OneBooke[str(mes.get("bookId"))].update({
                "market": chat_temp_one,"1000000": BookCh_temp
            })

        # 数据整合
    for key,value in BookList.items():
        bookId = value.get("bookId")
        market = BookList_OneBooke.get(str(bookId),{}).get("market",{})
        xuhao = BookList_OneBooke.get(str(bookId),{}).get("1000000",{})

        value["market"] = market
        value["1000000"] = xuhao
        BookList[key] = value

    return BookList

if __name__ == "__main__":
    # 增量获取数据
    get_du_book_list('increment')
