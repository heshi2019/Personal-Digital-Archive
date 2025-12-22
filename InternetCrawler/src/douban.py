import time
from datetime import datetime
from InternetCrawler.src.Config.config_manager import config
from InternetCrawler.src.douban_api import DouBanApi
from bs4 import BeautifulSoup



# 30天对应的Unix毫秒值（固定值）
THIRTY_DAYS_MS = 2592000000

'''
        Args: type = all        全量获取数据
              type = increment  增量获取数据
                默认全量获取
'''
def get_douban_list(model=None):

    print("开始获取豆瓣电影")
    douban_api = DouBanApi()

    movie_status = {
        "mark": "想看",
        "doing": "在看",
        "done": "看过",
    }
    # 按照状态字典获取数据
    doubanList = []
    for key,value in movie_status.items():
        # 获取数据
        temp = douban_api.fetch_subjects( key)
        # 筛选数据
        doubanList.append(Episodes_Arrange(temp,key,model))

    config.save("Data_End", "douban.json", doubanList, "txt")



'''
        Args: type = all        全量获取数据
              type = increment  增量获取数据
                默认全量获取
'''
def Episodes_Arrange(EpisodeList,type,model=None):
    errList = []
    Episodes = []
    for item in EpisodeList:

        # 剧名
        title = item.get("subject", {}).get("title", "未知剧名")
        print(f"豆瓣电影，正在同步：{title}")
        #
        # if "未知" not in title:
        #     continue  # 跳过不符合条件的书籍

        # 筛选数据错误的剧名
        if (item.get("subject",{}).get("is_released",False) == False and
                item.get("subject",{}).get("has_linewatch",False) == False):
            errList.append(item.get("subject",{}).get("id",""))
            continue
        else :
            # 导演
            directors = item.get("subject", {}).get("directors", {})
            directorList = []
            for value in directors:
                directorList.append(value["name"])

            # 演员，一个列表
            actors = item.get("subject",{}).get("actors","")
            actorsList = []
            for value in actors:
                actorsList.append(value["name"])

            card_subtitle = item.get("subject",{}).get("card_subtitle","")

            # 分割字符串并处理各部分
            parts = [p.strip() for p in card_subtitle.split('/')]

            # 制片国家（第二个斜杠前）
            count = parts[1] if len(parts) > 1 else "1970"

            # 编剧（第三个斜杠后，索引3）
            Scriptwriter = parts[3].split() if len(parts) > 3 else []

            # 类型，一个列表
            genres = item.get("subject",{}).get("genres","")

            rating = item.get("subject",{}).get("rating","")
            # 评分人数
            countNum = rating.get("count","")
            # 分数
            countIne = rating.get("value","")

            # 首播时间
            pubdate = item.get("subject", {}).get("pubdate", "")
            # 豆瓣页面url
            url = item.get("subject", {}).get("url", "")

            # 哪里能看
            vendor_icons = item.get("subject", {}).get("vendor_icons", [])
            vendor_names = [
                url.split('/')[-1].split('.')[0]  # 分割两次：先取最后一段，再去掉后缀
                for url in vendor_icons
                if isinstance(url, str)
            ]
            # 封面图
            cover_url = item.get("subject", {}).get("cover_url", "")
            # 所在榜单
            honor_infos = item.get("subject", {}).get("honor_infos", "")
            id = item.get("id")

            movieOne = {"id":id,"title":title,"directors":directorList,"Scriptwriter":Scriptwriter,
                        "actors":actorsList,"count":count,"genres":genres,"countNum":countNum,
                        "countIne":countIne,"pubdate":pubdate,"url":url,"vendor_names":vendor_names,
                        "cover_url":cover_url,"honor_infos":honor_infos}

            # 我的评价
            comment = item.get("comment","")
            # 我的评分
            if item.get("rating") is not None:
                MyValue = item.get("rating", {}).get("value","")
            else:
                MyValue = "未评分"
            # 标记时间
            create_time = item.get("create_time", "")

            # 全量获取
            if model == "all" or model is None:
                pass
            # 增量获取，只获取最近30天内更新的书籍
            elif model == "increment":
                # 使用strptime函数，将字符串解析为datetime对象，timestamp()方法将datetime对象转换为时间戳（秒级）
                LastReadTime = int(datetime.strptime(create_time, '%Y-%m-%d %H:%M:%S').timestamp()* 1000)
                nowTime = int(time.time() * 1000)
                if LastReadTime >= nowTime - THIRTY_DAYS_MS:
                    pass
                    # 如果数据的最后阅读时间大于当前时间，则增量获取数据
                else:
                    continue

            myComment = {"comment":comment,"MyValue":MyValue,"create_time":create_time}

            temp = {"movieOne":movieOne,"myComment":myComment}
            Episodes.append(temp)

    # 有些数据通过下面的这个API获取不到具体数据，如无耻之徒，会返回未知剧名，这些数据存在errList中
    # 而网页端有个接口，可以通过剧名id来获取一个该剧的html返回数据，这里尝试解析这个网页来获取缺失的数据
    # 如 https://movie.douban.com/subject/10759851/ 这个接口就不返回任何东西，直接报404

    # douban_api = DouBanApi()
    # for value in errList:
    #     get_err_id = douban_api.getErr_id(value)
    #     info = get_show_info(get_err_id)
    #     print(f"这是重新获取的数据:{info}")
    return {str(type):Episodes}

def get_show_info(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    # 剧名
    h1_tag = soup.find('h1')
    title = h1_tag.text.strip().split(' ')[0] if h1_tag else None

    # 导演
    directors = [director.text for director in soup.find_all('a', rel='v:directedBy')]

    # 编剧
    writers = [writer.text for writer in soup.find_all('a', href=lambda href: href and 'personage' in href and'screenplayBy' in href)]

    # 主演
    stars = [star.text for star in soup.find_all('a', rel='v:starring')]

    # 类型
    genres = [genre.text for genre in soup.find_all('span', property='v:genre')]

    # 制片国家
    country_span = soup.find('span', string='制片国家/地区:')
    country = country_span.find_next_sibling('span').text if country_span else None

    # 首播时间
    premiere_span = soup.find('span', property='v:initialReleaseDate')
    premiere = premiere_span['content'] if premiere_span else None

    # 我的评价（假设页面中只有一处评分相关且符合格式的数据）
    my_rating = soup.find('input', id='n_rating')['value'] if soup.find('input', id='n_rating') else None

    # 我的评价时间（假设页面中只有一处收藏时间相关且符合格式的数据）
    my_rating_time = soup.find('span', class_='collection_date').text.strip() if soup.find('span', class_='collection_date') else None

    # 豆瓣评分
    douban_rating = soup.find('strong', class_='rating_num').text if soup.find('strong', class_='rating_num') else None

    # 豆瓣评价人数
    rating_people = soup.find('a', class_='rating_people')
    review_count = rating_people.find('span', property='v:votes').text if rating_people else None

    # 封面图连接
    cover_img = soup.find('img', rel='v:image')
    cover_url = cover_img['src'] if cover_img else None

    # 我的评论（假设页面中只有一处符合格式的数据）
    my_comment = soup.find('span', class_='pl').text.strip() if soup.find('span', class_='pl') else None

    result = {
        "剧名": title,
        "导演": directors,
        "编剧": writers,
        "主演": stars,
        "类型": genres,
        "制片国家": country,
        "首播时间": premiere,
        "我的评价": my_rating,
        "我的评价时间": my_rating_time,
        "豆瓣评分": douban_rating,
        "豆瓣评价人数": review_count,
        "封面图连接": cover_url,
        "我的评论": my_comment
    }
    return result



if __name__ == "__main__":
    # 获取数据
    get_douban_list()





