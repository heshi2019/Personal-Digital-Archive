import os
import json
from datetime import datetime

from src.config.configClass import app_config
from src.utils.utils import app_Utils

#该脚本不能直接使用，需要准备对应的笔记信息和批注信息后，使用该脚本整合，最后会输出一个json文件和一个excel
# 将该脚本放在桌面的某个文件夹中，其他需要的数据如下所示存放
# 文件夹A
#     本脚本.py
#     掌阅-批注书籍.txt
#     掌阅笔记
#         书名-笔记.txt
#         书名-笔记.txt
#         ...

# 文件夹路径
folder_path = '掌阅笔记'

# 清理批注中的非法字符
def clean_illegal_characters(text):
    if text is None:
        return ""
    # 去除非法字符
    return ''.join(c for c in text if ord(c) >= 32 or ord(c) == 9)

# 读取批注文件
def read_annotation_files(folder_path):
    annotations = {}

    # 遍历文件夹中的所有批注文件
    for filename in os.listdir(folder_path):
        if filename.endswith("-笔记.txt"):
            # 通过从后往前查找第一个 - 来提取书名
            book_name = filename.rsplit('-', 1)[0]  # 只保留第一个 - 前的部分
            file_path = os.path.join(folder_path, filename)

            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()

                # 初始化一个批注列表
                book_annotations = []
                current_annotation = {}

                # 遍历文件中的每一行
                for line in lines:
                    line = line.strip()
                    if line:  # 跳过空行
                        if line.startswith("201"):  # 日期行
                            # 如果有已有的批注，保存它
                            if current_annotation:
                                book_annotations.append(current_annotation)
                            # 初始化新的批注
                            current_annotation = {"date": line}
                        elif line.startswith("原文："):  # 原文行
                            current_annotation["original_text"] = clean_illegal_characters(
                                line.replace("原文：", "").strip())
                        elif line.startswith("想法："):  # 想法行
                            current_annotation["thoughts"] = clean_illegal_characters(line.replace("想法：", "").strip())

                # 在最后处理完一条批注后，要保存它
                if current_annotation:
                    book_annotations.append(current_annotation)

                # 将该书的批注添加到annotations字典
                if book_name not in annotations:
                    annotations[book_name] = []
                annotations[book_name].extend(book_annotations)  # 添加多个批注

    return annotations


# 读取书籍数据（假设书籍数据从文件夹或之前的数据中读取）
def read_books_data():

    with open('../../data/Manual_Import_Data/zhangyue/掌阅-批注书籍.txt', 'r', encoding='utf-8') as file:
        json_data = file.read()
    data = json.loads(json_data)
    return data['body']['data']


# 合并相同书名的数据
def merge_books_data(books_data, annotations):
    merged_books = {}

    for book in books_data:
        book_name = book["name"]

        # 如果该书已经处理过，进行合并
        if book_name in merged_books:
            existing_book = merged_books[book_name]

            # 合并笔记数和阅读进展
            existing_book["notenum"] += book["notenum"]
            existing_book["readpercent"] += float(book["readpercent"]) * 100  # 累加阅读进展

            # 更新时间取最早的时间
            current_update_time = datetime.strptime(book['updatetime'].replace('更新时间：', ''), '%Y年%m月%d日')
            if existing_book['updatetime'] > current_update_time:
                existing_book['updatetime'] = current_update_time
        else:
            # 第一次遇到该书，初始化书籍信息
            merged_books[book_name] = {
                "name": book_name,
                "author": book["author"] if book["author"] else "未知",
                "deviceName": book["deviceName"],
                "notenum": book["notenum"],
                "readpercent": float(book["readpercent"]) * 100,  # 转换为百分比
                "updatetime": datetime.strptime(book['updatetime'].replace('更新时间：', ''), '%Y年%m月%d日'),
                "pic": book["pic"] if book["pic"] else "无图片",
                "annotations": []  # 初始化批注列表
            }

        # 将当前书的批注加入合并后的数据
        if book_name in annotations:
            merged_books[book_name]["annotations"].extend(annotations[book_name])

    return list(merged_books.values())


# 输出JSON格式数据
def save_json(books_data):
    final_books_data = []

    # 只保留所需字段
    for book in books_data:
        # 格式化 readpercent
        readpercent = f"{float(book['readpercent']-1) * 100:.2f}"

        # 格式化 updatetime
        updatetime = book["updatetime"].strftime("%Y-%m-%d")

        # 创建新的数据结构，移除不需要的字段
        book_info = {
            "title": book["name"],
            "authors": book["author"] if book["author"] else "未知",
            "deviceName": book["deviceName"],
            "notenum": book["notenum"],
            "readpercent": readpercent,
            "updateTime": updatetime,
            "imageUrl": book["pic"],
            "annotations": book.get("annotations", [])  # 保证批注是列表
        }

        final_books_data.append(book_info)

    app_Utils.save(app_config.Data_End, "zhangyue.json", final_books_data, "txt")
    print("掌阅数据解析完毕")

# 主程序
def main():
    print(f"开始解析掌阅数据")

    # 读取批注数据
    annotations = read_annotation_files('../../data/Manual_Import_Data/zhangyue/掌阅笔记')

    # 读取书籍数据
    books_data = read_books_data()

    # 合并书籍数据和批注数据
    books_data_with_annotations = merge_books_data(books_data, annotations)

    # 保存为JSON格式
    save_json(books_data_with_annotations)


if __name__ == "__main__":
    main()
