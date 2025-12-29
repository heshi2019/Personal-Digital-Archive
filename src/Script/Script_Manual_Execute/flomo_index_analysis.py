from pathlib import Path
import json
from bs4 import BeautifulSoup

from src.config.configClass import app_config
from src.utils.utils import app_Utils


def main():

    print("开始手动脚本解析 flomo 数据")

    print(f"尝试读取文件路径: {app_config.Flomo.file_path}")

    data = html_file_to_json(Path(app_config.Flomo.file_path) / app_config.Flomo.file_name)

    # 保存数据
    app_Utils.save(app_config.Data_End, "flomo.json", data, "json")

    print("flomo 手动解析完成")

def html_file_to_json(file_path):  # 参数改为文件路径
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()
        return html_to_json(html_content)  # 复用原有解析逻辑
    except FileNotFoundError:
        print(f"错误：文件 {file_path} 未找到")
        return None
    except Exception as e:
        print(f"错误：读取文件时发生异常 - {str(e)}")
        return None

def html_to_json(html_content):  # 保留原有解析逻辑，专注于 HTML 处理
    soup = BeautifulSoup(html_content, 'html.parser')
    memos = []
    for memo_div in soup.find_all('div', class_='memo'):
        time = memo_div.find('div', class_='time').text if memo_div.find('div', class_='time') else ''
        content = memo_div.find('div', class_='content').text if memo_div.find('div', class_='content') else ''
        files = [img['src'] for img in memo_div.find_all('img')] if memo_div.find_all('img') else []
        memo = {
            "time": time,
            "content": process_p_tag_string(content),
            "files": files
        }
        memos.append(memo)
    return json.dumps(memos, ensure_ascii=False, indent=4)


def process_p_tag_string(html_str):
    """
    处理包含<p>标签的Python字符串，保留空<p></p>对应的换行

    参数：
        html_str: 原始的包含<p>标签的字符串
    返回：
        处理后的纯文本字符串，空<p></p>转换为换行符
    """
    # 按<p>拆分字符串，过滤掉开头/结尾的空字符串
    parts = [part.strip() for part in html_str.split('<p>') if part.strip()]

    result = []
    for part in parts:
        # 去除</p>标签，清理首尾空白
        clean_part = part.replace('</p>', '').strip()
        # 如果是空的<p></p>（即clean_part为空），添加换行符
        if not clean_part:
            result.append('\n')
        # 非空段落，添加原文内容
        else:
            result.append(clean_part)

    # 拼接所有片段，确保换行符生效
    final_text = '\n'.join(result)
    # 处理可能的连续换行（可选，根据需求调整）
    final_text = '\n'.join([line for line in final_text.split('\n') if line.strip()])

    return final_text


if __name__ == "__main__":
    main()
