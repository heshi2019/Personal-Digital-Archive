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
            "content": content,
            "files": files
        }
        memos.append(memo)
    return json.dumps(memos, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    main()
