from pathlib import Path
import json
from InternetCrawler.src.Config.config_manager import config
from bs4 import BeautifulSoup

def main():

    print("开始解析flomo数据")

    # 获取项目根目录路径
    root_path = Path(__file__).parent.parent.parent.parent  # InternetCrawler目录

    # 获取配置文件中的相对路径
    relative_path = config.get_key("FLOMO", "file_path")

    # 构建完整的绝对路径
    file_path = root_path / relative_path

    print(f"尝试读取文件路径: {file_path}")

    data = html_file_to_json(file_path)

    # 保存数据
    config.save(f"{Path(__file__).parent.parent.parent}/Data_End", "flomo.json", data, "json")

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
