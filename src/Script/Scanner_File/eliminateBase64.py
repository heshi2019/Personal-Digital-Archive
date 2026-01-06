import json
import os
import re
import base64
import hashlib
from pathlib import Path

from src.config.configClass import app_config


def ensure_dir(directory):
    """确保目录存在"""
    if not os.path.exists(directory):
        os.makedirs(directory)


def process_single_file(file_path):
    """处理单个 Markdown 文件"""
    if not file_path.endswith('.md'):
        return

    print(f"正在处理: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 正则表达式匹配 ![图片](data:image/xxx;base64,编码内容)
    # 分组说明：g1: alt文字, g2: 图片后缀, g3: base64编码数据
    pattern = r'!\[(.*?)\]\(data:image\/(.*?);base64,([a-zA-Z0-9+/= \n]+)\)'

    def replace_func(match):
        alt_text = match.group(1)
        ext = match.group(2)
        b64_data = match.group(3).replace('\n', '').replace(' ', '')  # 清理可能的换行

        try:
            # 1. 解码
            img_binary = base64.b64decode(b64_data)

            # 2. 计算哈希值作为文件名
            file_hash = hashlib.md5(img_binary).hexdigest()
            file_name = f"{file_hash}.{ext}"

            # 3. 保存图片
            save_path = os.path.join(IMAGE_SAVE_DIR, file_name)
            if not os.path.exists(save_path):
                with open(save_path, 'wb') as img_f:
                    img_f.write(img_binary)
                print(f"  - 已提取图片: {file_name}")

            # 4. 返回替换后的字符串
            # 这里默认使用相对路径，如果你的MD和图片位置特殊，可以调整这里
            # 如果图片和MD在不同目录，可能需要处理相对路径关系，这里暂定直接写文件名
            return f"![images]({file_name})"

        except Exception as e:
            print(f"  - 图片提取失败: {e}")
            return match.group(0)  # 失败则保持原样

    # 执行替换
    new_content = re.sub(pattern, replace_func, content)

    # 保存新的 Markdown 文件
    ensure_dir(OUTPUT_MD_DIR)
    target_md_path = os.path.join(OUTPUT_MD_DIR, os.path.basename(file_path))

    with open(target_md_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print(f"完成！新文件已保存至: {target_md_path}")


def main(input_path):
    # 确保保存图片的目录存在
    # 为了方便 Markdown 预览，通常会在 MD 目录下建一个 images 文件夹
    # 如果你希望图片和新 MD 在一起，可以把 IMAGE_SAVE_DIR 设为 OUTPUT_MD_DIR + "/images"
    ensure_dir(IMAGE_SAVE_DIR)
    ensure_dir(OUTPUT_MD_DIR)

    if os.path.isfile(input_path):
        # 处理单个文件
        process_single_file(input_path)
    elif os.path.isdir(input_path):
        # 遍历目录
        for root, dirs, files in os.walk(input_path):
            for file in files:
                if file.endswith('.md'):
                    full_path = os.path.join(root, file)
                    process_single_file(full_path)
    else:
        print("错误: 输入的路径不存在")




def extract_md_images(md_file_path: str, image_target_dir: str) -> dict:
    """
    读取Markdown文件，提取所有图片名，并映射到指定目录的完整路径

    Args:
        md_file_path: Markdown文件的完整路径（如 "test.md" 或 "/home/user/note.md"）
        image_target_dir: 图片存放的目标目录（如 "/home/user/images"）

    Returns:
        dict: {图片名: 完整图片路径}，例如 {"d88571b8ce8f1d44f022cdc7ba5efe46.png": "/home/user/images/d88571b8ce8f1d44f022cdc7ba5efe46.png"}

    Raises:
        FileNotFoundError: 如果传入的Markdown文件不存在
        PermissionError: 如果没有读取Markdown文件的权限
    """
    # 正则表达式匹配 ![xxx](图片名) 格式的内容，提取括号内的图片名
    # 匹配规则：![任意字符](图片名)，捕获括号内的图片名部分
    pattern = r"!\[.*?\]\((.*?)\)"

    # 初始化返回的字典
    image_mapping = {}

    try:
        # 读取Markdown文件内容（使用utf-8编码，兼容中文）
        with open(md_file_path, 'r', encoding='utf-8') as f:
            md_content = f.read()

        # 查找所有匹配的图片名
        image_names = re.findall(pattern, md_content)

        # 遍历图片名，拼接完整路径并写入字典
        for img_name in image_names:
            # 过滤空值（防止匹配到空的图片名）
            if img_name.strip():
                # 拼接目标目录和图片名，生成完整路径
                full_img_path = os.path.join(image_target_dir, img_name).replace(os.sep, '/')

                image_mapping[img_name] = full_img_path

    except FileNotFoundError:
        raise FileNotFoundError(f"错误：未找到Markdown文件 {md_file_path}")
    except PermissionError:
        raise PermissionError(f"错误：没有权限读取文件 {md_file_path}")
    except Exception as e:
        raise Exception(f"读取Markdown文件时发生未知错误：{str(e)}")

    return image_mapping


# ================= 配置部分 =================
# 1. 恢复出来的图片保存到哪个目录
IMAGE_SAVE_DIR = app_config.image_save_path
# 2. 替换后的 Markdown 文件保存到哪个目录
OUTPUT_MD_DIR = r''

# ===========================================


if __name__ == '__main__':
    # 在这里输入你的文件路径或者目录路径

    test_path = r''

    # 运行
    main(test_path)

    # 测试下提取md图片列表
    # images = extract_md_images(test_path, IMAGE_SAVE_DIR)
    # print(json.dumps(images, ensure_ascii=False, indent=2))
