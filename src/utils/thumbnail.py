from PIL import Image
import os

# 生成指定尺寸的缩略图
def generate_thumbnail(input_path: str, output_path: str, size=(400, 400)):

    # 输出目录
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    try:
        # 打开，生成，保存，生成缩略图的函数是Image模块的，输入了要生成的大小
        img = Image.open(input_path)
        img.thumbnail(size)
        img.save(output_path, format='JPEG', quality=85)
        return output_path
    except Exception:
        return None
