import os
from datetime import datetime
try:
    import piexif
except Exception:
    piexif = None

# 从图片文件中提取 EXIF 元数据中的拍摄时间
def get_exif_datetime(path):

    # piexif是一个第三方的库，这个函数尝试从文件的EXIF元数据中提取拍摄时间
    if not piexif:
        return None
    try:
        exif_data = piexif.load(path)
        dt = exif_data['Exif'].get(piexif.ExifIFD.DateTimeOriginal)
        if dt:
            return dt.decode() if isinstance(dt, bytes) else dt
        return None
    except Exception:
        return None

# 获取文件的基本系统元数据
def parse_file_metadata(path: str):

    # os.stat()函数获取文件的系统状态信息，返回一个包含各种文件属性的对象
    stat = os.stat(path)
    # 注意：mtime表示修改时间，ctime则取决于具体平台

    # datetime.fromtimestamp将时间戳转换为python的datetime对象
    # stat获取的属性的原始名称是st_XXXX
    modified = datetime.fromtimestamp(stat.st_mtime)
    created = datetime.fromtimestamp(stat.st_ctime)

    return {
        # isoformat函数，将datetime对象转换为ISO格式字符串，例如：2023-08-24T12:34:56.789000
        "created_at": created.isoformat(),
        "modified_at": modified.isoformat(),
        "size": stat.st_size,
    }
