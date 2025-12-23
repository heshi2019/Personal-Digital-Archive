import os
# 新增导入
from src.DB.ImportSQLite.import_sqlite_scannerFile import FilesRepository
from src.DB.SQLite_util import SQLite_util
from src.config.configClass import app_config

from src.utils.file_hash import compute_hash
from src.utils.metadata import parse_file_metadata
from src.utils import pathUtil
from src.utils.thumbnail import generate_thumbnail
import uvicorn
import argparse
import datetime

# 预定义的文件类型
IMAGE_EXT = {".jpg", ".jpeg", ".png", ".gif", ".heic"}
VIDEO_EXT = {".mp4", ".mov", ".mkv"}
NOTE_EXT = {".md", ".txt"}
MUSUC_EXT = {".mp3"}

class Scanner:

    def __init__(self):

        # yaml是python内置的库，用于解析yaml文件，将yaml文件解析为python字典。
        # self.config = yaml.safe_load(open(config_path, encoding='utf-8'))

        self.config = app_config

        # 更新扫描路径为扩展后的实际目录列表
        self.config.scan_paths = pathUtil.path_match(self.config.scan_paths)
        self.config.skip_paths = pathUtil.path_match(self.config.skip_paths)

        # 初始化数据库，并创建对象
        self.db = SQLite_util(self.config.SQLitePath)
        self.thumbnail_cfg = self.config.thumbnail

    def close(self):
        """关闭数据库连接"""
        if hasattr(self, 'db'):
            self.db.close()

    def scan(self, full=False):
        # 换新的数据库管理方式
        filesTable = FilesRepository(self.db)
        filesTable.create_table()

        """扫描已配置的路径。如果full=True，则重新计算所有文件的哈希值/重新生成缩略图。"""
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始扫描...")

        # 统计所有需要扫描的文件总数/已处理文件数/已跳过文件数/错误文件数
        total_files = 0
        processed_files = 0
        skipped_files = 0
        error_files = 0
        
        # 首先统计文件总数
        for base_path in self.config.scan_paths:
            # root：当前正在遍历的目录路径
            # dirs：当前目录下的子目录列表
            # files：当前目录下的文件列表（不包含子目录中的文件）
            # os.walk用于递归遍历目录下的所有文件和子目录，dirs变量就是下一步递归的方向
            for root, dirs, files in os.walk(base_path):
                # 检查当前目录是否在跳过路径中
                if any(skip_path in root for skip_path in self.config.skip_paths):
                    # 清空 dirs 列表，阻止 os.walk() 继续遍历子目录
                    dirs.clear()
                    skipped_files += len(files)
                    continue


                total_files += len(files)
        
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 文件总数为： {total_files}")
        
        current_file = 0
        for base_path in self.config.scan_paths:
            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 扫描路径: {base_path}")
            
            for root, dirs, files in os.walk(base_path):
                # 检查当前目录是否在跳过路径中
                if any(skip_path in root for skip_path in self.config.skip_paths):
                    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 跳过路径: {root}")
                    skipped_files += len(files)
                    # 清空 dirs 列表，阻止 os.walk() 继续遍历子目录
                    dirs.clear()
                    continue

                for f in files:
                    current_file += 1
                    path = os.path.join(root, f)
                    # 用文件名（f）切割，获取文件扩展名
                    ext = os.path.splitext(f)[1].lower()
                    
                    # 打印进度
                    if current_file % 10 == 0 or current_file == total_files:
                        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 进度: {current_file}/{total_files} ({int(current_file/total_files*100)}%)")

                    # classify
                    if ext in IMAGE_EXT:
                        ftype = "image"
                    elif ext in VIDEO_EXT:
                        ftype = "video"
                    elif ext in NOTE_EXT:
                        ftype = "note"
                    elif ext in MUSUC_EXT:
                        ftype = "music"

                    else:
                        skipped_files += 1
                        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 跳过文件（不支持的文件类型）: {path}")
                        continue

                    try:
                        # 计算文件的哈希值
                        file_id = compute_hash(path)
                        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 哈希值 {ftype}: {path} (ID: {file_id[:16]}...)")

                        # 文件信息
                        meta = parse_file_metadata(path)

                        # 缩略图路径（目前仅适用于图片）
                        thumb_path = None
                        if ftype == "image" and self.thumbnail_cfg.enabled:
                            thumb_path = f"{self.thumbnail_cfg.path}/{file_id}.jpg"

                            # 如果缩略图不存在（not os.path.exists(thumb_path)） 或者 完全重新扫描（full=True），则生成缩略图
                            if not os.path.exists(thumb_path) or full:
                                generate_thumbnail(path, thumb_path, tuple(self.thumbnail_cfg.size))
                                print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 生成的缩略图: {thumb_path}")
                            else:
                                print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 缩略图已存在: {thumb_path}")

                        # 插入或更新数据库
                        filesTable.upsert({
                            "id": file_id,
                            "path": path,
                            "type": ftype,
                            "hash": file_id,
                            "created_at": meta["created_at"],
                            "modified_at": meta["modified_at"],
                            "size": meta["size"],
                            "thumbnail_path": thumb_path,
                        })

                        processed_files += 1
                        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 保存到数据库: {file_id[:16]}...")

                    except Exception as e:
                        error_files += 1
                        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 文件hash或缩略图生成失败:  {path}: {str(e)}")

        # 关闭数据库连接
        self.close()

        # Print final statistics
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 扫描完成！")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 总结：")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - 文件总数: {total_files}")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - 已处理文件数: {processed_files}")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - 跳过文件（不支持的文件类型）数: {skipped_files}")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - 错误文件数: {error_files}")


if __name__ == "__main__":

    # argparse是一个处理命令行参数的库，用于解析命令行参数。
    # ArgumentParser是一个类，用于创建一个解析器对象。
    # 下面定义了两个参数：action='store_true' 表示如果参数出现在命令行中，就将对应的属性设置为 True。
    parser = argparse.ArgumentParser()
    parser.add_argument('--scan-only', action='store_true', help='只运行扫描器，不启动 API 服务')
    parser.add_argument('--full', action='store_true', help='强制进行完整扫描（重新计算哈希值/缩略图）')

    # 解析命令行参数
    args = parser.parse_args()

    # full是全量扫描，incremental是增量扫描，
    # scanner.scan函数默认的full变量为false，并且其实也没有定义 incremental 这个值是增量扫描
    # 其实只要full变量不是true，那就是增量扫描，因此默认就是增量扫描
    scan_mode = "full" if args.full else "incremental"
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 扫描模式: {scan_mode}")

    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 正在初始化扫描仪...")

    # 初始化扫描仪
    scanner = Scanner()
    scanner.scan(full=args.full)

