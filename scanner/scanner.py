import os
import yaml
import uuid
import datetime
from db.operations import DB
from scanner.file_hash import compute_hash
from scanner.metadata import parse_file_metadata
from utils.thumbnail import generate_thumbnail
from scanner.scanner import Scanner
import uvicorn
import argparse
import datetime

# 预定义的文件类型
IMAGE_EXT = {".jpg", ".jpeg", ".png", ".gif", ".heic"}
VIDEO_EXT = {".mp4", ".mov", ".mkv"}
NOTE_EXT = {".md", ".txt"}

class Scanner:

    def __init__(self, config_path="config/config.yaml"):

        # yaml是python内置的库，用于解析yaml文件，将yaml文件解析为python字典。
        self.config = yaml.safe_load(open(config_path, encoding='utf-8'))

        # 初始化数据库，并创建对象
        self.db = DB(self.config["database_path"])
        self.thumbnail_cfg = self.config.get("thumbnails", {"enabled": True, "size": [400,400]})

    def scan(self, full=False):
        """扫描已配置的路径。如果full=True，则重新计算所有文件的哈希值/重新生成缩略图。"""
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始扫描...")

        # 统计所有需要扫描的文件总数/已处理文件数/已跳过文件数/错误文件数
        total_files = 0
        processed_files = 0
        skipped_files = 0
        error_files = 0
        
        # 首先统计文件总数
        for base_path in self.config["scan_paths"]:
            # root：当前正在遍历的目录路径
            # dirs：当前目录下的子目录列表
            # files：当前目录下的文件列表（不包含子目录中的文件）
            # os.walk用于递归遍历目录下的所有文件和子目录，dirs变量就是下一步递归的方向
            for root, dirs, files in os.walk(base_path):
                total_files += len(files)
        
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 文件总数为： {total_files}")
        
        current_file = 0
        for base_path in self.config["scan_paths"]:
            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 扫描路径: {base_path}")
            
            for root, dirs, files in os.walk(base_path):
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
                        if ftype == "image" and self.thumbnail_cfg.get("enabled", True):
                            thumb_path = f"data/thumbnails/{file_id}.jpg"

                            # 如果缩略图不存在（not os.path.exists(thumb_path)） 或者 完全重新扫描（full=True），则生成缩略图
                            if not os.path.exists(thumb_path) or full:
                                generate_thumbnail(path, thumb_path, tuple(self.thumbnail_cfg.get("size", [400,400])))
                                print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 生成的缩略图: {thumb_path}")
                            else:
                                print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 缩略图已存在: {thumb_path}")

                        # 写入数据库
                        self.db.insert_or_update_file({
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
                        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 错误文件:  {path}: {str(e)}")
        
        # Print final statistics
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 扫描完成！")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 总结：")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - 文件总数: {total_files}")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - 已处理文件数: {processed_files}")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - 跳过文件（不支持的文件类型）数: {skipped_files}")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - 错误文件数: {error_files}")


if __name__ == "__main__":
    # Print program header
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ==========================================")
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Personal Digital Archive - Scanner Service")
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ==========================================")
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--scan-only', action='store_true', help='Run a scan and exit')
    parser.add_argument('--full', action='store_true', help='Force full scan (recompute hashes/thumbnails)')
    args = parser.parse_args()

    # Show scan mode
    scan_mode = "full" if args.full else "incremental"
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scan mode: {scan_mode}")
    
    # Initialize scanner
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Initializing scanner...")
    scanner = Scanner()
    
    # Run scan
    scanner.scan(full=args.full)

    if args.scan_only:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scan-only mode: Exiting program")
    else:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting API server...")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API address: http://127.0.0.1:5000")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API docs: http://127.0.0.1:5000/docs")
        uvicorn.run("api.server:app", host="127.0.0.1", port=5000)