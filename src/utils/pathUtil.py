import datetime
import glob
import os
from pathlib import Path
def path_match(paths: list[str]) -> list[str]:
    # 新增：处理通配符路径，将通配符扩展为实际存在的目录列表
    expanded_paths = []
    for path in paths:
        # 使用glob扩展通配符路径
        matched_paths = glob.glob(path)
        # 过滤出实际存在的目录
        for matched_path in matched_paths:
            if os.path.isdir(matched_path):

                # 使用 Path 标准化路径分隔符
                normalized_path = str(Path(matched_path))

                expanded_paths.append(normalized_path)
            else:
                print(
                    f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 警告: {matched_path} 不是有效的目录，已跳过")
    return expanded_paths