import yaml
from pathlib import Path
from dataclasses import dataclass
from typing import List

@dataclass
class ThumbnailConfig:
    size: list
    enabled: bool

@dataclass
class DatabaseConfig:
    path: str

@dataclass
class AppConfig:
    scan_paths: List[str]
    skip_paths: List[str]
    thumbnail: ThumbnailConfig
    database: DatabaseConfig

def load_config(path=None) -> AppConfig:
    # 如果没有提供路径，则使用相对于当前文件的配置文件路径
    if path is None:
        # 获取config.py文件的绝对路径
        current_file = Path(__file__).resolve()
        # 构建config.yaml文件的绝对路径（与config.py在同一目录）
        path = current_file.parent / "config.yaml"
    
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # 将数据库相对路径转换为绝对路径
    current_file = Path(__file__).resolve()
    db_path = Path(data["database"]["path"])
    if not db_path.is_absolute():
        # 如果是相对路径，相对于当前配置文件所在目录
        data["database"]["path"] = str(current_file.parent.parent / db_path)


    return AppConfig(
        scan_paths=data["scan_paths"],
        skip_paths=data["skip_paths"],
        thumbnail=ThumbnailConfig(**data["thumbnails"]),
        database=DatabaseConfig(**data["database"]),
    )