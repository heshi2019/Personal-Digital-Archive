import json
import os
import yaml
from pathlib import Path


class ConfigManager:
    _instance = None

    # 构建实例，区别于init是初始化实例
    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance

    # 实例读取数据
    def _load_config(self):
        # 自动定位配置文件（支持当前目录和上级目录）
        base_path = Path(__file__).parent.parent.parent
        config_path = base_path / "Config.yaml"

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
        except (FileNotFoundError, yaml.YAMLError) as e:
            raise RuntimeError(f"加载配置文件失败: {str(e)}")

    # 获取配置文件数据
    def get_key(self,Bits,key):
        return self._config[Bits].get(key, None)

    # 工具函数，用来保存数据
    def save(self,path,fileName,data,status):
        os.makedirs(path, exist_ok=True)
        output_path = os.path.join(path, fileName)

        with open(output_path, "w", encoding='utf-8') as f:
            if status == "json":
                f.write(data)
            else:
                f.write(json.dumps(data, indent=4, ensure_ascii=False))


# 单例实例
# config = ConfigManager()