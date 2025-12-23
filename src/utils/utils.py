import json
import os



class Utils:
    # 单例实例
    _instance = None

    def __init__(self):
        pass

    # 单例模式实现
    def __new__(cls, path=None):
        if not cls._instance:
            # 创建实例
            instance = super().__new__(cls)

            cls._instance = instance
        return cls._instance

    def save(self,path,fileName,data,status):
        os.makedirs(path, exist_ok=True)
        output_path = os.path.join(path, fileName)

        with open(output_path, "w", encoding='utf-8') as f:
            if status == "json":
                f.write(data)
            else:
                f.write(json.dumps(data, indent=4, ensure_ascii=False))



# 创建全局单例实例
app_Utils = Utils()



