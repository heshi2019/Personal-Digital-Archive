import yaml
from pathlib import Path
from dataclasses import dataclass
from typing import List

@dataclass
class MYSQLConfig:
    host: str
    user: str
    password: str
    database: str

@dataclass
class FlomoConfig:
    file_path: str
    authorization: str

@dataclass
class ThumbnailConfig:
    size: list
    enabled: bool
    path: str



@dataclass(init=False)  # 关键：禁用自动生成的__init__
# @dataclass装饰的类会自动生成__init__方法，该方法要求必须传入所有字段的参数
class AppConfig:
    # 单例实例
    _instance = None

    douban_username: str
    Grcores:str

    flyme_cookie: str
    du_cookie: str
    weread_cookie: str
    zhangyue_cookie: str
    qianji_cookie: str

    MySQL: MYSQLConfig
    Flomo: FlomoConfig
    scan_paths: List[str]
    skip_paths: List[str]
    thumbnail: ThumbnailConfig
    SQLitePath: str
    Data_End: str
    Data_Star: str

    # 手动定义空的__init__，避免参数检查
    def __init__(self):
        pass

    # 单例模式实现
    def __new__(cls, path=None):
        if not cls._instance:
            # 创建实例
            instance = super().__new__(cls)

            # 加载配置
            if path is None:
                current_file = Path(__file__).resolve()
                path = current_file.parent / "config.yaml"

            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)

            # 将数据库相对路径转换为绝对路径
            current_file = Path(__file__).resolve()
            db_path = Path(data["SQLite"]["path"])
            if not db_path.is_absolute():
                data["SQLite"]["path"] = str(current_file.parent.parent / db_path)

            data_end = Path(data["Data_End"])
            if not data_end.is_absolute():
                data["Data_End"] = str(current_file.parent.parent / data_end)

            data_star = Path(data["Data_Star"])
            if not data_star.is_absolute():
                data["Data_Star"] = str(current_file.parent.parent / data_star)

            thumbnails_path = Path(data["thumbnails"]["path"])
            if not thumbnails_path.is_absolute():
                data["thumbnails"]["path"] = str(current_file.parent.parent / thumbnails_path)

            flomo_file_path = Path(data["FLOMO"]["file_path"])
            if not flomo_file_path.is_absolute():
                data["FLOMO"]["file_path"] = str(current_file.parent.parent / flomo_file_path)




            instance.douban_username = data["DOUBAN"]["user"]
            instance.Grcores = data["GRCORES"]["cookie"]
            instance.flyme_cookie = data["FLYME"]["cookie"]
            instance.du_cookie = data["DU"]["cookie"]
            instance.weread_cookie = data["WEREAD"]["cookie"]
            instance.zhangyue_cookie = data["ZHANGYUE"]["cookie"]
            instance.qianji_cookie = data["QIANJI"]["cookie"]
            instance.MySQL = MYSQLConfig(**data["MySQL"])
            instance.Flomo = FlomoConfig(**data["FLOMO"])
            instance.scan_paths = data["scan_paths"]
            instance.skip_paths = data["skip_paths"]
            instance.thumbnail = ThumbnailConfig(**data["thumbnails"])
            instance.SQLitePath = data["SQLite"]["path"]
            instance.Data_End = data["Data_End"]
            instance.Data_Star = data["Data_Star"]

            cls._instance = instance
        return cls._instance


# 创建全局单例实例
app_config = AppConfig()






'''

在Python中，单星号（*）和双星号（**）有多种不同的作用，主要用于解包、可变参数和参数传递。下面结合您提供的代码示例详细解释：

1. 单星号（*）的作用
1.1 解包可迭代对象
在函数调用时，将列表、元组、字符串等可迭代对象解包为单个元素：


python
Apply
# 例子1：解包列表
def add(a, b, c):
    return a + b + c

numbers = [1, 2, 3]
result = add(*numbers)  # 等价于 add(1, 2, 3)
print(result)  # 输出: 6

# 例子2：解包字符串
print(*"hello")  # 输出: h e l l o
1.2 可变参数（*args）
在函数定义时，用于接收任意数量的位置参数，这些参数会被收集到一个元组中：


python
Apply
def sum_all(*args):
    return sum(args)

print(sum_all(1, 2, 3))  # 输出: 6
print(sum_all(4, 5, 6, 7))  # 输出: 22
1.3 强制关键字参数
在函数定义时，*后面的参数必须用关键字方式传递：


python
Apply
def func(a, *, b, c):
    return a + b + c

# 正确调用
func(1, b=2, c=3)  # 输出: 6

# 错误调用（b和c必须用关键字参数）
# func(1, 2, 3)  # TypeError
1.4 忽略多个值
在解包时，用*来忽略某些值：


python
Apply
# 例子1：忽略中间值
first, *middle, last = [1, 2, 3, 4, 5]
print(first)    # 输出: 1
print(middle)   # 输出: [2, 3, 4]
print(last)     # 输出: 5

# 例子2：忽略所有值
*_, last = [1, 2, 3, 4, 5]
print(last)     # 输出: 5
2. 双星号（**）的作用
2.1 解包字典（关键字参数传递）
在函数调用时，将字典解包为关键字参数：


python
Apply
def user_info(name, age, city):
    return f"{name} is {age} years old from {city}"

user_data = {"name": "Alice", "age": 30, "city": "Beijing"}
result = user_info(**user_data)  # 等价于 user_info(name="Alice", age=30, city="Beijing")
print(result)  # 输出: Alice is 30 years old from Beijing
这正是您代码中使用的方式，如第44-45行：


python
Apply
thumbnail=ThumbnailConfig(**data["thumbnails"]),  # 解包thumbnails字典创建ThumbnailConfig对象
SQLite=SQLiteConfig(**data["SQLite"]),            # 解包SQLite字典创建SQLiteConfig对象
这里**data["thumbnails"]将字典data["thumbnails"]的键值对解包为关键字参数传递给ThumbnailConfig构造函数。

2.2 可变关键字参数（**kwargs）
在函数定义时，用于接收任意数量的关键字参数，这些参数会被收集到一个字典中：


python
Apply
def print_info(**kwargs):
    for key, value in kwargs.items():
        print(f"{key}: {value}")

print_info(name="Bob", age=25, job="Engineer")
# 输出:
# name: Bob
# age: 25
# job: Engineer
2.3 合并字典（Python 3.5+）
在Python 3.5及以上版本中，可以用**来合并多个字典：


python
Apply
dict1 = {"a": 1, "b": 2}
dict2 = {"b": 3, "c": 4}
merged = {**dict1, **dict2}  # 后面的字典会覆盖前面的同名键
print(merged)  # 输出: {'a': 1, 'b': 3, 'c': 4}
3. 总结
单星号（*）：主要用于解包可迭代对象、定义可变位置参数、强制关键字参数和忽略值。
双星号（**）：主要用于解包字典、定义可变关键字参数和合并字典。
在您的configClass.py中，双星号（**）被用来将YAML配置文件中解析出的字典数据，方便地传递给各个配置类的构造函数，这是Python中处理配置数据的常用且简洁的方式。'''