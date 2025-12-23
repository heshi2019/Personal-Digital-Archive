from src import QianJiApi

def main():

    # 2025.12.08
    # 好消息是，钱迹手机app可以直接导出json格式的数据，
    # 坏消息是，目前这个自动化接口还没法用
    qainji_api = QianJiApi()
    qainji_api.get_data()
    qainji_api.get_catefories()

if __name__ == "__main__":
    main()
