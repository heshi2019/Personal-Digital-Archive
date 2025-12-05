from scanner.scanner import Scanner
import uvicorn
import argparse
import datetime

if __name__ == "__main__":
    # Print program header
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ==========================================")
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Personal Digital Archive - Backend Service")
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ==========================================")

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

    if args.scan_only:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scan-only mode: Exiting program")
    else:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting API server...")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API address: http://127.0.0.1:5000")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API docs: http://127.0.0.1:5000/docs")

        ''' 
            Uvicorn是一个基于ASGI（Asynchronous Server Gateway Interface）的Python异步Web服务器
            run()是Uvicorn的核心函数，用于启动Web服务器并加载指定的ASGI应用
            "api.server:app"：
            
            这是一个ASGI应用的导入路径，遵循模块名:变量名的格式
            api.server表示项目中api/server.py模块
            app表示该模块中定义的FastAPI应用实例（在server.py第13行app = FastAPI()）
            host="127.0.0.1"：
            
            指定服务器监听的主机地址
            127.0.0.1表示本地回环地址，只接受来自本地的请求
            port=5000：
            
            指定服务器监听的端口号
            客户端需要通过这个端口访问API服务
        '''
        uvicorn.run("api.server:app", host="127.0.0.1", port=5000)