import hashlib

BUFFER = 1024 * 1024

"""
计算文件的 SHA-256 哈希值。

参数:
    path (str): 文件的路径。

返回:
    str: 文件的 SHA-256 哈希值。
"""
def compute_hash(path: str) -> str:

    # hashlib模块提供各类哈希算法的实现，如下面的sha256算法。
    h = hashlib.sha256()

    # 以二进制只读模式打开文件，确保在不同操作系统下的兼容性。
    with open(path, "rb") as f:
        while True:
            # 每次读取BUFFER（1MB）大小的字节数据。大文件分块读取，避免内存溢出。
            chunk = f.read(BUFFER)
            if not chunk:
                break

            # 每次读取到的字节数据都更新到哈希对象中。
            h.update(chunk)

    # 计算完成后，返回哈希值的十六进制表示。
    return h.hexdigest()
