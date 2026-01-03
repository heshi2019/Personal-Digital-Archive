from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware  # FastAPI的CORS中间件
from fastapi.responses import FileResponse
import os, mimetypes

from src.DB.SQLite_util import SQLite_util
from src.DB.controllerSelectSQLite import controllerSelectSQLite
from src.config.configClass import app_config

# FastAPI框架，其实也就是python用于和前端通行的入口
# 和java中的SpringMVC的请求映射机制类似，都是将HTTP请求路径映射到后端处理函数的机制
# java中称为注解，通过反射实现，python中，@这种形式称为装饰器，基于函数式编程

app = FastAPI()

# 配置CORS中间件（FastAPI方式）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源，生产环境应该限制为特定域名
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有HTTP方法
    allow_headers=["*"],  # 允许所有HTTP头
)

# 初始化数据库，并创建对象
db = SQLite_util(app_config.SQLitePath)
controllerSelectSQLite = controllerSelectSQLite(db)


@app.get("/api/files")
def list_files():
    rows = controllerSelectSQLite.select_files_data()
    if not rows:
        raise HTTPException(status_code=404, detail="not found")
    return rows


@app.get("/api/douban")
def get_item():
    rows = controllerSelectSQLite.select_douban_data()
    if not rows:
        raise HTTPException(status_code=404, detail="not found")
    return rows




@app.get("/api/thumbnail/{file_id}")
def get_thumbnail(file_id: str):
    row = db.get_file_by_id(file_id)
    if not row or not row.get('thumbnail_path'):
        raise HTTPException(status_code=404, detail="thumbnail not found")
    path = row['thumbnail_path']
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="thumbnail file missing")
    return FileResponse(path, media_type='image/jpeg')

@app.get("/api/file/{file_id}")
def serve_file(file_id: str, request: Request):
    row = db.get_file_by_id(file_id)
    if not row:
        raise HTTPException(status_code=404, detail="file not found")
    path = row['path']
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="file missing on disk")
    # For videos, let the server return the file directly supporting Range if possible.
    return FileResponse(path, media_type=mimetypes.guess_type(path)[0] or 'application/octet-stream')


@app.get("/api/serve-file-by-path/{full_path:path}")
def serve_file_by_path(full_path: str):
    # 确保路径是绝对路径
    if not os.path.isabs(full_path):
        raise HTTPException(status_code=400, detail="path must be absolute")
    
    # 检查文件是否存在
    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="file not found")
    
    # 检查是否是文件
    if not os.path.isfile(full_path):
        raise HTTPException(status_code=400, detail="path must point to a file")
    
    # 猜测文件的MIME类型
    mime_type, _ = mimetypes.guess_type(full_path)
    
    # 返回文件响应
    return FileResponse(full_path, media_type=mime_type or 'application/octet-stream')