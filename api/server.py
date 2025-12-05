from fastapi import FastAPI, HTTPException, Response, Request
from fastapi.responses import StreamingResponse, FileResponse
import yaml, os, mimetypes
from db.operations import DB


# FastAPI框架，其实也就是python用于和前端通行的入口
# 和java中的SpringMVC的请求映射机制类似，都是将HTTP请求路径映射到后端处理函数的机制
# java中称为注解，通过反射实现，python中，@这种形式称为装饰器，基于函数式编程

app = FastAPI()
config = yaml.safe_load(open("config/config.yaml", encoding='utf-8'))
db = DB(config["database_path"])

@app.get("/api/files")
def list_files():
    rows = db.get_files()
    return [dict(r) for r in rows]

@app.get("/api/item/{file_id}")
def get_item(file_id: str):
    row = db.get_file_by_id(file_id)
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    return row

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
