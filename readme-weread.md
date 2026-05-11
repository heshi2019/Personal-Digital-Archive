# 微信读书数据采集脚本说明

这份文档只说明微信读书数据采集脚本的用途、文件位置、执行流程、输出文件和使用方式。

## 脚本能做什么

这个脚本用于拉取微信读书账号中有笔记或划线的书籍数据，并整理成项目统一使用的最终 JSON。

目前会处理的信息包括：

- 微信读书 cookie 的读取、刷新、校验和扫码登录。
- 当前账号 notebook 列表。
- 单本书的基本信息。
- 划线、高亮和想法。
- 章节目录。
- 阅读进度、阅读时长和最近阅读时间。
- 每本书的阅读页 URL。

## 文件位置

```text
Personal Digital Archive/
├─ readme-weread.md
└─ src/
   ├─ config/
   │  └─ weread_cookie.json
   │     └─ 微信读书登录 cookie，不提交到 Git
   ├─ Script/
   │  ├─ Script_API/
   │  │  ├─ weread_cookie.py
   │  │  │  └─ cookie 读取、刷新、校验和扫码登录
   │  │  └─ weread_api_client.py
   │  │     └─ 微信读书 API 客户端
   │  └─ Script_Repeat/
   │     └─ Weread.py
   │        └─ 一键启动入口
   └─ data/
      ├─ Data_End/
      │  └─ weread_1.json
      │     └─ 最终输出数据
      └─ Data_Star/
         └─ weread/
            ├─ notebooks.txt
            └─ raw/<book_id>/
               ├─ reader_url.txt
               ├─ book.txt
               ├─ bookmarks.txt
               ├─ reviews.txt
               ├─ chapters.txt
               ├─ progress.txt
               └─ readinfo.txt
```

## 主要文件说明

`src/Script/Script_Repeat/Weread.py`

一键启动入口。它负责检查登录状态、调用微信读书 API、保存原始接口返回，并生成最终的 `weread_1.json`。

`src/Script/Script_API/weread_cookie.py`

负责微信读书 cookie 管理。它会读取 `src/config/weread_cookie.json`，尝试刷新并校验 cookie；如果 cookie 不存在或失效，会尝试打开浏览器进行扫码登录。

`src/Script/Script_API/weread_api_client.py`

负责真正调用微信读书接口，包括 notebook、书籍信息、划线、想法、目录、阅读进度、阅读信息等接口。

`src/config/weread_cookie.json`

保存微信读书登录 cookie。它属于敏感配置，不应提交到 Git。

`src/data/Data_End/weread_1.json`

最终输出文件，是后续统一导入和展示需要使用的数据文件。

`src/data/Data_Star/weread/`

保存每个接口的原始返回，方便排查接口问题和二次处理。

## 登录流程

登录检查、cookie 刷新和扫码登录已经融合在 `Weread.py` 的运行流程里。

直接运行同步脚本时，会先检查：

```text
src/config/weread_cookie.json
```

如果 cookie 存在且可用，脚本会直接同步数据。

如果 cookie 不存在、为空、过期或校验失败，脚本会尝试打开浏览器进行扫码登录。登录成功后，新的 cookie 会自动保存回 `src/config/weread_cookie.json`，然后继续执行同步。

唯一前提是当前运行环境允许 Playwright 打开浏览器。如果浏览器进程被权限限制拦截，需要在允许启动浏览器的环境中重新运行。

## 如何执行

在项目根目录执行：

```powershell
python -B .\src\Script\Script_Repeat\Weread.py
```

脚本默认执行全量同步：读取当前账号下所有有笔记或划线的书籍，逐本同步书籍信息、划线、想法、章节目录和阅读进度。

如果只想在代码里调用某一本书，可以调用：

```python
main(book_id="书籍ID")
```

或：

```python
full_sync(book_id="书籍ID")
```

## 同步模式

`full_sync()`

全量模式，重新请求所有目标书籍并重建最终数据。

`incremental_sync()`

增量合并模式。它会读取已有的：

```text
src/data/Data_End/weread_1.json
```

然后只追加新书、新划线/笔记和新目录项，不删除已有内容，也不覆盖已有字段值。

当前直接运行 `Weread.py` 时默认走 `main()`，也就是 `full_sync()`。

## 输出文件

最终整理后的微信读书数据会输出到：

```text
src/data/Data_End/weread_1.json
```

这个文件会按每本书的 `LastDay` 降序排列，最近阅读的书排在前面。

原始接口返回会输出到：

```text
src/data/Data_Star/weread/notebooks.txt
src/data/Data_Star/weread/raw/<book_id>/reader_url.txt
src/data/Data_Star/weread/raw/<book_id>/book.txt
src/data/Data_Star/weread/raw/<book_id>/bookmarks.txt
src/data/Data_Star/weread/raw/<book_id>/reviews.txt
src/data/Data_Star/weread/raw/<book_id>/chapters.txt
src/data/Data_Star/weread/raw/<book_id>/progress.txt
src/data/Data_Star/weread/raw/<book_id>/readinfo.txt
```

## 敏感文件

这些文件包含登录状态或个人阅读数据，不应提交到 Git：

```text
src/config/weread_cookie.json
src/data/Data_End/weread_1.json
src/data/Data_Star/weread/
```
