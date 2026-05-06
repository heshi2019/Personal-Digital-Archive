## 简介

本项目是一个个人数据收集，将你在互联网上留在各个app和网站的个人数据收集起来。使用时间轴前端串联展示。

预期会包含以下功能：

|                平台                 | 功能      | 最后更新       |   开发状态   | 备注                        |
|:---------------------------------:|---------|------------|:--------:|---------------------------|
|               微信读书                | 书籍笔记与划线 | 2025-04-01 |  ✅ 已完成   | 在25年中旬，微信读书更新了API，该接口已不可用 |
|                豆瓣                 | 已标记电影   | 2025-04-06 |  ✅ 已完成   | -                         |
|                机核                 | 电台信息    | 2025-04-08 |  ✅ 已完成   | -                         |
|              网易蜗牛读书               | 书籍笔记与划线 | 2025-04-01 |  ✅ 已完成   | 使用模拟器+抓包工具获取cookie        |
|               魅族便签                | 便签数据    | 2025-04-02 |  ✅ 已完成   | -                         |
|              掌阅读书信息               | 籍笔记与划线  | 2025-04-03 |  ✅ 已完成   | 无API，数据整合脚本               |
|               flomo               | 笔记和图片   | 2025-04-21 |  ✅ 已完成   | -                         |
|                钱迹                 |         | 2025-04-08 | 官方导出JSON | app直接导出JSON               |
|                本地                 | 图片笔记入库  | 2026.01.08 |  ✅ 已完成   | 增加本地ollama对笔记内容总结         |
|               小米手环                | 个人健康数据  | 2026.01.08 |  ✅ 已完成   | 官方数据导出，脚本整合数据             |
|            QZoneExport            | 时间轴数据整合 | -          |  ❌ 未开始   | -                         |
|                高德                 | 出行轨迹    | -          |  ❌ 未开始   | 我的-出行里程-里程-全部记录           |
| [missAV](https://missav.ai/dm265) | 收藏视频    | -          |  ❌ 未开始   |                           |
|                知乎                 | 我的收藏、回答 | -          |  ❌ 未开始   | 暂时延后                      |

 - 数据导入MYSQL   已完成
 - 数据导入SQLite  已完成


---
## 使用

将src/config/config_Example.yaml，去掉_Example，填入对应cookie
### 后端

数据更新（本地，网络）
```
python src/Main_Execution_In.py
```


后端接口启动
```angular2html
python src/Controller_Api_Run.py
```

# qianji collector

钱迹个人数据采集脚本。日常只需要运行 `qianji_main.py`，它会先检查 token，必要时自动登录获取 token，然后同步钱迹数据并导出为项目统一格式。

## 涉及文件

- `src/Script/Script_API/qianji_token.py`: 登录、保存 token、按需刷新 token。
- `src/Script/Script_API/qianji_api.py`: 调用钱迹同步 API，处理全量/增量同步和同步游标。
- `src/Script/Script_Repeat/qianji_main.py`: 主入口，串起 token、API 同步、Data_Star 原始数据输出和 Data_End 最终数据输出。

## token 与状态文件

- `src/config/qianji_token.json`: 钱迹登录 token 文件，包含账号授权信息，属于敏感配置，不应提交到 Git。
- `src/config/qianji_state/state/sync_cursor.json`: 增量同步游标。`--mode incremental` 会读取这里的 `lasttimes`，只拉取上次同步后的变更；`--mode full` 不读取游标，会重新拉取全量数据。

## 常用命令

首次运行或 token 失效时，直接用 main 登录并同步：

```powershell
python -B .\src\Script\Script_Repeat\qianji_main.py --mode full --account 手机号 --password 密码
```

如果不想把密码写在命令里，可以只传账号，脚本会在终端提示输入密码：

```powershell
python -B .\src\Script\Script_Repeat\qianji_main.py --mode full --account 手机号
```

已有可用 token 后，日常增量同步：

```powershell
python -B .\src\Script\Script_Repeat\qianji_main.py --mode incremental
```

只检查当前 token：

```powershell
python -B .\src\Script\Script_API\qianji_token.py show --json
```

## 输出文件

最终数据会固定输出到：

```text
src/data/Data_End/qianji.json
```

原始同步数据和报告会输出到：

```text
src/data/Data_Star/qianji_raw_bills.json
src/data/Data_Star/qianji_deletes.json
src/data/Data_Star/qianji_export_report.json
```

`Data_End/qianji.json` 用于后续统一导入和展示；`Data_Star` 中的文件保留原始账单、删除记录和导出报告，方便排查接口返回和二次处理。

# WeRead collector

微信读书个人数据采集脚本，用于拉取有笔记或划线的书籍数据，并整理成项目统一的最终 JSON。

## 涉及文件

- `src/Script/Script_API/weread_cookie.py`: 微信读书 cookie 管理，负责读取、刷新、校验 cookie；cookie 不可用时会尝试扫码登录。
- `src/Script/Script_API/weread_api_client.py`: 微信读书 API 客户端，封装 notebook、书籍信息、划线、评论、目录、阅读进度等接口。
- `src/Script/Script_Repeat/Weread.py`: 主运行脚本，串起 cookie、API 请求、原始数据保存和最终数据生成。

## cookie 文件

- `src/config/weread_cookie.json`: 微信读书登录 cookie 文件，属于敏感配置，不应提交到 Git。

脚本运行时会先检查这个 cookie；如果 cookie 不存在或失效，会尝试打开浏览器进行扫码登录，登录成功后自动保存到这个文件。

也就是说，登录检查、cookie 刷新和扫码登录已经融合在 `Weread.py` 的运行流程里。第一次运行或 cookie 过期时，直接执行微信读书同步命令即可，不需要单独运行 `weread_cookie.py`。唯一前提是当前环境允许 Playwright 打开浏览器；如果浏览器进程被权限限制拦截，需要在允许启动浏览器的环境中重新运行。

## 常用命令

运行微信读书同步：

```powershell
python -B .\src\Script\Script_Repeat\Weread.py
```

脚本默认执行全量同步：读取当前账号下所有有笔记或划线的书籍，逐本同步书籍信息、划线、想法、章节目录和阅读进度。

如果只想在代码里调用某一本书，可以调用 `main(book_id="书籍ID")` 或 `full_sync(book_id="书籍ID")`。

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

`Data_End/weread_1.json` 用于后续统一导入和展示；`Data_Star/weread` 中保留每个接口的原始返回，方便排查和二次处理。

## 同步模式

- `full_sync()`: 全量模式，重新请求所有目标书籍并重建最终数据。
- `incremental_sync()`: 增量合并模式，会读取已有 `src/data/Data_End/weread_1.json`，只追加新书、新划线/笔记和新目录项，不删除已有内容，也不覆盖已有字段值。

当前直接运行 `Weread.py` 时默认走 `main()`，也就是 `full_sync()`。


### 前端
```angular2html
-- 整体前端
cd vue


npm install

npm run dev

-- gcores前端
cd vue_gcores

npm run dev
```
前端目前有一个测试接口，请求豆瓣的电影接口，测试前端渲染用的

gemini3生成的页面无法直接使用，用trae重构了下，时间轴展示大量数据时样式有问题



---
## 后续更新

| 位置 | 功能点      | 目前状态          | 希望状态                           | 备注                                           | 状态                     |
|:--:|----------|---------------|--------------------------------|----------------------------------------------|------------------------|
| 后端 | gcores脚本 | 全量更新          | 添加增量更新                         | 需要权衡输出json的数据，增量更新-增量更新json/全量更新json-增量更新数据库 | 增量/全量更新json，全量更新sqlite |
| 后端 | 本地脚本     | 全量更新          | 添加增量更新                         | 如何增量扫描本地                                     |                        |
| 后端 | 持续运行     | -             | 脚本完善，定时执行                      | 筛选可重复执行脚本                                    |                        |
| 前端 | 页面构建     | 唯一一版可本地运行的vue | gemini3生成样式，vue3脚手架项目          | gemini3无法直接生成vue脚手架项目                        |                        |
| 前端 | 展示逻辑     | -             | 时间轴四级颗粒度展示（all，year，month，day） | 如何聚合数据，哪些数据是有价值的                             |                        |
| 前端 | 自动构建离线页面 |  -            |如项目[QZoneExport](https://github.com/ShunCai/QZoneExport)所示 |                                              |                        |

前端页面要如何展示数据，时间轴展示可以提现数据的变化情况，要如何提现，我想要什么样的东西，哪些数据对我是有价值的，哪些数据（决定）是具有深远影响的。

---

## 项目结构

```
├── LifeBitsCollector
│   ├── .github
│   │   └──workflows
│   │       └──weread.yml    # 微信读书自动部署脚本
│   ├── LifeBitsCollector
│   │   ├── BasicDate        # 基础数据
│   │   │   ├── flomo        # flomo
│   │   │   └── zhangyue     # 掌阅
│   │   │       ├── 掌阅笔记  	    # 笔记
│   │   │       │   └── ……  	    # 具体笔记
│   │   │       └── 掌阅-批注书籍  	# 所有书籍
│   │   ├── Config           # 工具类
│   │   │   ├── config_manager.py  	# 工具类
│   │   │   └── mysql_conn.py     	# 数据库连接工具
│   │   ├── Data_End         # 结果输出文件夹
│   │   │   ├── douban.json  	# 豆瓣数据
│   │   │   ├── du.json      	# 网易蜗牛读书数据
│   │   │   ├── flyme.json   	# 魅族便签数据
│   │   │   ├── weread.json  	# 微信读书数据
│   │   │   ├── flyme.json   	# 魅族便签数据
│   │   │   └── weread.json  	# 微信读书数据
│   │   ├── Data_Star        # 相关API返回数据，如需二次开发，可在这里查看原始数据
│   │   │   └── ……
│   │   ├── FlymeImages      # 魅族便签图片
│   │   │   └── ……
│   │   ├── ImportMySQL      # mysql导入脚本
│   │   │   └── ……
│   │   └── JsonExample      # 示例json文件
│   │       └── ……
│   ├── config.yaml          # 配置文件
│   ├── requitrmrnts.txt     # 第三方Python包
│   └── setup.py        
```

---

## gemini生成页面问题

```angular2html

好，我们来梳理下思路，在页面展示方面gpt告诉我首先确定页面逻辑，再决定页面样式，这样其实也是很好的，
因为首先要确定前端页面的功能，再决定页面长什么样。

另外前端页面目前展示的数据，目前的数据是好几个平台的数据，都有到天的数据，如果全部显示时间轴太拥挤，
另外时间轴其实也不用展示所有数据，要展示相关数据的变化，这个要怎么展示还没想好，另外现在的这些数据，
其实也要聚合然后向上，渲染到时间轴上，怎么聚合呢。

哪些数据需要穿透日颗粒度，渲染到月颗粒度上，哪些需要穿透到年颗粒度呢。

另外现在的前端由gemini3生成，页面非常精美，动画效果，互动按钮效果都不错，但可惜的是这个页面用不了，
只能在gemini3的浏览器页面中浏览，因为项目使用的esm模式构建，然后页面里面用的是字符串模板，
但这样这个项目其实是个死扣，没办法在本地运行，

启动要使用npm vite运行，但项目配置文件（vite.config.ts）需要依赖本地安装的 Vite，
而使用npm install之后再运行项目，前端只会显示一个背景，其他啥也不显示，这是因为在 Vue 组件里
用了 template 选项，但当前 Vue 版本不支持运行时编译模板（esm-bundler 版本默认关闭）。
所以这个页面是需要重构的，

好在，现在已经有个确定的前端样式来确定前端的模样了

```

---

## 项目起源

23年我毕业后，为了谋生找到了一份工作，这份工作能让我在社会上活下去，但也让我几近奔溃，一想到这样的日子还有四十年（现在或许更多），心情更加绝望了。 

在那无数的夜晚，我在想我是一个什么样的人，我想过什么样的生活，未来是什么样的，这一路的终点是什么。

而我们这么拼命的活下去，是为了什么。

未来是由现在铸造的，而现在是由曾经决定的

过往的曾经就在这个文件夹中，每一条记录都有时间，如果把他们串在时间轴上。

那就是我的模样

![结局](https://wx4.sinaimg.cn/mw690/005Kem6Tly1i09o29g851j337k2eob2a.jpg)
