# 钱迹数据采集脚本说明

这份文档只说明钱迹数据采集脚本的用途、文件位置、执行流程、输出文件和使用方式。

## 脚本能做什么

这个脚本用于通过钱迹 API 同步个人账单数据，并导出为项目统一使用的 JSON 文件。

目前会处理的信息包括：

- 钱迹账号 token 的登录、保存和按需刷新。
- 钱迹账单同步接口返回的账单变更数据。
- 接口返回的删除记录。
- 全量同步和基于同步游标的增量同步。
- 将钱迹账单整理成后续导入和展示需要的最终 JSON。

## 文件位置

```text
Personal Digital Archive/
├─ readme-qianji.md
└─ src/
   ├─ config/
   │  ├─ config.yaml
   │  │  └─ 本地真实配置，包含 Qianji.app_id，不提交到 Git
   │  ├─ config_Example.yaml
   │  │  └─ 配置示例，Qianji.app_id 留空
   │  ├─ qianji_token.json
   │  │  └─ 钱迹登录 token 文件，不提交到 Git
   │  └─ qianji_state/
   │     └─ state/sync_cursor.json
   │        └─ 增量同步游标
   ├─ Script/
   │  ├─ Script_API/
   │  │  ├─ qianji_token.py
   │  │  │  └─ 登录、保存 token、按需刷新 token
   │  │  └─ qianji_api.py
   │  │     └─ 调用钱迹同步 API，处理全量/增量同步
   │  └─ Script_Repeat/
   │     └─ qianji_main.py
   │        └─ 一键启动入口
   └─ data/
      ├─ Data_End/
      │  └─ qianji.json
      │     └─ 最终输出数据
      └─ Data_Star/
         ├─ qianji_raw_bills.json
         ├─ qianji_deletes.json
         └─ qianji_export_report.json
```

## 主要文件说明

`src/Script/Script_Repeat/qianji_main.py`

一键启动入口。日常只需要运行这个脚本。它会先检查 token，必要时自动调用登录流程获取 token，然后调用同步 API，最后写入 `Data_Star` 和 `Data_End`。

`src/Script/Script_API/qianji_token.py`

负责钱迹 token 相关逻辑，包括账号密码登录、token 保存、token 刷新和 token 状态检查。

`src/Script/Script_API/qianji_api.py`

负责调用钱迹同步接口。`--mode full` 会全量拉取；`--mode incremental` 会读取同步游标，只拉取上次同步后的变更。

`src/config/qianji_token.json`

保存钱迹登录 token 和账号授权信息。它属于敏感配置，不应提交到 Git。

`src/config/qianji_state/state/sync_cursor.json`

保存增量同步游标。`--mode incremental` 会读取这里的 `lasttimes`；`--mode full` 不读取游标。

`src/config/config.yaml`

保存真实本地配置，其中 `Qianji.app_id` 是钱迹微信登录使用的微信开放平台 AppID。这个文件不应提交到 Git。

`src/config/config_Example.yaml`

配置示例文件。里面包含空的 `Qianji.app_id` 字段和说明注释，用于提示本地配置需要填写什么。

## 如何执行

在项目根目录执行。

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

## 同步模式

`--mode full`

全量同步。不读取历史游标，不传 `lasttimes`，会重新拉取钱迹当前可同步的数据。首次同步建议使用这个模式。

`--mode incremental`

增量同步。会读取 `src/config/qianji_state/state/sync_cursor.json` 里的 `lasttimes`，请求接口时带上这个游标，只拉取上次同步后的变更。第一次运行或游标不存在时，效果会接近全量同步。

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

## 敏感文件

这些文件包含个人账号、token 或同步状态，不应提交到 Git：

```text
src/config/config.yaml
src/config/qianji_token.json
src/config/qianji_state/
src/data/Data_End/qianji.json
src/data/Data_Star/qianji_raw_bills.json
src/data/Data_Star/qianji_deletes.json
src/data/Data_Star/qianji_export_report.json
```
