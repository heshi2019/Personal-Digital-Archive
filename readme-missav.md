# MissAV 收藏归档脚本说明

这份文档只说明 MissAV 收藏归档脚本的用途、文件位置、执行流程和使用方式。

## 脚本能做什么

这个脚本用于抓取 MissAV 账号里的“我的影片收藏”列表，并把每个收藏视频整理成统一 JSON 数据，同时下载封面图和预览 MP4。

目前会抓取的信息包括：

- 收藏页中的标题、详情页链接、封面图、预览 MP4、时长、角标。
- 详情页中的简介。
- 详情页中的字段信息，例如发行日期、番号、标题、女优、男优、类型、系列、发行商、导演等。
- 详情页“磁力下载”里可见的 magnet 链接。
- 收藏列表分页，目前会自动遍历页面上能识别到的所有收藏分页。

最终数据用于后续重建本地收藏页面。

## 文件位置

```text
Personal Digital Archive/
├─ readme-missav.md
├─ .codex-missav-browser-profile/
│  └─ MissAV 可见 Chrome 的固定浏览器档案
└─ src/
   ├─ config/
   │  └─ missav_cookie.json
   │     └─ MissAV 登录状态，Playwright storageState 格式
   ├─ Script/
   │  ├─ Script_API/
   │  │  ├─ missav_session.mjs
   │  │  │  └─ 登录态检查、弹出 Chrome、保存 cookie、切换无头浏览器
   │  │  └─ missav_api.mjs
   │  │     └─ 抓收藏页、抓详情页、下载封面和 preview、整理数据
   │  └─ Script_Repeat/
   │     └─ missav.mjs
   │        └─ 一键启动入口
   └─ data/
      ├─ Data_End/
      │  └─ missav.json
      │     └─ 最终输出数据
      └─ middle_data/
         └─ missav_assets/
            └─ 每个作品的 cover.jpg 和 preview.mp4
```

## 主要文件说明

`src/Script/Script_Repeat/missav.mjs`

一键启动入口。它负责启动整个归档任务，调用登录态管理和抓取 API，最后输出 `missav.json`。

`src/Script/Script_API/missav_session.mjs`

负责 MissAV 登录态相关逻辑。它会先尝试用 `missav_cookie.json` 启动无头浏览器；如果失败，就弹出普通 Chrome，让用户确认登录状态；确认后保存新的登录状态，再尝试切回无头浏览器执行。

`src/Script/Script_API/missav_api.mjs`

负责真正的数据抓取和资源下载。它会访问收藏页分页、进入每个视频详情页、提取简介和字段、提取 magnet 链接，并下载封面和 preview MP4。

`src/config/missav_cookie.json`

保存 MissAV 登录状态。它不是普通意义上只有 cookie 的文件，而是 Playwright 的 storage state，里面包含 cookies 和 localStorage 等状态。

`.codex-missav-browser-profile/`

普通 Chrome 的固定用户档案目录。脚本弹出 Chrome 时会使用这个目录，所以它可以保留真实浏览器中的登录状态、站点验证状态和浏览器环境。

`src/data/Data_End/missav.json`

最终输出文件，是后续真正需要使用的数据文件。

`src/data/middle_data/missav_assets/`

下载下来的资源文件目录。每个作品会有一个子目录，里面通常包含：

```text
cover.jpg
preview.mp4
```

## 输出 JSON 格式

`missav.json` 是一个数组，每个元素大致如下：

```json
{
  "title": "作品标题",
  "href": "https://missav.ai/dm5/cn/example",
  "cover": "https://example.com/example/cover-t.jpg",
  "preview": "https://example.com/example/preview.mp4",
  "duration": "3:02:13",
  "badge": "无码影片",
  "description": "详情页简介文本",
  "fields": {
    "发行日期": "2025-05-09",
    "番号": "SONE-711-UNCENSORED-LEAK",
    "标题": "原始标题",
    "女优": "演员名",
    "男优": "演员名",
    "类型": "类型标签",
    "系列": "系列名",
    "发行商": "发行商",
    "导演": "导演"
  },
  "visibleMagnetLinks": [
    "magnet:?xt=urn:btih:..."
  ]
}
```

注意：`cover` 和 `preview` 字段保留的是网站原始 URL；本地下载文件在 `src/data/middle_data/missav_assets/` 下。

## 如何执行

在项目根目录执行：

```powershell
& "C:\Users\28484\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe" ".\src\Script\Script_Repeat\missav.mjs"
```

如果后续配置了统一启动器，也可以从统一入口调用这个脚本。

## 执行流程

```text
启动 missav.mjs
  ↓
检查 src/config/missav_cookie.json
  ↓
尝试用无头浏览器打开 https://missav.ai/cn/saved
  ↓
如果无头浏览器能看到收藏卡片
  ↓
直接在后台抓取收藏页、详情页、封面和 preview
  ↓
输出 src/data/Data_End/missav.json
```

如果无头浏览器登录态不可用，流程会变成：

```text
无头浏览器检查失败
  ↓
弹出普通 Chrome
  ↓
用户确认 Chrome 中已经打开收藏页并处于登录状态
  ↓
回到终端按 Enter
  ↓
脚本保存新的 src/config/missav_cookie.json
  ↓
再次尝试无头浏览器
  ↓
如果无头浏览器可用，则后台继续抓取
  ↓
如果无头浏览器仍不可用，则复用当前可见 Chrome 执行本次任务
```

## 用户需要做什么

正常情况下：

1. 执行脚本。
2. 如果没有弹浏览器，就等待脚本跑完。
3. 如果弹出 Chrome，检查页面是否已经是 MissAV 收藏页。
4. 如果已经登录并看到收藏列表，回到终端按 Enter。
5. 如果没有登录，先在弹出的 Chrome 中登录，看到收藏列表后再回终端按 Enter。
6. 等待终端显示任务完成。

终端完成时会显示最终输出文件路径。

## 为什么有时会弹出 Chrome

MissAV 对登录态和浏览器环境比较敏感。脚本优先使用无头浏览器在后台执行，但无头浏览器有时无法通过站点校验。

弹出的普通 Chrome 主要有两个作用：

- 让用户完成登录或确认登录状态。
- 用真实 Chrome 环境刷新站点需要的 cookies、localStorage 或验证状态。

用户按 Enter 后，脚本会把这个状态保存到 `src/config/missav_cookie.json`，然后再尝试切回无头浏览器。

## 为什么需要按 Enter

按 Enter 是一个人工确认点。

它告诉脚本：

- 可见 Chrome 已经打开。
- 页面已经加载到收藏页。
- 如果需要登录，用户已经登录完成。
- 脚本现在可以读取并保存浏览器状态。

后续可以优化成自动检测收藏卡片出现后继续执行，但目前保留 Enter 可以避免脚本过早接管登录阶段，减少 403 或登录失败。

## 关于无头浏览器

无头浏览器就是没有窗口界面的 Chrome。它适合后台执行，不影响用户操作电脑。

当前策略是：

- 优先无头浏览器。
- 无头失败时弹出普通 Chrome 刷新会话。
- 刷新后再回到无头浏览器。
- 如果无头仍失败，才复用普通 Chrome 执行本次任务。

## 当前已知限制

- MissAV 的反爬和登录校验不稳定，不能保证每次都完全静默执行。
- 第一次运行或登录态过期时，可能需要弹出 Chrome。
- `missav_cookie.json` 有价值，但它不一定能独立保证无头浏览器永远可用。
- 普通 Chrome profile `.codex-missav-browser-profile/` 对保持登录状态也很重要。
- 脚本当前不下载正片，只下载收藏卡片封面和 preview MP4。
- 脚本只保留可见 magnet 链接，也就是 `visibleMagnetLinks`。
- 右侧推荐视频、广告、页面底部推荐内容不会被刻意抓取。

## 旧的中间文件

早期调试时曾经使用过：

```text
src/data/middle_data/missav_progress.json
```

它只是中途进度备份文件，不是最终产物。现在脚本已经不再写入这个文件，最终只关注：

```text
src/data/Data_End/missav.json
src/data/middle_data/missav_assets/
```

## 排错提示

如果终端显示“后台登录态不可用”，不一定表示账号退出了。它只表示无头浏览器没能确认收藏页可用。弹出的普通 Chrome 可能仍然是登录状态。

如果弹出的 Chrome 已经在收藏页，直接按 Enter。

如果弹出的 Chrome 显示登录框，登录后进入收藏页，再按 Enter。

如果出现 `405 Method Not Allowed`，优先确认地址是否为：

```text
https://missav.ai/cn/saved
```

如果出现 `.thumbnail` 超时，说明脚本没有在页面里找到收藏卡片。终端会打印当前页面 URL、标题和内容预览，用来判断它到底进了登录页、错误页还是空页面。

