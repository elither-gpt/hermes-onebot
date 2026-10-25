# hermes-onebot

让 [Hermes Agent](https://hermes-agent.nousresearch.com/) 接入 QQ 的平台插件。通过 OneBot v11 协议连接 [NapCat](https://github.com/NapNeko/NapCatQQ)，私聊、群聊、图片、语音、视频、文件都能收发。

插件放进 Hermes 的插件目录就能用，不需要修改 Hermes 本体。

## 功能

- **私聊**：发消息就回复。
- **群聊**：被 @、回复机器人的消息、戳一戳、命中关键词时才回复，不刷屏。
- **管理员**：可单独指定管理员，还可以设置只回复管理员。访客默认用不了记忆、历史会话等涉及机器人所有者隐私的工具。
- **适配 QQ 显示**：QQ 不支持 Markdown，回复发出前会自动去掉标题、加粗、表格等格式，常见数学符号（如 `\times`）也会换成 ×。
- **长消息**：超过长度自动分段，段与段之间有间隔，群里第一段会 @ 提问的人。
- **上下文**：引用的消息、合并转发的聊天记录会展开给模型看；群里被触发时，会带上前面几条群消息。
- **收媒体**：图片、语音、视频、文件下载到 Hermes 的缓存里，模型可以直接读取。引用消息里的媒体也会一起下载。
- **发媒体**：模型在回复里单独写一行 `MEDIA:<本机绝对路径>`，就会按类型发出图片、语音、视频或文件。
- **不重复发送**：上传超时、网络断开时，可能已经发出去的内容不会重发，避免刷屏。
- **定时任务**：Hermes 的定时任务可以直接推送到 QQ 私聊或群。
- **慢命令不堵塞**：下载、生图这类耗时的插件命令可以放在后台单独跑，群里其他人照常聊天。
- **WSL 支持**：NapCat 跑在 WSL（Linux 版 QQ）里也行，文件路径会自动转换。

## 环境要求

- Hermes Agent（插件用到的 `websockets`、`httpx` 已随 Hermes 自带）
- NapCat，已登录机器人 QQ，并开启**正向 WebSocket 服务器**
- 插件和 NapCat 要能读到对方的本地文件：同一台机器最简单；NapCat 在 WSL 里时请配置 `napcat_wsl_distro`

## 安装

1. 从 [Releases](../../releases) 下载 `hermes-onebot-<版本>.zip`，解压到 Hermes 插件目录，文件夹名必须是 `onebot`：

   | 系统 | 插件目录 |
   | --- | --- |
   | Windows | `%LOCALAPPDATA%\hermes\plugins\onebot\` |
   | Linux / macOS | `~/.hermes/plugins/onebot/` |

   解压后目录里应该能直接看到 `plugin.yaml`、`adapter.py`、`__init__.py`。

2. 在 NapCat 网页后台新建一个「WebSocket 服务器」（正向），记下端口和令牌，例如 `3001`。

3. 在 Hermes 的 `.env` 里填写连接信息：

   ```env
   ONEBOT_WS_URL=ws://127.0.0.1:3001
   ONEBOT_ACCESS_TOKEN=和 NapCat 里填的令牌一致
   ONEBOT_ALLOWED_USERS=10001,10002
   ```

4. 重启 Hermes 网关，私聊机器人测试一下。

## 配置

配置分两处：

- **`.env` 环境变量**：连接信息和白名单，适合放令牌这类敏感内容。
- **`config.yaml`**：其余选项都写在 `platforms.onebot.extra` 下面。

### 环境变量

| 变量 | 说明 | 示例 |
| --- | --- | --- |
| `ONEBOT_WS_URL` | NapCat 正向 WebSocket 地址（**必填**） | `ws://127.0.0.1:3001` |
| `ONEBOT_ACCESS_TOKEN` | 连接令牌，要和 NapCat 一致 | |
| `ONEBOT_ALLOWED_USERS` | 允许使用机器人的 QQ 号，逗号分隔 | `10001,10002` |
| `ONEBOT_ALLOW_ALL_USERS` | 设为 `true` 时所有人都能用（群里仍然要 @） | `false` |
| `ONEBOT_HOME_CHANNEL` | 定时任务默认推送目标：QQ 号，或 `g` + 群号 | `10001` / `g123456` |

`ws_url`、`access_token` 也可以写在 `config.yaml` 的 `extra` 里，效果相同。

### 最小配置示例

```yaml
platforms:
  onebot:
    enabled: true
    extra:
      admins: 10001          # 管理员 QQ 号
```

### 常用配置示例

```yaml
platforms:
  onebot:
    enabled: true
    extra:
      admins: [10001]
      keyword_triggers: [小助手]        # 群里说到「小助手」也会触发
      allowed_groups: [123456, 654321] # 只在这两个群里工作
      history_limit: 8                 # 群里触发时带上前 8 条消息
      max_message_length: 2000
      guest_deny_toolsets: [memory, session_search, connections, cronjob]
```

### 全部选项

以下选项都写在 `platforms.onebot.extra` 下。QQ 号、群号列表可以写成 YAML 列表，也可以写成逗号分隔的字符串。

#### 触发与权限

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `admins` | 空 | 管理员 QQ 号列表 |
| `admin_only_chat` | `false` | 只回复管理员，其他人的消息全部忽略 |
| `require_mention` | `true` | 群里需要 @、回复或戳一戳才触发；设为 `false` 则群里每条消息都回复 |
| `mention_all_triggers` | `true` | 群里 @全体成员 也算 @ 了机器人 |
| `keyword_triggers` | 空 | 群消息里包含这些词也会触发 |
| `keyword_only_trigger` | `false` | 群里**只**认关键词，忽略 @ 和回复；适合和其他机器人共用一个 QQ 号 |
| `allow_bare_group_commands` | `true` | 群里不 @ 直接发 `/命令` 也能触发。管理员发任何 `/...` 都会触发，普通成员只有发 Hermes 认识的命令才会触发 |
| `allowed_groups` | 空（全部） | 只在这些群里工作 |
| `blocked_users` | 空 | 完全忽略这些 QQ 号 |
| `restrict_guest_tools` | `true` | 非管理员使用时屏蔽 `guest_deny_toolsets` 里的工具 |
| `guest_deny_toolsets` | `memory, session_search, connections` | 对非管理员屏蔽的工具集 |

每个命令谁能用，仍然由 Hermes 自带的 `user_allowed_commands`、`group_user_allowed_commands` 等选项控制，插件不改变这部分规则。

#### 回复格式

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `format_markdown` | `true` | 发送前去掉 Markdown 格式 |
| `markdown_code_blocks` | `placeholder` | 代码块的处理方式：`placeholder` 换成「[代码块]」；`keep` 保留代码、只去掉 ``` 标记 |
| `anti_risk_mode` | `false` | 在链接的 `://` 后加空格，降低被 QQ 风控拦截的概率 |
| `max_message_length` | `4000` | 单条消息最大字数，超过就分段（最小 200） |
| `rate_limit_ms` | `1000` | 分段发送时每段的间隔，单位毫秒 |
| `group_at_sender` | `true` | 群里回复的第一段 @ 提问的人 |

#### 上下文

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `enrich_reply_forward_context` | `true` | 展开引用消息和合并转发，交给模型 |
| `max_reply_layers` | `1` | 引用链最多往上追几层 |
| `max_forward_layers` | `5` | 嵌套的合并转发最多展开几层 |
| `max_forward_messages_per_layer` | `8` | 每层合并转发最多取几条消息 |
| `max_chars_per_layer` | `900` | 每层最多保留多少字 |
| `max_total_context_chars` | `3000` | 展开内容的总字数上限 |
| `history_limit` | `5` | 群里触发时带上前面几条群消息，`0` 为关闭 |

#### 媒体

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `inbound_file_max_mb` | `2048` | 收到的文件、视频超过这个大小就不下载，单位 MB |
| `outbound_media_max_mb` | `1024` | 发出的媒体文件大小上限，单位 MB |
| `napcat_wsl_distro` | 空 | NapCat 跑在 WSL 里时填发行版名称，例如 `Ubuntu-24.04`，插件会自动转换两边的文件路径 |

#### 命令

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `command_aliases` | 空 | 命令别名，`别名: 真实命令`。例如 `{newsession: new}` 后，发 `/newsession` 等于发 `/new` |
| `background_commands` | 空 | 放到后台单独跑的慢命令，`命令名: 等待提示`。提示留空就不发；不带参数或参数是 `help` 时也不发 |
| `group_file_commands` | 空 | 群里执行这些命令产生的文件，传到群文件而不是直接发在聊天里（按命令名前缀匹配） |

`background_commands` 只对插件注册的命令生效。为什么要放后台：Hermes 里一个群共用一个会话，一个下载要跑半小时，群里其他人的消息就得一直排队；放到后台跑就不会堵住。同时最多跑 3 个。

```yaml
extra:
  command_aliases:
    newsession: new
  background_commands:
    download: "正在下载，请稍候……"
    draw: "正在生成图片，请稍候……"
  group_file_commands: [download]
```

## 给模型的提示

插件会告诉模型这些事情，一般不需要额外写进提示词：

- QQ 不显示 Markdown，要写纯文本
- 发文件用 `MEDIA:<本机绝对路径>`，单独占一行
- 发送者显示成 `QQ <号码>｜<昵称>`，只有号码可信，昵称谁都能改
- 用 `send_message` 主动发消息时，私聊写 `onebot:<QQ号>`，群聊写 `onebot:g<群号>`

## 常见问题

**连不上 NapCat**：确认 NapCat 开的是「WebSocket 服务器」（正向），不是「WebSocket 客户端」（反向）；端口和令牌要和 `.env` 一致。

**群里不回复**：先确认 @ 的是机器人本身。再检查群号是否在 `allowed_groups` 里、`keyword_only_trigger` 有没有打开。

**发图片或文件失败**：NapCat 要能读到这个文件。NapCat 在 WSL 或 Docker 里时，Windows 上的路径它读不到；WSL 请配置 `napcat_wsl_distro`，Docker 需要把文件目录挂载进容器。

**群文件上传失败**：QQ 有时会因为文件名里的词拒绝上传，插件会自动换一个中性的文件名再试一次。

## 更新记录

见 [CHANGELOG.md](CHANGELOG.md)。

## 致谢与许可

插件的触发规则、上下文展开、媒体收发等设计参考了 [constansino/openclaw_qq](https://github.com/constansino/openclaw_qq)（OpenClaw 的 QQ 插件，MIT 许可），代码针对 Hermes 的插件接口用 Python 重新编写。

本项目使用 MIT 许可，见 [LICENSE](LICENSE)。
