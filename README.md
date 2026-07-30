# 小黑盒bot

小黑盒bot 是一个运行在 AstrBot 内的小黑盒社区插件。它可以让机器人使用现有的人设和会话上下文回复社区评论与私信，也可以通过 LLM 工具读取账号内容、搜索帖子、发布图文、管理互动并查询消息统计。

插件独立运行，不需要额外部署桥接服务或数据库服务。登录、消息处理、自动回复、图片上传、数据归档和 WebUI 都由插件完成。

本仓库基于 [Whereis-Alice/astrbot_plugin_xhhrobot](https://github.com/Whereis-Alice/astrbot_plugin_xhhrobot) 继续开发，感谢原作者 Whereis-Alice 创建并开源本插件。

> [!WARNING]
> 本插件使用的不是小黑盒面向第三方开放的稳定公共 API。接口变化、账号状态和平台风控都可能影响功能。建议先使用测试账号和允许列表验证，并保持默认限速。

[快速开始](#快速开始) · [自动回复](#人设与消息链) · [LLM 工具](#自然语言与-llm-工具) · [消息数据库](#消息数据库与-webui) · [常见问题](#常见问题)

## 主要功能

| 能力 | 说明 |
| --- | --- |
| 人设自动回复 | 将小黑盒评论和私信转换为 AstrBot 标准消息事件，继续使用已配置的人设、会话历史和兼容的消息钩子。 |
| 评论与私信 | 处理明确提及机器人的评论、机器人自己帖子下的普通评论，以及可选的好友或陌生人私信。 |
| 当前账号视图 | 读取当前登录账号的通知、收藏、服务端草稿、私信，以及自己发布的帖子、评论和动态。 |
| 自主巡帖 | 定时浏览推荐流，由模型选择帖子、读取正文并决定评论或跳过。 |
| LLM 工具 | 提供社区浏览、搜索、帖子、评论、用户、话题、收藏、点赞、关注、私信、发帖、草稿和统计工具。 |
| 图片与富文本 | 将评论、被回复评论、原帖和私信图片交给视觉模型；支持网络图片、本地图片、Base64 图片和有序富文本发帖。 |
| 消息数据库 | 使用 SQLite 归档评论和私信，保存去重结果、处理状态和机器人发出的评论，并通过 WebUI 或 LLM 工具查询。 |
| 人工审核 | 可选择先生成回复再审草稿，或先审原消息再生成；指定私信用户可免审自动发送。 |
| WebUI | 提供扫码登录、运行状态、消息统计、筛选查询和消息详情页面。 |
| 写入保护 | 写工具默认关闭，并带有管理员权限、允许列表、可选逐次确认、冷却、去重和发送结果不确定保护。 |

## 环境要求

- AstrBot `>=4.24.5,<5`
- Python 3.10 或更高版本
- 一个可用的小黑盒账号
- 一个已在 AstrBot 中配置的 LLM 提供商
- 如需识别评论或帖子图片，所选模型必须支持视觉输入

默认配置偏保守：刚安装时允许列表为空，私信自动回复、自主巡帖和 LLM 写工具均为关闭状态。完成登录不会立即让账号自动发言。

## 快速开始

### 1. 安装插件

可以在 AstrBot WebUI 的插件管理页通过仓库地址安装，也可以把仓库克隆到 AstrBot 的 `data/plugins/astrbot_plugin_xhhrobot` 目录。手动安装时，还需要在 AstrBot 使用的 Python 环境中安装 `requirements.txt` 中的依赖，然后重载插件。

仓库地址：

```text
https://github.com/yun474/astrbot_plugin_xhhrobot
```

### 2. 配置回复模型和范围

建议先完成以下配置：

| 配置项 | 建议 |
| --- | --- |
| `ai.provider_id` | 选择负责小黑盒回复的模型提供商。留空时使用 AstrBot 当前默认提供商。 |
| `ai.persona_id` | 选择现有 AstrBot 人设。留空时可使用 `ai.session_umo` 对应会话的默认人设。 |
| `event_bridge.enabled` | 建议保持开启，让评论和私信进入 AstrBot 标准消息链。 |
| `filters.allowed_user_ids` | 初次使用时只填写测试用户的小黑盒 ID。 |
| `filters.allow_all_users` | 确认回复效果和频率后再考虑开启。 |
| `filters.reply_to_own_post_comments` | 控制是否回复机器人自己帖子下没有明确提及机器人的普通评论，默认开启。 |
| `filters.reply_to_comment_replies` | 控制是否回复别人对机器人已有评论的直接回复，默认开启。 |
| `direct_messages.enabled` | 控制私信自动回复，默认关闭。 |
| `auto_browse.enabled` | 控制自主巡帖，默认关闭。 |
| `tools.enable_write_tools` | 控制 LLM 写工具，默认关闭。 |

配置页已按账号、人设、标准事件、回复范围、私信、巡帖、工具、图片、统计、WebUI、通知、稳定性和连接分组。完整字段说明以插件配置页为准。

### 3. 扫码登录

打开 AstrBot 插件详情中的“小黑盒bot”页面，在“扫码登录”标签生成二维码。也可以由 AstrBot 管理员发送：

WebUI 会在页面内绘制二维码，不需要浏览器直接加载小黑盒图片地址。刷新页面时，尚未过期的扫码会话仍会重新显示。

```text
/小黑盒登录
```

使用手机小黑盒 App 扫码确认，然后检查状态：

```text
/小黑盒状态
```

状态中会显示当前账号、登录来源、后台任务、自动回复范围、队列、代理和 LLM 工具状态。插件页面还会显示评论与私信统计。

> [!IMPORTANT]
> 推荐将 `account.cookie` 留空并使用扫码登录。Cookie 等同账号凭据，不要放入群聊、日志、问题反馈或公开仓库。

需要更换登录态时，可在插件扫码登录页点击“清除登录”，或由管理员执行 `/小黑盒退出`。两种方式都只清除登录凭据，不会删除消息数据库。

### 4. 小范围验证

初次使用时建议保持 `filters.allow_all_users=false`，只在 `filters.allowed_user_ids` 中加入测试用户。可以先使用以下命令检查人设生成效果：

```text
/小黑盒测试 帖子ID 测试消息
```

自主巡帖应先使用只生成、不发布的预览：

```text
/小黑盒逛帖 预览
```

## 人设与消息链

评论和私信默认通过标准事件桥进入 AstrBot：

- 评论使用独立的帖子会话。
- 私信按对方用户建立独立会话。
- AstrBot 当前人设、会话历史和兼容的 LLM 请求钩子会继续参与生成。
- 小黑盒专用规则可以放在 `ai.extra_system_prompt`，不需要复制整份人设。

标准事件也会经过已启用的 LLM 请求钩子，因此可以兼容世界书等扩展。扩展规则是否生效，取决于相应插件自己的平台、会话和触发条件。

来自小黑盒的帖子、评论、用户资料和私信都被视为不可信外部内容。小黑盒用户 ID 会使用 `xhh:` 命名空间，不能冒充 AstrBot 管理员；外部消息默认也不能调用本插件的账号工具。只有明确开启高风险配置 `event_bridge.allow_llm_tools` 后，才会向这些事件开放工具。

### 评论回复范围

| 场景 | 默认行为 |
| --- | --- |
| 用户在任意帖子中明确提及机器人 | 用户通过允许范围检查后回复。 |
| 用户在机器人自己发布的帖子下普通评论 | `filters.reply_to_own_post_comments=true` 且用户通过范围检查后回复，无需明确提及机器人。 |
| 用户直接回复机器人已有评论 | `filters.reply_to_comment_replies=true` 且用户通过范围检查后回复，包括其他账号的帖子。 |
| 用户在其他账号的帖子下普通评论 | 不回复。 |
| 机器人自己的评论 | 始终忽略，避免自我回复循环。 |

`filters.allowed_user_ids`、`filters.allow_all_users` 和 `filters.blocked_user_ids` 同时作用于评论与私信自动回复。默认允许列表为空，因此新安装的插件不会回复任何用户。

插件会再次读取帖子详情并核验作者，避免把其他账号帖子下的普通评论误认为机器人自己的帖子。相同评论即使同时出现在提及通知和帖子评论通知中，也会按帖子与评论 ID 去重；事件创建、实际外发和重复 `send()` 还分别带有保护，避免同一评论被回复多次。

消息中心的评论消息类型 2 会单独识别为“评论回复”。因此别人回复机器人在其他用户帖子下发表的评论时，也可以进入回复或人工审核流程，不会被“原帖必须属于机器人账号”的普通评论检查误伤。

## 人工审核

开启 `manual_review.enabled` 后，可以通过 `manual_review.workflow` 选择审核时机：

| 选项 | 行为 |
| --- | --- |
| `generate_then_review`（先生成回复，再人工审核） | AstrBot 先按当前人设和消息链生成草稿。管理员可以编辑草稿，再批准发送或拒绝。 |
| `review_then_generate`（先人工审核消息，再生成回复） | 原消息先进入审核队列，不调用模型。管理员批准后才提交给 AstrBot 生成，并按正常流程自动发送。 |

插件 WebUI 的“人工审核”页面可以：

- 查看对方消息、来源、用户、帖子与评论 ID，以及随消息提供的图片。
- 在先生成后审核模式中，直接修改模型生成的文本，然后批准并发送；模型生成的回复图片会保持原顺序。
- 在先审核后生成模式中，批准原消息进入生成队列，或在调用模型前直接拒绝。
- 自动巡帖生成的评论草稿也可在同一页面编辑、批准发布或拒绝。
- 拒绝不需要发送的回复并记录原因。
- 按待审核、已发送、已拒绝、失败或结果不确定等状态查询历史。

各消息类型可以分别控制：

| 配置 | 含义 |
| --- | --- |
| `manual_review.workflow` | 选择“先生成后审核”或“先审核后生成”。 |
| `manual_review.review_mentions` | 审核明确 @ 机器人触发的回复。 |
| `manual_review.review_own_post_comments` | 审核机器人自己帖子下普通评论触发的回复。 |
| `manual_review.review_comment_replies` | 审核别人对机器人已有评论的回复。 |
| `manual_review.review_direct_messages` | 审核好友私信回复。 |
| `manual_review.review_stranger_direct_messages` | 审核陌生人私信回复。 |
| `manual_review.review_auto_browse_comments` | 审核自动巡帖生成的评论草稿。 |
| `manual_review.dm_auto_approve_user_ids` | 这些用户的私信回复跳过审核并自动发送。 |

私信免审列表只改变“是否人工审核”，不会绕过 `filters.allowed_user_ids`、`filters.allow_all_users` 或 `filters.blocked_user_ids`。屏蔽名单始终优先。评论与私信入站审核依赖 `event_bridge.enabled=true`；自动巡帖审核不依赖标准事件桥。

批准操作会在真正外发前重新检查相应的用户范围、额度、冷却、静默时段和内容规则。自动巡帖草稿还会重新读取帖子，并检查 24 小时额度、作者屏蔽与冷却、关键词和评论长度。多人同时操作或重复点击只允许一个请求取得处理权。先审核模式的批准状态会持久化；插件重启或安全重试时不会要求再次审核。发送结果无法确认时不会自动重试。关闭 `webui.show_message_content` 后，审核页隐藏正文并禁止编辑，但仍可批准数据库内保存的原始草稿，或批准原消息进入生成队列。

## 图片上下文

评论视觉上下文按以下顺序加入 AstrBot 消息链：

1. 当前评论图片
2. 被回复评论图片
3. 原帖图片

重复 URL 会自动去重，`ai.max_context_images` 默认最多传入 8 张图片。纯文本模型只能收到图片数量和来源说明，只有支持视觉输入的模型才能识别图片内容。

机器人回复可包含文本和图片。网络图片会先由小黑盒转存，本地图片和 Base64 图片会经过格式、大小与路径检查后上传。

## 私信自动回复

私信自动回复默认关闭。开启 `direct_messages.enabled` 后，插件会轮询好友私信；开启 `direct_messages.reply_to_strangers` 后还会处理陌生人入口。

首次启用默认只建立当前消息基线，不回复已经存在的历史私信。私信配置页各项含义如下：

| 配置项中文名 | 配置键 | 说明 |
| --- | --- | --- |
| 启用私信自动回复 | `direct_messages.enabled` | 自动读取并回复新的好友私信；需要同时启用标准事件。 |
| 回复陌生人私信 | `direct_messages.reply_to_strangers` | 额外轮询陌生人入口，默认关闭。 |
| 私信轮询最短间隔（秒） | `direct_messages.poll_interval_min_sec` | 与最长间隔组成随机轮询区间，默认 90 秒。 |
| 私信轮询最长间隔（秒） | `direct_messages.poll_interval_max_sec` | 随机轮询区间上限，默认 180 秒。 |
| 单次会话列表上限 | `direct_messages.conversation_limit` | 每轮最多检查的最近私信会话数。 |
| 单会话消息上限 | `direct_messages.history_limit` | 会话更新后最多读取的最近消息数。 |
| 单轮最多提交私信数 | `direct_messages.max_dispatch_per_cycle` | 每轮最多交给 AstrBot 生成人设回复的私信数。 |
| 首次处理历史私信 | `direct_messages.process_existing_on_first_start` | 开启后会处理首次启动时可见的历史私信，通常保持关闭。 |
| 私信静默时段 | `direct_messages.quiet_hours` | 例如 `00:30-07:30`；使用云服务器本地时区。 |
| 24 小时私信回复上限 | `direct_messages.max_replies_per_24h` | 限制滚动 24 小时内全部自动私信回复。 |
| 单用户 24 小时上限 | `direct_messages.max_replies_per_user_24h` | 限制同一用户在滚动 24 小时内收到的自动回复。 |
| 单用户回复冷却（秒） | `direct_messages.user_cooldown_sec` | 限制同一用户连续收到自动回复的频率。 |
| 私信消息链间隔（秒） | `direct_messages.send_cooldown_sec` | 文本和多张图片拆成多条消息时的发送间隔。 |
| 私信被拒后暂停（秒） | `direct_messages.restriction_pause_sec` | 平台明确拒绝发送时临时暂停后续私信，默认 1800 秒；`0` 表示只跳过当前消息。 |
| 私信真实 API URL（可选） | `direct_messages.api_params_url` | 默认留空；严格校验时可复用浏览器真实请求中的白名单客户端参数。 |
| 私信回复后通知 | `direct_messages.notify_on_reply` | 成功回复后向通知会话发送双方内容、图片数量和消息 ID。 |

私信发送使用独立的网页客户端参数、当前网页签名规则和 UTF-8 表单编码，不会沿用评论接口地址。扫码登录只保存平台实际返回的必要登录 Cookie，登录完成后会切换到干净的日常请求会话，避免二维码页面 Cookie 混入私信。文本与多张图片会按小黑盒消息链顺序发送。如果发送过程中网络中断，插件会将结果标记为“发送结果不确定”，不会自动重发整条消息。

若小黑盒返回“禁止发送消息行为”等明确限制，插件会跳过当前消息，并按“私信被拒后暂停（秒）”临时暂停后续外发；暂停到期后自动恢复处理新消息。收信、SQLite 归档和 WebUI 查询不会暂停。该错误只表示当前网页 API 请求被拒绝，不一定代表手机 App 也无法私信。

“私信真实 API URL（可选）”通常不需要填写。默认请求仍被拒绝、但同一账号在网页版工作正常时，可以在浏览器开发者工具的 Network 中复制一条 `https://api.xiaoheihe.cn/...` 完整请求 URL。插件只读取 `app`、`version`、`web_version`、`device_id` 等客户端参数，忽略其中的签名、目标用户、Cookie 和其他字段；Cookie 不要粘贴到该配置项。修改后需要重载插件。

## 自主巡帖

自主巡帖默认关闭。开启 `auto_browse.enabled` 后，机器人会定时读取推荐流，从摘要中选择候选帖子，再读取完整正文并决定评论或跳过。

帖子内容被视为不可信输入，不能借此要求模型修改规则、调用工具或泄露提示词。建议先执行 `/小黑盒逛帖 预览` 检查选帖和评论效果。

开启 `manual_review.enabled` 和 `manual_review.review_auto_browse_comments` 后，定时巡帖生成的评论不会自动发布，而会进入 WebUI 审核队列。此时即使 `auto_browse.dry_run=true`，定时生成的预览也会成为可人工批准的持久草稿；只有管理员批准后才可能发布。手动执行 `/小黑盒逛帖 预览` 始终只是一次性预览，不进入审核队列，也不能从该命令直接发布。

默认保护如下：

| 保护项 | 默认值 |
| --- | --- |
| 巡帖间隔 | 180 分钟，随机浮动 30 分钟 |
| 启动等待 | 10 分钟 |
| 单轮评论上限 | 1 条 |
| 滚动 24 小时上限 | 3 条 |
| 同作者冷却 | 72 小时 |
| 同帖子去重 | 30 天 |
| 内容保护 | 关键词、长度、网址、提及、重复评论和提示注入检查 |

自主巡帖是一条独立授权的后台写入路径，不受 `tools.enable_write_tools` 控制。未开启自动巡帖审核时，它不会逐条要求聊天确认；开启后，每条准备发布的评论都必须在 WebUI 批准。发送结果不确定时会停止重试并占用额度，避免重复评论。

## 自然语言与 LLM 工具

启用工具后，用户不需要记忆工具名，可以直接用自然语言表达需求：

```text
搜索最近讨论 AstrBot 的帖子，列出标题、作者和帖子 ID。
查看帖子 123456 的正文、图片和热门评论。
查一下用户 98765 最近发布了什么。
看看谁回复了当前账号，以及最近收藏了哪些帖子。
统计评论数据库里包含“AstrBot”的内容，排除机器人自己的回复。
先生成一篇图文帖草稿供确认，不要发布。
```

模型会根据请求选择工具。读取工具默认注册；写工具和本地草稿箱需要单独开启。

### 社区公开读取

| 工具 | 能力 |
| --- | --- |
| `xhh_get_feed` | 获取社区推荐动态。 |
| `xhh_search` | 搜索帖子、用户、游戏、标签和商城内容。 |
| `xhh_get_post` | 获取帖子正文、图片和评论。 |
| `xhh_get_sub_comments` | 分页读取子评论。 |
| `xhh_get_user_profile` | 获取用户公开资料。 |
| `xhh_get_user_activity` | 获取用户发布的帖子、评论和动态。 |
| `xhh_get_user_relations` | 获取用户的粉丝和关注列表。 |
| `xhh_get_topics` | 获取或搜索发帖话题。 |
| `xhh_get_emojis` | 获取小黑盒表情列表。 |

### 当前账号与私密读取

这些工具直接读取已登录账号的数据，默认仅 AstrBot 管理员可以调用。

| 工具 | 能力 |
| --- | --- |
| `xhh_status` | 查看当前登录账号、后台队列和工具状态，并返回账号 ID 与昵称。 |
| `xhh_get_mentions` | 读取当前账号收到的提及消息。 |
| `xhh_get_notifications` | 统一读取提及、自己帖子下的评论和回复。 |
| `xhh_get_favorite_folders` | 获取当前账号的收藏夹目录。 |
| `xhh_get_my_favorites` | 读取当前账号收藏的帖子，无需手动提供账号或收藏夹 ID。 |
| `xhh_get_remote_drafts` | 读取小黑盒服务端保存的发帖草稿。 |
| `xhh_get_direct_messages` | 获取最近私信会话或指定用户的私信历史。 |
| `xhh_comment_stats` | 统计收到和发出的评论、原始观察、去重结果、用户、帖子及处理状态。 |
| `xhh_search_comment_archive` | 按关键词、时间、用户、帖子和状态查询具体评论记录。 |
| `xhh_get_drafts` | 读取插件本地草稿。仅在本地草稿箱开启时注册。 |

查看当前账号自己发布的帖子、评论和动态时，先通过 `xhh_status` 获取 `heybox_id`，再调用 `xhh_get_user_activity`。

小黑盒服务端草稿和插件本地草稿互不相同：前者保存在小黑盒账号中，后者只保存在 AstrBot 服务器。

### 写操作

| 工具 | 能力 |
| --- | --- |
| `xhh_publish_post` | 发布普通图文帖，最多两个话题、五个标签，并支持有序富文本内容块。 |
| `xhh_create_comment` | 评论帖子或回复指定评论。 |
| `xhh_set_favorite` | 收藏或取消收藏。 |
| `xhh_set_like` | 点赞或取消帖子、评论点赞。 |
| `xhh_set_follow` | 关注或取消关注用户。 |
| `xhh_delete_post` | 删除当前账号自己发布的帖子。 |
| `xhh_send_direct_message` | 发送私信文本和多张网络或本地图片。 |
| `xhh_save_draft` | 保存或更新插件本地草稿，不会发布到小黑盒。仅在本地草稿箱开启时注册。 |
| `xhh_delete_draft` | 删除插件本地草稿，不影响小黑盒服务端草稿或已发布帖子。仅在本地草稿箱开启时注册。 |

## 写操作保护

写工具默认关闭。启用 `tools.enable_write_tools` 并重载插件后，模型才能看到发帖、评论、互动、删除和私信工具。

`tools.require_explicit_confirmation` 默认开启，推荐分两轮执行：

```text
用户：按当前人设写一篇介绍 AstrBot 的小黑盒帖子，先返回预览，不要发布。
机器人：返回标题、正文、话题和标签草稿，等待确认。
用户：确认执行小黑盒操作，发布刚才那一版。
机器人：调用发布工具并返回帖子 ID 或错误。
```

也可以在一条消息中同时给出完整内容和确认词：

```text
确认执行小黑盒操作：在帖子 123456 下评论“写得很清楚，谢谢分享”。
```

不需要逐次确认时，可以关闭 `tools.require_explicit_confirmation` 并重载插件。关闭后，用户明确要求执行写操作时模型可以直接调用工具，但管理员权限、允许列表、冷却和重复写入保护仍然生效。

| 配置项 | 默认值 | 作用 |
| --- | --- | --- |
| `tools.enable_write_tools` | `false` | 不注册写工具。 |
| `tools.enable_draft_tools` | `false` | 不注册插件本地草稿工具。 |
| `tools.write_admin_only` | `true` | 仅 AstrBot 管理员可执行写操作。 |
| `tools.private_tools_admin_only` | `true` | 仅 AstrBot 管理员可读取账号私密内容。 |
| `tools.require_explicit_confirmation` | `true` | 要求工具参数和用户当前原始消息同时满足确认条件。 |
| `tools.duplicate_guard_sec` | `120` | 阻止同一消息和相同参数被短期重复执行。 |
| `tools.write_cooldown_sec` | `3` | 限制连续写操作频率。 |

关闭管理员专用后，非管理员仍需命中 `tools.allowed_astrbot_user_ids` 或 `tools.allowed_umos`。列表留空不会放行，只有显式填写 `*` 才表示允许全部。

### 本地草稿箱

`tools.enable_draft_tools` 默认关闭。开启并重载后，才会注册 `xhh_get_drafts`、`xhh_save_draft` 和 `xhh_delete_draft`。

本地草稿保存在插件数据目录的 `post_drafts.sqlite3`，不会上传或同步到小黑盒。读取按账号私密权限处理；保存和删除还要求 `tools.enable_write_tools=true`，并继续受写入权限、确认、冷却和去重保护。

### 富文本与图片

`xhh_publish_post` 和 `xhh_save_draft` 可以通过 `content_blocks` 按顺序组合文本、受限 HTML 和图片：

```json
[
  {"type": "text", "text": "第一段纯文本"},
  {"type": "html", "html": "<p><strong>重点</strong> <a href=\"https://example.com\">链接</a></p>"},
  {"type": "image", "url": "https://example.com/image.jpg"}
]
```

允许的 HTML 包括段落、换行、强调、删除线、列表、引用、代码块和公开 HTTP(S) 链接。脚本、样式、事件属性、私网链接、内嵌图片和未知标签会被拒绝。图片必须使用独立的 `image` 内容块。

发帖、评论和私信支持公开 HTTP(S) 图片、Base64 图片以及允许目录中的本地图片。本地路径只允许 AstrBot 管理员通过写工具使用；不要将 `/`、`C:\\` 或整个用户目录加入 `media.allowed_local_roots`。

## 消息数据库与 WebUI

插件使用自己的 SQLite 文件，不需要 MySQL、PostgreSQL 或其他数据库服务：

| 文件 | 内容 |
| --- | --- |
| `comment_archive.sqlite3` | 收到的评论、平台原始观察、机器人自动回复、自主巡帖评论和工具评论。 |
| `direct_messages.sqlite3` | 私信会话、消息、待处理队列、回复内容和发送状态。 |
| `review_queue.sqlite3` | 评论与私信的待审核草稿、人工编辑、拒绝原因和发送状态。 |
| `post_drafts.sqlite3` | 可选的插件本地发帖草稿。 |

评论统计将收到的外部评论和机器人发出的评论分开计算。同一条外部评论以“帖子 ID + 评论 ID”去重，不同通知入口仍可保留为原始观察，因此可以区分：

- 平台推送了多少条原始观察
- 实际有多少条唯一评论
- 有多少条重复通知
- 机器人确认发出了多少条评论
- 有多少次发送结果无法确认

WebUI 的“消息数据库”页面支持按数据集、关键词、方向、来源、状态、用户 ID 和帖子 ID 分页筛选。关闭 `webui.show_message_content` 后，页面只显示隐藏占位、字符数、ID、时间和状态。

归档只包含插件启用后实际观察到的消息，不会自动抓取小黑盒全部历史。评论和私信首次轮询默认只建立游标或基线。确需处理当前可见历史时，应在首次拉取前分别开启：

- `polling.process_existing_on_first_start`
- `direct_messages.process_existing_on_first_start`

`analytics.retention_days` 默认保留 365 天，设为 `0` 表示永久保留。

## 管理命令

以下命令要求 AstrBot 管理员权限：

| 命令 | 作用 |
| --- | --- |
| `/小黑盒帮助` | 显示命令帮助。 |
| `/小黑盒状态` | 查看账号、轮询、队列、工具和代理状态。 |
| `/小黑盒登录` | 发起二维码扫码登录。 |
| `/小黑盒退出` | 清除扫码登录凭据并停止后台任务。 |
| `/小黑盒启动` | 启动后台轮询。 |
| `/小黑盒停止` | 停止后台轮询。 |
| `/小黑盒检查` | 立即执行一轮检查。 |
| `/小黑盒重试` | 重试明确失败且可安全重试的记录。 |
| `/小黑盒重试 确认` | 同时重试发送结果不确定的记录，存在重复发送风险。 |
| `/小黑盒测试 帖子ID 测试消息` | 使用指定帖子和文本测试人设回复，不发布。 |
| `/小黑盒逛帖 预览` | 立即选帖并生成评论，不发布。 |
| `/小黑盒逛帖` | 自主巡帖开启时立即执行一轮。 |

## 可选 SOCKS5 出口

`connection.proxy_url` 可以让本插件访问小黑盒的流量单独经过 SOCKS5 代理，不会影响 AstrBot 平台消息、LLM 请求或服务器上的其他程序。

适合以下情况：

- AstrBot 服务器到小黑盒的网络线路不稳定
- 希望账号长期使用一个稳定的登录出口
- AstrBot 与常用登录设备位于不同网络区域

推荐通过 Tailscale、WireGuard 或 SSH 反向隧道连接私有 SOCKS5 服务，不要把代理端口直接暴露到公网。代理设备必须保持开机、联网和服务常驻。

配置格式：

```text
connection.proxy_url = socks5://用户名:密码@主机:端口
```

可以在 AstrBot 服务器上先验证出口：

```bash
curl --proxy 'socks5h://用户名:密码@主机:端口' https://api.ipify.org
```

插件使用代理端解析 DNS。用户名或密码包含 `@`、`:`、`/` 等字符时，需要进行 URL 百分号编码。代理不可用时，小黑盒请求会失败，不会自动回退到服务器直连。修改代理配置后需要重载插件。

代理只能改善网络路径或保持出口稳定，不能绕过平台规则，也不能保证账号不受限制。

## 配置分组

| 分组 | 内容 |
| --- | --- |
| `account` | 扫码登录、手动 Cookie、账号 ID 和设备 ID。 |
| `ai` | 提供商、人设、额外规则、帖子上下文、图片和生成限制。 |
| `event_bridge` | 标准事件、并发、超时和外部消息工具隔离。 |
| `filters` | 自动回复范围、自己帖子评论和用户允许或屏蔽列表。 |
| `manual_review` | 审核时机、各类消息的人工审核开关和私信免审用户列表。 |
| `polling` | 提及与普通评论轮询、分页、回复间隔和首次历史策略。 |
| `direct_messages` | 私信入口、轮询、静默时段、额度和冷却。 |
| `auto_browse` | 自主巡帖频率、额度、筛选、作者冷却和内容保护。 |
| `tools` | 工具注册、私密读取权限、写入权限、确认、限速和内容长度。 |
| `media` | 回复图片数量、本地图片大小和允许上传目录。 |
| `analytics` | SQLite 保留时间、容量和查询上限。 |
| `webui` | 插件页面 API、正文显示和单页读取上限。 |
| `notifications` | 主动通知目标和成功回复通知。 |
| `reliability` | HTTP 超时、失败重试、熔断和持久化记录上限。 |
| `connection` | 可选 SOCKS5 代理、接口地址和客户端版本参数。 |

影响工具注册或 schema 的配置需要重载插件，包括 `tools.enabled`、`tools.enable_write_tools`、`tools.enable_draft_tools` 和 `tools.require_explicit_confirmation`。修改登录、代理、归档或 WebUI 配置后也建议重载。

## 数据与隐私

- 扫码凭据、设备 ID、轮询游标、待处理队列、巡帖记录和失败记录使用 AstrBot 插件 KV 存储。
- 评论、私信和本地草稿保存在插件数据目录的 SQLite 文件中。
- Cookie、代理凭据、数据库及其 WAL/SHM 文件不会包含在插件发布包中。
- 浏览器只通过 AstrBot 登录态保护的插件 API 查询数据，不会收到 SQLite 路径、Cookie 或代理配置。
- 停止或卸载插件不会主动删除登录凭据、统计和草稿。
- `/小黑盒退出` 只清除扫码登录凭据，不会删除 SQLite 数据。

彻底清理数据前，应先停止插件，再删除对应 SQLite 主文件和同名的 `-wal`、`-shm` 文件。

## 常见问题

### 已登录，但机器人不回复

先检查 `/小黑盒状态`。新安装时 `filters.allowed_user_ids` 为空且 `filters.allow_all_users=false`，因此默认不会回复任何人。还需要确认后台轮询已启动、用户未被屏蔽，并且评论满足“明确提及机器人”或“机器人自己帖子下普通评论”的规则。

### 现有人设或消息钩子没有生效

确认 `event_bridge.enabled=true`，并检查 `ai.persona_id` 或默认会话人设。第三方扩展如果限定了平台、会话或触发词，还需要允许 `xhhrobot` 平台及小黑盒会话。

### 机器人看不到评论图片

确认图片没有被 `ai.max_context_images=0` 禁用，并选择支持图片输入的模型。纯文本模型只能看到图片来源和数量提示。

### 日志显示小黑盒请求超时

这表示回复已经生成，但插件没有及时取得小黑盒 API 回执。写请求超时会标记为“发送结果不确定”，默认不会自动重试，以免重复发帖或评论。应先在小黑盒中确认内容是否出现，再决定是否使用 `/小黑盒重试 确认`。

可以检查服务器网络、`connection.proxy_url` 和代理存活状态，并适当提高 `reliability.request_timeout_sec`。提高超时只能容忍慢连接，不能修复失效代理或不可达线路。

### 私信提示“禁止发送消息行为”

这表示当前网页 API 私信请求被小黑盒拒绝，不等于账号在手机 App 中也被禁言。插件默认跳过当前消息并暂停外发 1800 秒，暂停结束后自动恢复，收信和归档照常运行。可通过“私信被拒后暂停（秒）”调整时间，设为 `0` 时不会阻塞后续消息。

从旧版本升级后，请先按以下顺序建立新的干净登录态：

1. 在配置页确认“手动 Cookie（可选）”为空。
2. 执行 `/小黑盒退出` 清除旧扫码凭据。该操作不会删除 SQLite 消息数据库。
3. 执行 `/小黑盒登录`，使用手机 App 重新扫码确认。
4. 等待一条新私信触发回复，并检查 AstrBot 后台日志。

插件会在首次拒绝日志后输出一条 `direct-message restriction diagnostics` 脱敏诊断，其中只有客户端参数、设备 ID 哈希、Cookie 名称和代理是否启用，不含凭据原值。若新的扫码登录仍被拒绝、而同一网络出口上的其他客户端确实可以发送，可以填写“私信真实 API URL（可选）”复用非敏感客户端参数。该选项不能绕过账号或接口限制，也不能保证平台一定接受请求。

### 仍然出现重复回复

插件会按帖子和评论 ID 去重，并阻止同一事件重复发送。如果仍然重复，检查 AstrBot 中是否同时启用了旧版本、重复插件目录或另一个小黑盒自动回复插件。独立插件实例无法共享去重状态。

## 风险说明

小黑盒接口字段、签名、发布规则和风控策略都可能变化。建议：

- 从测试账号和用户允许列表开始
- 保持默认回复间隔与写入冷却
- 自主巡帖先预览，再以低频率启用
- 不公开 Cookie、设备 ID 或代理凭据
- 关注 `/小黑盒状态`、WebUI 处理状态和 AstrBot 日志

账号受限、Cookie 失效或接口变化时，插件会返回错误或暂停相关自动功能，不会让 AstrBot 主进程退出。写请求发出后如网络中断，结果会标记为不确定，并停止自动重发。

## 更新记录

版本变化和修复内容见 [changelog.md](./changelog.md)。

## 开发与许可证

运行测试：

```powershell
python -m unittest discover -s astrbot_plugin_xhhrobot/tests -t . -v
```

项目许可证见 [LICENSE](./LICENSE)，第三方许可声明见 [THIRD_PARTY_NOTICES.md](./THIRD_PARTY_NOTICES.md)。

## 致谢

- 感谢原作者 [Whereis-Alice](https://github.com/Whereis-Alice) 创建并开源 [astrbot_plugin_xhhrobot](https://github.com/Whereis-Alice/astrbot_plugin_xhhrobot)，本仓库在其工作基础上继续开发。
- [AstrBot](https://github.com/AstrBotDevs/AstrBot) 提供插件、标准消息事件、LLM 工具与 WebUI 基础能力。
- [SomeOvO/xhhRobot](https://github.com/SomeOvO/xhhRobot) 提供小黑盒社区接口与机器人流程的早期协议参考。
- [advent259141/astrbot_plugin_xiaoheihe_adapter](https://github.com/advent259141/astrbot_plugin_xiaoheihe_adapter) 提供当前网页请求、私信、图片上传和 AstrBot 适配思路的实现参考。

也感谢所有参与测试、反馈问题和完善使用说明的用户。
