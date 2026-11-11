# 架构与调度

Deamo Wake 把持续情境、设备观测和定时安排整理成一份下一次唤醒报告，再交给模型运行函数。报告带上相关 scene、短期字条和设备状态；聊天程序仍沿用自己的 persona、对话历史、记忆库和工具执行方式。

## 一次唤醒怎样发生

1. 聊天程序收到真实用户消息后，调用 `user_message(conversation_id, message_id, text, at=None)`。可注入的 SLM 将消息转换成 scene 增量；内核保存用户消息元数据并更新 scene。
2. 聊天程序按授权提供 phone、presence、location 或 health 事件。`known_places`、`known_apps` 和 `scene_apps` 决定哪些数据可用于条件匹配和设备触发。
3. `tick()` 按优先级检查 due 工作，领取后重新核验 scene 状态，再选择最新真实用户会话作为目标。
4. runner 接收包含会话标识和唤醒报告的请求。模型运行函数在这里加载已有的设定、对话历史、长期记忆和工具。
5. runner 通过 `on_accepted(native_thread_id)` 确认对应会话已接收这一轮；运行完成和显式通知分别记录。成功完成后，内核结算该报告引用的字条。

聊天程序也可在正常助手回复后调用 `assistant_activity(at=None)`，更新巡检静默门槛中的助手活动时间。真实用户消息的活跃时间会参与全局静默判断；唤醒目标选择仍只从合格会话中挑选。

## Scene 类型和默认节奏

`cadence` 是默认检查间隔，`lifetime` 是默认有效期；工具可在 schema 允许范围内覆盖。优先级用于报告中多个 scene 的呈现顺序。

| Scene | 默认 cadence | 默认 lifetime | priority |
|---|---:|---:|---:|
| `outing` | 30 分钟 | 8 小时 | 50 |
| `work` | 60 分钟 | 10 小时 | 40 |
| `bedtime` | 45 分钟 | 10 小时 | 55 |
| `morning` | 20 分钟 | 3 小时 | 60 |
| `active_without_chat` | 30 分钟 | 4 小时 | 45 |
| `silence_escalation` | 15 分钟 | 4 小时 | 65 |
| `repeat` | 10 分钟 | 3 小时 | 70 |
| `private_play` | 10 分钟 | 1 小时 | 100 |
| `sulking` | 20 分钟 | 8 小时 | 75 |
| `custom` | 45 分钟 | 6 小时 | 35 |

每条真实用户消息最多应用 3 个增量。增量可以开始、更新或结束 scene；未来计划不会提前开始当前 scene。消息 ID 去重，重复消息不会创建第二次分类处理。

## 唤醒路线与优先级

普通 scheduler tick 需要 `collar_wake.enabled` 和 `wake_live` 都为真。执行顺序如下：

| 顺序 | 入口 | 主要条件 |
|---:|---|---|
| 1 | 闹钟 | 到时后先领取并交给 runner。 |
| 2 | Scene | scene 自己保存下次时间；系统会检查睡眠状态、条件命中、snooze 和提交前状态。已确认睡眠的 bedtime scene 有一次特殊领取，并带租约和递增重试。 |
| 3 | 巡检 | 默认每 15 分钟检查一次。活动时段默认 UTC+08:00 的 09:00–24:00；另需通过最近唤醒/助手活动 10 分钟去重、用户 15 分钟静默、睡眠、snooze 和每日 50 次额度门槛。已有 active scene 时不跑巡检分类。 |
| 独立红线 | 健康样本 | 新鲜连续样本满足规则时直接按代码阈值发起，不经过巡检 SLM；需要通过冷却和每日上限。红线会等待正在运行的普通唤醒完成。 |

Scene API 尝试预算默认关闭。显式启用 `scene_api_budget_enabled` 后，每次尝试计数，包括明确失败；按 UTC+08:00 自然日重置。bedtime 睡眠确认的特殊领取免于这项预算。

上述锁属于单个 `WakeEngine` 和 `SQLiteStore` 实例；同一数据库使用一个 engine/store 实例。周期 tick 是 best-effort，长时间运行的 callback 会延迟后续调度。

## `wake_when` 条件

每个 scene 最多配置一条条件，可与 scene timer 同时存在。SLM 在设置时将自然语言翻译并校验为以下固定规则；事件匹配由内核本地完成。

| 规则 | 含义 | 依赖输入 |
|---|---|---|
| `arrive` | 到达已配置地点 | location event、`known_places` |
| `leave` | 离开已配置地点 | location event、`known_places` |
| `app_open` | 打开一个已配置 App | phone open event、`known_apps` |
| `app_use` | 连续使用一个 App 达到时长 | phone 当前 App 和开始时间 |
| `device_active` | 设备安静一段时间后恢复活动 | 前序活动轨迹及 phone/presence event |
| `all_quiet` | 所有已观测设备达到静默时长 | phone、presence 中的 Mac 状态 |

条件允许的设备值为 `iphone`、`ipad`、`macbook`、`any`；App 条件使用 iPhone/iPad 的 App 使用观测。地点和 App 必须来自聊天程序配置的地点和 App 列表。时长为 5–720 整数分钟。可选时间窗格式为 `HH:MM-HH:MM`，按固定 UTC+08:00 计算并可跨午夜。每个条件仅描述一种事件，不接受任意逻辑表达式。

设置时间之前的事件不触发条件。普通 timer 到时会继续保留尚未命中的条件；条件命中后仍须经过 scene 再核验，且在唤醒成功完成时才清除。拒绝或失败会保留条件并安排重试。

## 状态、报告与回执

SQLite 保存 wake 设置、会话路由元数据、scene 状态和 wake receipt。会话表与消息表只保存 ID、时间、资格等调度数据；消息正文由聊天程序保管。聊天程序可以让 runner 在拿到 `conversation_id` / `native_thread_id` 后访问已有数据库或记忆服务，并将需要的信息加入正常模型调用上下文。

报告是纯展示层，返回报告正文和其中实际显示的短期字条引用。完整报告按第 1、6、11……次轮换；精简报告继续保留必要通知说明。字条最多成功读取三次；完成回执只消费报告中引用且版本仍匹配的字条。

| 字段 | 事实含义 |
|---|---|
| `accepted` | 模型运行器确认对应会话已接收本轮；由 `on_accepted` 记录。 |
| `completed` | runner 已完成该轮，且没有运行错误。 |
| `notified` | 通知回调确认消息已送达。 |

三项可分别发生。runner 报告 `accepted: null` 表示接收情况未知，调用结果保持这一状态。恢复时应结合实际执行和发送回执处理；各类调度有自己的重试规则。普通 runner 文本与显式通知分别处理。

## 时间默认值

所有持久化时间戳按 UTC 保存。活动时段、巡检自然日计数、睡眠时段、红线自然日和 `wake_when.between` 均固定使用 UTC+08:00；机器本地时区不参与解释。
