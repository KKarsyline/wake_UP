# 接入指南

这份指南面向把 Deamo Wake 接入现有聊天程序的开发者。Deamo Wake 保存主动唤醒相关状态并选择何时回来；聊天程序持有正常聊天链路、模型 persona、历史/记忆、数据库、会话身份、设备采集和通知通道。

## 1. 创建 engine 并连接正常聊天

```python
from deamo_wake.engine import WakeEngine
from deamo_wake.storage import SQLiteStore

store = SQLiteStore("wake-state.sqlite3")
engine = WakeEngine(
    store,
    slm=host_slm,
    runner=host_runner,
    notifier=host_notifier,
    known_places={"home": {"label": "家", "role": "home"}},
    known_apps=["chat-app"],
)
await engine.initialize()
await engine.configure(enabled=True, wake_live=True)
```

`WakeEngine` 在构造时接收 `store`、可选 `slm`、必需 `runner`、可选 `notifier` 和地点/App 配置。初始化后，默认唤醒开关为关闭；上例中 `enabled` 打开 scheduler，`wake_live` 允许向 runner 派发请求。调度工具不会暗中修改开关。应用关闭时调用 `await engine.close()`。

每个 SQLite 数据库配一个 `WakeEngine` 实例。engine 的唤醒锁和 store 的状态锁都按实例创建；多个实例共用同一数据库时不会互相协调。

### 连接现有会话和记忆库

开始处理一条正常聊天消息前，先提交用户消息：

```python
await engine.user_message(conversation_id, message_id, text, at=received_at)
```

`conversation_id` 使用聊天程序稳定的会话 ID；`message_id` 是真实用户消息的去重 ID。`native_thread_id` 是聊天程序运行时使用的稳定会话标识，Deamo Wake 将它作为 opaque string 保存，不要求指定模型供应商或特定 session API。如果聊天程序自行组装主模型请求，可用这个标识映射到已有对话上下文记录，再按正常聊天的方式拼接历史和构造消息。助手正常回复完成后，再调用：

```python
await engine.assistant_activity(at=reply_finished_at)
```

这只更新助手活动时间，不会生成用户消息，也不会改变唤醒目标。所有真实用户消息都参与全局活动/静默判断；唤醒目标仍按是否为聊天会话、是否启用/可见/未归档和是否已有用户消息筛选。

`SQLiteStore` 保存 Deamo Wake 自己的调度状态：scene、wake 设置、conversation/thread 映射和回执。聊天程序已有数据库或记忆服务由 runner 按 `conversation_id`、`native_thread_id` 查询，再按聊天程序现有方式组装模型上下文；情境短期字条通过 `collar_companion` 的 `set_note` / `clear_note` 写入或清除，长期记忆仍可由聊天程序自己的服务管理。

首次 wake 发生时 `native_thread_id` 可以为空。runner 创建或选定原生线程后，必须在聊天程序确认该轮已进入该线程时调用 `on_accepted(native_thread_id)`，Deamo Wake 会保存映射。之后的 wake 会带上已有会话和原生线程标识。

## 2. 注入 SLM、runner 和 notifier

### SLM / classifier

SLM 是一个 async callable，以 keyword arguments 接收 prompt，并返回已经解析好的 `dict`：

```python
async def host_slm(*, system, user_content, max_tokens, temperature) -> dict:
    response = await model_client.generate(
        system=system,
        user=user_content,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return parse_json_object(response)
```

同一个 `slm` callable 用于三类任务：真实用户消息 scene 分类、`wake_when` 翻译和周期巡检判词。每次调用都会收到相同的 keyword 签名，聊天程序只需负责调用模型服务并将 JSON 文本解析成字典；具体输出形状按当前任务区分：

| 用途 | 返回字典形状 |
|---|---|
| Scene 分类 | `{"deltas":[{"action":"start|update|end","scene_type":"...","facts":[],"cadence_min":null,"duration_min":null}],"reason":"..."}` |
| 条件翻译 | 成功为 `{"ok":true,"kind":"arrive|leave|app_open|app_use|device_active|all_quiet","place":null,"app":null,"device":"any","minutes":null,"between":null}`；拒绝为 `{"ok":false,"reason":"..."}` |
| 巡检判词 | `{"wake_master":true,"reason":"...","urgency":"low|normal|high"}` |

prompt、结构校验、超时和本地状态更新由库处理。分类失败会返回空 delta，正常用户消息仍然入库；巡检判词只用于巡检路线。

### Runner / 聊天程序主模型

runner 签名：

```python
async def host_runner(request, on_accepted, tools, interrupted) -> dict:
    ...
```

`request` 的字段固定为 `wake_id`、`trigger`、`conversation_id`、`native_thread_id`、`report`、`full_report`。scene、alarm、巡检和红线的背景已经整理进 `report`；聊天程序可将报告与既有 persona、会话历史、记忆查询结果及聊天程序工具一起交给正常聊天模型。

runner 应遵循这个顺序：

1. 确认请求可进入聊天程序原生会话；一经接收，调用并等待 `await on_accepted(native_thread_id)`。
2. 在模型回合中通过 `await tools(name, arguments)` 调度 Deamo Wake 的工具调用。
3. 返回包含 `accepted`、`completed`、`substantive`、`text`、`error` 的结果字典。成功完成需满足 `completed=True` 且没有 `error`；engine根据显式通知工具的实际送达回执单独填写 `notified`。

`accepted`、`completed` 和 `notified` 是三个独立事实。`on_accepted` 会写入接受回执；runner 在接受事实未知时返回 `accepted=None`，调用结果会保持这个未知值；发生在 `on_accepted` 之后的异常仍保留已接收事实。接入方应结合实际执行和发送回执处理恢复，各类调度按自己的重试规则继续。

用户在 wake 执行中发来新消息时，engine 会设置 `interrupted` 并协调该轮清理后再处理正常对话。聊天程序 runner 需要观察这个 `asyncio.Event`，将它映射为自身 runtime 的中断/取消流程；聊天程序正常聊天仍由聊天程序自己的链路产生。

### Notifier / 显式通知

notifier 签名：`await notifier(conversation_id, message)`。成功必须返回 `{"delivered": true}`。wake 中主模型确实选择联系用户时，由 runner 使用 `tools("notify_user", {"message": ...})` 触发 notifier；通知工具的这次调用随当前 accepted wake 记入 receipt。runner 结果中的 `text` 由聊天程序按正常聊天链路接收和保存，显式通知另由 notifier 发送。

## 3. 聊天程序工具

`engine.tool_schemas` 暴露 `collar_companion` 和 `notify_user` 两个 schema。正常聊天中的情境和调度工具调用由聊天程序通过 `engine.tool(name, arguments, conversation_id)` 分发。wake 回合中的工具调用统一交给 runner 收到的 `tools(name, arguments)` callback；它绑定当前 wake receipt，`notify_user` 也由此关联 accepted 回执并确认通知结果。

工具名称和动作如下：

| 工具 | 动作 | 常用字段 |
|---|---|---|
| `collar_companion` | `status`、`start_scene`、`update_scene`、`end_scene`、`set_repeat`、`set_rhythm`、`set_alarm`、`clear_alarm`、`snooze`、`goodnight`、`set_interval`、`set_note`、`clear_note` | `scene_type`、`scene_id`、`title`、`note`、`facts`、`nudge`、`cadence_min`、`duration_min`、`wake_when`、`recreate_after_minutes`、`at`、`minutes`、`alarm_id` |
| `notify_user` | 显式联系用户 | `message` |

schema 定义字段类型、必填项和上下界。scene 节奏使用 `cadence_min`；set_interval 使用 `minutes`（5–240），snooze 时长为 5–720 分钟；闹钟使用未来 ISO 时间 `at`。完整机器可读定义以 `engine.tool_schemas` 为准。

短期字条绑定一个有效情境。`set_note` 需要 `scene_id` 和 `note`，`clear_note` 需要 `scene_id`；创建或更新情境时也可通过 `nudge` 留下字条。成功且有实质内容的相关唤醒按报告中的字条版本消费一次，共三次。

`wake_when` 可和 timer 一起设置。例如，给工作 scene 设置半小时检查间隔，并等待 App 打开：

```python
await engine.tool("collar_companion", {
    "action": "start_scene",
    "scene_type": "work",
    "title": "工作",
    "cadence_min": 30,
    "duration_min": 360,
    "wake_when": "chat-app 在手机上打开时提醒我",
}, conversation_id=conversation_id)
```

当事件匹配且 runner 成功完成 wake，条件自动清除，原 timer 继续控制后续检查。若尚未命中时用户主动取消，调用 `update_scene` 并传 `wake_when: ""`；换条件时传入新的自然语言条件，新规则从本次设置时间开始生效。带完整 SLM、allowlist、事件和 runner 的可运行版本见 [`examples/scene_condition.py`](../examples/scene_condition.py)。

## 4. 设备与状态事件

所有事件通过 `await engine.observe(event)` 提交。时间戳建议使用 UTC ISO 格式；来源提供稳定事件 ID 时一并传入。事件重复会被识别，旧的 phone/presence 状态不会覆盖更新的状态。

### Phone

打开或关闭 App 的事件字段为 `kind`、`body.event`、`body.device`、`body.app`、`body.at`、`body.event_id`；`body.source` 可标记采集来源。

```json
{"kind":"phone","body":{"event":"open","device":"iphone","app":"chat-app","at":"2026-11-11T12:00:00+00:00","event_id":"phone-101","source":"host"}}
```

事件值应表达实际 open/close 状态，例如 `open` 或 `close`。当前 App 的开始时间由 engine 据此维护，后续会计算使用时长。

### Presence

presence 的 `semantic` 位于 envelope，与 `body` 分开。body 用于设备和来源身份、事件时间及快照字段。

```json
{"kind":"presence","body":{"device":"iphone","source":"watch","at":"2026-11-11T12:01:00+00:00","event_id":"presence-101"},"semantic":{"active":true,"effective_state":"active","app":"chat-app"}}
```

macOS 快照可通过 body 带入 `online`、`screen_locked`、`idle_seconds` 等字段。缺少设备字段表示未知；有状态时再按它进行条件和睡眠判断。

### Location

地点事件放在 envelope 的 `location_events` 列表。每条记录使用 `event` (`arrive` / `leave`)、`detected_at` 或 `at`，以及 `{name, label}` 形式的 `place`。`known_places` 以 `name` 为键，内容为 `{label, role}`，role 可用 `home` 或 `work`。

```json
{"kind":"location","body":{"source":"host","event_id":"location-101"},"location_events":[{"event":"arrive","detected_at":"2026-11-11T12:02:00+00:00","place":{"name":"home","label":"家"}}]}
```

首个 location observation 只建立事件水位，以该 batch 最后一条记录作为当前水位。后续 batch 按事件时间顺序由聊天程序传入，并包含上一次已知水位记录及其后的新事件；内核从水位之后继续处理，按收到顺序消费，不替聊天程序重排。若只发送水位之后的一条新事件，内核无法在批次中定位旧水位，因此聊天程序应发送“上次水位 + 新事件”的连续片段。

### Health

健康样本放在 envelope 的 `samples` 列表，包含 `type`、数值 `value`、`unit`、`at`；`source` 和结构化 `extra` 可选。

```json
{"kind":"health","samples":[{"type":"heart_rate","value":72,"unit":"bpm","at":"2026-11-11T12:03:00+00:00","source":"watch"}]}
```

红线使用可配置的代码阈值；默认心率高于等于 140 或低于等于 42，需连续 3 个新鲜样本确认，样本最长 30 分钟。每类异常冷却 30 分钟，每日上限 10 次。红线不调用巡检 SLM。

## 5. 与现有网关/聊天库衔接

| 现有应用中的职责 | Deamo Wake 接口 | 聊天程序继续提供的内容 |
|---|---|---|
| 用户消息入口 | `engine.user_message(conversation_id, message_id, text, at)` | 稳定 conversation ID 和真实消息身份 |
| 正常助手回复完成 | `engine.assistant_activity(at)` | 在聊天程序回复结束时调用；只记录助手活动时间 |
| 原生聊天会话与上下文 | runner request 的 `conversation_id` / `native_thread_id` | 用现有会话映射查 history、persona、长期记忆，并交给正常聊天 runtime |
| 手机/在场/位置/健康采集 | `engine.observe(event)` | 用户授权的传感器或业务事件，转换成本文 JSON 形状 |
| 通知/推送出口 | 注入 `notifier(conversation_id, message)` | 聊天程序原有通知服务，并返回明确 `{"delivered": true}` |
| 情境与唤醒状态 | `SQLiteStore(path)` | Deamo Wake 自有 SQLite 状态文件；聊天程序业务库继续由 runner 通过原访问层查询 |

因此，独立后的聊天程序只需把原聊天消息和助手回复挂到两个方法，把已有 conversation/thread ID 传入。历史或长期记忆由 runner 通过聊天程序已有的数据访问层取回，并加入正常模型上下文。

## 6. 数据授权与接入阶段

Deamo Wake 只读取聊天程序选择提交的消息、设备、地点和健康信息。可先接最小聊天和定时能力，再按产品体验逐项请求并接入其它数据源：

1. **聊天与闹钟**：接真实用户消息、`assistant_activity`、`SQLiteStore` 和正常 runner。
2. **持续 scene 与短期字条**：注入 SLM，注册工具 schema，按聊天程序 session ID 映射 native thread。
3. **手机与在场状态**：接入获得用户授权的 phone/presence 事件，配置 App allowlist。
4. **地点条件**：接入用户授权的围栏事件和地点名称/角色配置。
5. **可穿戴与红线**：传入用户授权的健康样本及事件时间。
6. **外部通知**：接入通知回调，并只由明确的 `notify_user` 调用发送。

## 7. 时间、并发与本地验证

时间戳以 UTC 保存。活动时段、自然日计数、睡眠时段和条件时间窗按固定 UTC+08:00 计算，不跟随机器时区。单个 engine 实例在进程内串行协调；一个数据库应使用一个 `WakeEngine`/`SQLiteStore` 实例。

在项目根运行：

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -e .
.venv/bin/python examples/local_wake.py
.venv/bin/python examples/scene_condition.py
.venv/bin/python -m unittest discover -s tests -v
```

两个示例使用 SQLite 与本地确定性 SLM/runner/notifier，分别演示 alarm 和 scene-condition 全链路。真实模型服务和聊天程序通知通道可在集成阶段替换这些 callable。
