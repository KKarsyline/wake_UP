# 本地示例

在 Python 3.12 下，从项目根创建隔离环境并运行两个场景：

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -e .
.venv/bin/python examples/local_wake.py
.venv/bin/python examples/scene_condition.py
```

`local_wake.py` 展示真实用户消息、设置闹钟、scheduler tick、runner 接受会话、显式调用 `notify_user` 和回执持久化。`scene_condition.py` 展示 SLM 对话增量、设置 `wake_when`、phone 事件命中、工具更新 cadence、显式通知和条件结算。

两个示例都使用临时 SQLite 数据库与确定性本地 callback，不需要模型服务、设备或通知账号。接入真实应用时，用聊天程序自己的 SLM callable、正常聊天 runner 和 notifier 替换示例函数；已有 persona、对话历史和记忆由聊天程序 runner 按 conversation/thread 映射继续提供。
