"""Pure wake report projection. It never reads storage or consumes notes."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

_TZ = timezone(timedelta(hours=8))
_GUIDANCE = {
    "outing": "用户正在外出；结合场景记录判断是否需要再次联系。",
    "work": "用户正在工作或学习；结合最近进展决定何时继续。",
    "bedtime": "用户准备休息；遵循场景中明确留下的安排。",
    "morning": "用户已开始新的一天；按当前情况决定是否联系。",
    "active_without_chat": "设备活动持续但尚无新消息；结合已知状态判断是否联系。",
    "silence_escalation": "场景包含持续跟进安排；每次根据最新状态重新判断。",
    "repeat": "持续场景仍有效；遵循用户设定或场景默认的节奏。",
    "private_play": "设备事件触发了此场景；只依据明确记录判断下一步。",
    "sulking": "根据最近消息和场景记录决定是否继续联系。",
    "custom": "结合用户的消息和场景记录，自主决定下一步。",
}
_NOTE_INTRO = "【短期字条】这段内容会在后续有效唤醒中逐次读取。"


def _dt(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _time(value):
    parsed = _dt(value)
    return parsed.astimezone(_TZ).isoformat(timespec="minutes") if parsed else str(value or "未知")


def _age(value, now):
    parsed = _dt(value)
    if parsed is None:
        return "未知"
    minutes = max(0, int((now - parsed).total_seconds() // 60))
    return f"{minutes} 分钟" if minutes < 60 else f"{minutes // 60} 小时 {minutes % 60} 分钟"


def _note(record, owner, refs):
    if not isinstance(record, dict):
        return ""
    nudge, left, identity = record.get("nudge"), record.get("nudge_wakes_left"), record.get("id")
    try:
        if left is not None and int(left) <= 0:
            return ""
    except (TypeError, ValueError):
        return ""
    if not isinstance(nudge, str) or not nudge.strip() or identity is None:
        return ""
    ref = {"owner": owner, "id": identity, "nudge": nudge, "nudge_at": record.get("nudge_at"),
           "nudge_version": record.get("nudge_version")}
    if ref in refs:
        return ""
    refs.append(ref)
    return f"{_NOTE_INTRO}\n【记录于 {_time(record.get('nudge_at'))}】{nudge}"


def _trigger_text(trigger, alarms, primary_scene, scenes):
    if trigger == "companion_scene":
        scene = primary_scene or next(iter(scenes), {})
        condition = scene.get("wake_condition") or {}
        title = scene.get("title") or scene.get("type") or "持续场景"
        if condition.get("met_at"):
            return f"【唤醒原因】等待的事件已发生：{condition.get('text', '')}（{condition.get('summary', '')}）\n{condition.get('met_event', '')}"
        return f"【唤醒原因】持续场景“{title}”到了下次检查时间。"
    if trigger == "alarm" and alarms:
        return "\n".join(f"【闹钟】{a.get('at')}；备注：{a.get('note') or '无'}" for a in alarms)
    return f"【唤醒原因】{trigger}"


def _device_state(state):
    if not isinstance(state, dict):
        return ""
    raw = state.get("presence") or {}
    lines = []
    for name, device in sorted(raw.get("devices", {}).items() if isinstance(raw.get("devices"), dict) else []):
        if isinstance(device, dict):
            fields = [f"{key}：{device[key]}" for key in ("state", "online", "screen_locked", "idle_seconds", "last_seen_at") if isinstance(device.get(key), (str, int, float, bool))]
            if fields:
                lines.append(f"{name}：" + "；".join(fields))
    phone = state.get("phone")
    apps = phone.get("current_app", {}) if isinstance(phone, dict) else {}
    if isinstance(apps, dict):
        for device, app in sorted(apps.items()):
            name = app.get("app") or app.get("name") if isinstance(app, dict) else None
            if name:
                lines.append(f"{device}：应用上报 {name}")
    events = state.get("location_events")
    if isinstance(events, list):
        lines.extend(event for event in events[-5:] if isinstance(event, str) and event.strip())
    samples = state.get("health_samples")
    if isinstance(samples, list):
        for sample in samples[-5:]:
            if isinstance(sample, dict):
                fields = [f"{key}：{sample[key]}" for key in ("type", "value", "at")
                          if isinstance(sample.get(key), (str, int, float))]
                if fields:
                    lines.append("健康样本：" + "；".join(fields))
    return "【已知设备状态】\n" + "\n".join(lines) if lines else ""


def render_report(*, now: datetime, full: bool, trigger: str, scenes: list[dict],
                  device_state: dict | None = None,
                  alarms: list | None = None, scanner: dict | None = None,
                  extra_scene_text: str = "", primary_scene: dict | None = None,
                  redline: dict | None = None) -> tuple[str, list[dict]]:
    """Return report text and only the note identities displayed in it."""
    now = now.replace(tzinfo=timezone.utc) if now.tzinfo is None else now
    refs, active, seen = [], [], set()
    for scene in scenes or []:
        if not isinstance(scene, dict) or scene.get("status") != "active":
            continue
        expires, identity = _dt(scene.get("expires_at")), scene.get("id")
        if (expires and expires <= now) or (identity is not None and identity in seen):
            continue
        if identity is not None:
            seen.add(identity)
        active.append(scene)
    active.sort(key=lambda s: (-int(s.get("priority") or 0), str(s.get("started_at") or "")))
    active = active[:4]
    scene_lines = []
    for scene in active:
        kind = scene.get("type") or "custom"
        scene_lines += [f"【持续场景：{scene.get('title') or kind}】", f"已持续：{_age(scene.get('started_at'), now)}"]
        facts = scene.get("facts") or []
        facts = facts if isinstance(facts, list) else [facts]
        unique = {}
        for fact in facts:
            text = " ".join(str(fact or "").split())
            if text:
                unique[re.sub(r"^\d{1,2}-\d{1,2} \d{1,2}:\d{2}\s+", "", text)] = text
        if unique:
            scene_lines.append("已知事实：" + "；".join(list(unique.values())[-4:]))
        scene_lines.append(_GUIDANCE.get(kind, _GUIDANCE["custom"]))
        note = _note(scene, "scene", refs)
        if note:
            scene_lines.append(note)
        if scene.get("next_wake_at"):
            scene_lines.append("下次检查：" + _time(scene["next_wake_at"]))
        scene_lines.append(f"检查节奏：每 {scene.get('cadence_min', 0)} 分钟（{'自定义' if scene.get('cadence_by_master') else '默认'}）")
        condition = scene.get("wake_condition") or {}
        if condition and not condition.get("met_at"):
            scene_lines.append(f"等待条件：“{condition.get('text', '')}”（{condition.get('summary', '')}）")
    reason = _trigger_text(trigger, alarms or [], primary_scene, active)
    parts = [f"【自动唤醒】助手收到一次后台唤醒请求。\n【当前时间】{_time(now)}", reason]
    devices = _device_state(device_state)
    if devices:
        parts.append(devices)
    if redline:
        parts.append("【阈值事件】" + "; ".join(f"{k}={redline[k]}" for k in ("breach", "threshold", "latest_value", "latest_at") if redline.get(k) is not None))
    if scanner:
        parts.append("【巡检提示】" + str(scanner.get("reason") or "需要检查当前状态"))
    if scene_lines or extra_scene_text.strip():
        body = "\n\n".join(scene_lines)
        if extra_scene_text.strip():
            body += ("\n\n" if body else "") + extra_scene_text.strip()
        parts.append("<scenes>\n" + body + "\n</scenes>")
    if full:
        parts.append("【可用工具】collar_companion 用于查看或调整场景、短期字条、闹钟和唤醒节奏；notify_user 仅在你明确要联系用户时发送消息。普通回复不会自动发送。")
    else:
        parts.append("需要联系用户时使用 notify_user；普通回复不会自动发送。")
    return "\n\n".join(part for part in parts if part), refs
