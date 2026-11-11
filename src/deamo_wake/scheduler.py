"""One-shot priority scheduler for alarms, scenes, patrol and redline."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

_WAKE_KEY = "collar_wake"
_AUTO_WAKE_SCANNER_SYSTEM = """You decide whether a scheduled patrol should wake the assistant to review the supplied report. Return JSON only: wake_master (boolean), reason (short string), urgency (low, normal, high).

Use only facts in the report. A location arrival or departure at a meaningful configured place can justify a review when more than one hour has passed since conversation. Clearly unusual timing or location patterns can justify a high-urgency review. If the user is actively chatting, do not wake. Stale data lowers confidence, but neither creates a reason to wake nor automatically suppresses other fresh evidence. When uncertain, lean toward waking the assistant to review; the assistant independently decides whether to contact the user. Never invent location, activity, or conversation facts."""
_TICK_S = 60
_DEDUP_WINDOW_MIN = 10
_SILENCE_GATE_MIN = 15
_EVENTS_WINDOW_H = 3

_DEFAULT_STATE: dict[str, Any] = {
    "enabled": True,
    "patrol_interval_min": 15,
    "active_hours": [9, 24],
    "alarms": [],                  # [{id, at, note, fired_at?}]
    "snooze_until": None,
    "last_master_wake_at": None,
    "last_patrol_at": None,
    "daily_wake_date": None,       # +08 自然日 "2026-07-03"
    "daily_wake_count": 0,
    "daily_wake_cap": 50,
    "scene_api_wake_daily_cap": 20,  # Optional API scene attempt budget; disabled by default.
    "scene_api_wake_date": None,
    "scene_api_wake_count": 0,
    "wake_live": False,       # 安全阀:false=观察模式只记日志,true=真唤醒(B-2)
    "conversation_id": None,  # 唤醒消息落哪个对话;空=最近活跃
}

_TZ = timezone(timedelta(hours=8))


# ------------------------------------------------------------
# 小工具
# ------------------------------------------------------------

def _now(storage) -> datetime:
    return storage.now()


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False)


def _parse_dt(raw: Any, *, naive_tz: timezone = timezone.utc) -> Optional[datetime]:
    """Parse ISO timestamps; naive input uses the supplied timezone."""
    if raw is None:
        return None
    if isinstance(raw, datetime):
        dt = raw
    else:
        text = str(raw).strip()
        if not text:
            return None
        dt = None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                        "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%dT%H:%M"):
                try:
                    dt = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue
        if dt is None:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=naive_tz)
    return dt


def _age_min(now: datetime, dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    return max(0, int((now - dt).total_seconds() // 60))


def _fmt_bj(raw: Any) -> Optional[str]:
    """Render in the configured UTC+08 local timezone; storage stays UTC."""
    dt = _parse_dt(raw)
    return dt.astimezone(_TZ).isoformat(timespec="minutes") if dt else None


def _local_date(now: datetime) -> str:
    return now.astimezone(_TZ).strftime("%Y-%m-%d")


# ------------------------------------------------------------
# 状态存取 (app_settings["collar_wake"])
# ------------------------------------------------------------

async def _load_state(storage) -> dict[str, Any]:
    state = dict(_DEFAULT_STATE)
    raw = await storage.get_setting(_WAKE_KEY)
    if isinstance(raw, dict):
        state.update(raw)
    if not isinstance(state.get("alarms"), list):
        state["alarms"] = []
    return state


async def _save_state(storage, state: dict[str, Any], *, update_snooze: bool = False) -> None:
    # The loop may retain a snapshot across model/device awaits. Serialize the
    # snooze field with scene creation/claim and preserve newer snooze writes.
    async with storage.state_lock:
        latest = await storage.get_setting(_WAKE_KEY)
        if not update_snooze and isinstance(latest, dict) and "snooze_until" in latest:
            state["snooze_until"] = latest["snooze_until"]
        await _save_state_unlocked(storage, state)


async def _save_state_unlocked(storage, state: dict[str, Any]) -> None:
    # 已触发的旧闹钟只留最近 20 条,防止无限增长
    alarms = state.get("alarms") or []
    fired = [a for a in alarms if a.get("fired_at")]
    if len(fired) > 20:
        keep_fired = set(id(a) for a in fired[-20:])
        state["alarms"] = [a for a in alarms
                           if not a.get("fired_at") or id(a) in keep_fired]
    await storage.set_setting(_WAKE_KEY, state)


def _effective_daily_count(state: dict[str, Any], now: datetime) -> int:
    if state.get("daily_wake_date") != _local_date(now):
        return 0
    return int(state.get("daily_wake_count") or 0)


def _bump_daily_count(state: dict[str, Any], now: datetime) -> None:
    today = _local_date(now)
    if state.get("daily_wake_date") != today:
        state["daily_wake_date"] = today
        state["daily_wake_count"] = 0
    state["daily_wake_count"] = int(state.get("daily_wake_count") or 0) + 1


# ------------------------------------------------------------
# 硬规则 (B3)
# ------------------------------------------------------------

def _in_active_hours(state: dict[str, Any], now: datetime) -> bool:
    hours = state.get("active_hours") or [9, 24]
    try:
        start, end = int(hours[0]), int(hours[1])
    except Exception:
        start, end = 9, 24
    h = now.astimezone(_TZ).hour
    if start <= end:
        return start <= h < end
    return h >= start or h < end    # 跨午夜写法,顺手支持


def _due_alarms(state: dict[str, Any], now: datetime) -> list[dict[str, Any]]:
    due = []
    for a in state.get("alarms") or []:
        if not isinstance(a, dict) or a.get("fired_at"):
            continue
        at = _parse_dt(a.get("at"), naive_tz=_TZ)
        if at is None:
            a["fired_at"] = _iso(now)
            a["error"] = "unparseable_at"
            continue
        if at <= now:
            due.append(a)
    return due


def _alarm_imminent(state: dict[str, Any], now: datetime) -> bool:
    horizon = now + timedelta(minutes=_DEDUP_WINDOW_MIN)
    for a in state.get("alarms") or []:
        if not isinstance(a, dict) or a.get("fired_at"):
            continue
        at = _parse_dt(a.get("at"), naive_tz=_TZ)
        if at is not None and now < at <= horizon:
            return True
    return False


def _patrol_due(state: dict[str, Any], now: datetime) -> bool:
    last = _parse_dt(state.get("last_patrol_at"))
    if last is None:
        return True
    interval = max(1, int(state.get("patrol_interval_min") or 15))
    return now >= last + timedelta(minutes=interval)


def _patrol_skip_reason(
    state: dict[str, Any],
    now: datetime,
    last_user_at: Optional[datetime],
    last_assistant_at: Optional[datetime],
    asleep: bool = False,
) -> Optional[str]:
    """B3 规则 2-5,返回第一条命中的跳过原因;None = 值得叫自动唤醒扫描器看。"""
    if not _in_active_hours(state, now):
        return "outside_active_hours"

    if not state.get("wake_live"):
        return "wake_disabled"

    dedup = timedelta(minutes=_DEDUP_WINDOW_MIN)
    last_wake = _parse_dt(state.get("last_master_wake_at"))
    for candidate in (last_wake, last_assistant_at):
        if candidate is not None and now - candidate < dedup:
            return "master_recently_awake"
    if _alarm_imminent(state, now):
        return "alarm_imminent"

    if asleep:                                         # 用户睡眠期间，暂停巡逻唤醒
        return "user_asleep"

    if last_user_at is not None and now - last_user_at < timedelta(minutes=_SILENCE_GATE_MIN):
        return "user_recently_active"

    snooze = _parse_dt(state.get("snooze_until"))
    if snooze is not None and now < snooze:
        return "snoozed"
    if _effective_daily_count(state, now) >= int(state.get("daily_wake_cap") or 50):
        return "daily_cap_reached"
    return None


async def _last_message_times(storage) -> dict[str, Optional[datetime]]:
    chat = await storage.get_setting("chat_activity") or {}
    return {"user": _parse_dt(await storage.latest_user_message_at()),
            "assistant": _parse_dt(chat.get("last_assistant_at"))}


# ------------------------------------------------------------
# 自动唤醒扫描器输入:位置版自动唤醒检查报告 (A-1 上线后健康数据自动汇入 collar_status,
# 届时这里换成读 collar_status 全家桶)
# ------------------------------------------------------------

def _recent_events(now: datetime) -> list[str]:
    return []


async def _build_report(state, now, times, *, device_state=None, location_events=None):
    user_at = times.get("user")
    age = _age_min(now, user_at)
    facts = []
    for event in location_events or []:
        if isinstance(event, dict):
            facts.append({k: event.get(k) for k in ("kind", "event", "place", "occurred_at", "accuracy_m") if event.get(k) is not None})
    snapshot = device_state if isinstance(device_state, dict) else {}
    return "\n".join((
        f"Current time: {now.astimezone(_TZ).isoformat(timespec='minutes')}",
        f"Last real user message age: {age if age is not None else 'unknown'} minutes",
        f"Wake check interval: {state.get('patrol_interval_min', 15)} minutes",
        f"Provided device snapshot: {_json({'phone': snapshot.get('phone'), 'presence': snapshot.get('presence')})}",
        f"Provided recent location events: {_json(facts)}",
    ))


async def _call_auto_wake_scanner(slm, report: str, system_prompt: Optional[str] = None) -> dict[str, Any]:
    raw = await slm(
        system=system_prompt or _AUTO_WAKE_SCANNER_SYSTEM,
        user_content=report,
        max_tokens=800,   # v4-flash 带推理链,给足预算防 finish=length 空 content
        temperature=0.0,
    )
    urgency = str(raw.get("urgency") or "low").lower()
    return {
        "wake_master": bool(raw.get("wake_master")),
        "reason": str(raw.get("reason") or "")[:200],
        "urgency": urgency if urgency in ("low", "normal", "high") else "low",
    }


# ------------------------------------------------------------
# 唤醒(B-1 桩:只记日志。B-2 在这里接真正的 Opus 唤醒管线)
# ------------------------------------------------------------

async def _record_real_wake(storage, now):
    state = await _load_state(storage)
    state["last_master_wake_at"] = _iso(now)
    _bump_daily_count(state, now)
    await _save_state(storage, state)


def _effective_scene_api_count(state, now):
    today = now.astimezone(_TZ).strftime("%Y-%m-%d")
    return int(state.get("scene_api_wake_count") or 0) if state.get("scene_api_wake_date") == today else 0


async def claim_scene_api_wake_budget(storage, now):
    state = await _load_state(storage)
    cap = int(state.get("scene_api_wake_daily_cap") or 0)
    used = _effective_scene_api_count(state, now)
    if cap <= 0:
        return True, used, cap
    if used >= cap:
        return False, used, cap
    state["scene_api_wake_date"] = now.astimezone(_TZ).strftime("%Y-%m-%d")
    state["scene_api_wake_count"] = used + 1
    await _save_state(storage, state)
    return True, used + 1, cap


async def tick(storage, *, slm=None, wake, device_state=None, location_events=None,
               known_places=None, known_apps=None, now=None):
    """Run one scheduler pass in the source priority order."""
    now = now or storage.now()
    state = await _load_state(storage)
    if not state.get("enabled", True) or not state.get("wake_live", False):
        return {"skipped": "wake_disabled"}
    from . import companion
    due = _due_alarms(state, now)
    if due:
        for alarm in due:
            alarm["fired_at"] = _iso(now)
        await _save_state(storage, state)
        request = {"trigger": "alarm", "alarms": [
            {k: alarm.get(k) for k in ("id", "at", "note")} for alarm in due]}
        result = await wake(request)
        if isinstance(result, dict): result.setdefault("trigger", "alarm")
        if isinstance(result, dict) and result.get("ok"):
            await _record_real_wake(storage, now)
        return result
    asleep = await companion._user_asleep(storage, now, device_state=device_state)
    if location_events:
        await companion.process_location_events(storage, location_events=location_events,
                                                known_places=known_places)
    await companion.check_wake_conditions(storage, device_state=device_state,
                                          known_places=known_places, known_apps=known_apps,
                                          location_events=location_events)
    bundle = await companion.claim_due_scene_wake(storage, now, asleep=asleep)
    if bundle:
        wake_state = await storage.get_setting(_WAKE_KEY) or {}
        bedtime_confirmed = bundle.get("wake_reason") == "bedtime_asleep_confirmed"
        if wake_state.get("scene_api_budget_enabled") and not bedtime_confirmed:
            allowed, _, _ = await claim_scene_api_wake_budget(storage, now)
            if not allowed:
                await companion.complete_scene_wake(storage, bundle, {"skipped": "scene_api_daily_cap", "accepted": False})
                return {"skipped": "scene_api_daily_cap"}
        if not await companion.revalidate_scene_wake(storage, bundle):
            await companion.complete_scene_wake(storage, bundle, {"skipped": "scene_suppressed_before_dispatch", "accepted": False})
            return {"skipped": "scene_suppressed_before_dispatch"}
        request = {"trigger": "companion_scene", "scene_bundle": bundle,
                   "conversation_id": bundle.get("conversation_id")}
        result = await wake(request)
        if isinstance(result, dict): result.setdefault("trigger", "companion_scene")
        await companion.complete_scene_wake(storage, bundle, result)
        if isinstance(result, dict) and result.get("ok"):
            await _record_real_wake(storage, now)
        return result
    if not _patrol_due(state, now):
        return {"skipped": "patrol_not_due"}
    state["last_patrol_at"] = _iso(now)
    await _save_state(storage, state)
    times = await _last_message_times(storage)
    skip = _patrol_skip_reason(state, now, times.get("user"), times.get("assistant"), asleep=asleep)
    if skip:
        return {"skipped": skip}
    if companion._active_from_state(await storage.get_setting("collar_companion") or {}, now):
        return {"skipped": "scene_active"}
    if slm is None:
        return {"skipped": "slm_unavailable"}
    report = await _build_report(state, now, times, device_state=device_state, location_events=location_events)
    try:
        verdict = await _call_auto_wake_scanner(slm, report)
    except Exception as exc:
        return {"error": str(exc)[:200], "accepted": None}
    if not verdict["wake_master"]:
        return {"skipped": "scanner_declined", "scanner": verdict}
    result = await wake({"trigger": "patrol", "scanner": verdict})
    if isinstance(result, dict): result.setdefault("trigger", "patrol")
    if isinstance(result, dict) and result.get("ok"):
        await _record_real_wake(storage, now)
    return result


async def check_redline(storage, samples, *, wake, now=None):
    """Apply the source heart-rate threshold, freshness, cooldown and daily cap rules."""
    now = now or storage.now()
    cfg = {"hr_high": 140, "hr_low": 42, "confirm_samples": 3,
           "cooldown_min": 30, "max_sample_age_min": 30, "daily_redline_cap": 10}
    custom = await storage.get_setting("collar_health")
    if isinstance(custom, dict):
        cfg.update({k: custom[k] for k in cfg.keys() & custom.keys()})
    telemetry = await storage.get_setting("telemetry") or {}
    fresh = []
    def fresh_hr(sample):
        if not isinstance(sample, dict) or sample.get("type") != "heart_rate":
            return None
        at = _parse_dt(sample.get("at"))
        try: value = float(sample.get("value"))
        except (TypeError, ValueError): return None
        if at and timedelta(0) <= now - at <= timedelta(minutes=int(cfg["max_sample_age_min"])):
            return at, value
        return None
    fresh_new = [item for sample in (samples or []) if (item := fresh_hr(sample))]
    if not fresh_new:
        return {"redline": False}
    seen = set()
    recent = list(telemetry.get("health_samples") or []) + list(samples or [])
    for sample in recent:
        item = fresh_hr(sample)
        if item is None:
            continue
        key = (sample.get("event_id") or sample.get("id") or item)
        if key not in seen:
            seen.add(key)
            fresh.append(item)
    fresh.sort(key=lambda pair: pair[0])
    count = max(1, int(cfg["confirm_samples"]))
    if len(fresh) < count:
        return {"redline": False}
    window = fresh[-count:]
    values = [value for _, value in window]
    if all(value >= float(cfg["hr_high"]) for value in values):
        breach, threshold = "high", float(cfg["hr_high"])
    elif all(value <= float(cfg["hr_low"]) for value in values):
        breach, threshold = "low", float(cfg["hr_low"])
    else:
        return {"redline": False}
    state = await storage.get_setting("redline_state") or {}
    last = _parse_dt((state.get("last_trigger") or {}).get(breach))
    if last and now - last < timedelta(minutes=int(cfg["cooldown_min"])):
        return {"redline": False}
    today = now.astimezone(_TZ).date().isoformat()
    if state.get("date") != today:
        state = {"date": today, "count": 0, "last_trigger": state.get("last_trigger", {})}
    if int(state.get("count") or 0) >= int(cfg["daily_redline_cap"]):
        return {"redline": False}
    state.setdefault("last_trigger", {})[breach] = _iso(now)
    state["count"] = int(state.get("count") or 0) + 1
    await storage.set_setting("redline_state", state)
    latest_at, latest_value = window[-1]
    detail = {"breach": breach, "threshold": threshold, "values": values,
              "confirm_samples": count, "latest_value": latest_value,
              "latest_at_bj": latest_at.astimezone(_TZ).isoformat(timespec="minutes"),
              "data_age_min": max(0, int((now-latest_at).total_seconds() // 60))}
    wake_state = await storage.get_setting("collar_wake") or {}
    if wake_state.get("wake_live") and wake_state.get("enabled", True):
        detail["wake_result"] = await wake({"trigger": "redline", "redline": detail})
    else:
        detail["wake_result"] = {"skipped": "wake_disabled"}
    return {"stored": len(samples or []), "redline": detail}


# ------------------------------------------------------------
# Public alarm and scheduler controls
# ------------------------------------------------------------

async def execute_collar_wake(storage, arguments: dict[str, Any]) -> str:
    args = arguments or {}
    action = str(args.get("action") or "").strip()
    state = await _load_state(storage)
    now = _now(storage)

    if action == "status":
        pending = [a for a in state.get("alarms") or [] if not a.get("fired_at")]
        last_patrol = _parse_dt(state.get("last_patrol_at"))
        interval = max(1, int(state.get("patrol_interval_min") or 15))
        next_eta = None
        if last_patrol is not None:
            next_eta = max(0, int((last_patrol + timedelta(minutes=interval) - now).total_seconds() // 60))
        return _json({
            "mode": "local scheduler",
            "enabled": state.get("enabled"),
            "auto_wake_check_interval_min": interval,
            "active_hours": state.get("active_hours"),
            "next_auto_wake_check_in_min": next_eta,
            "pending_alarms": pending,
            "snooze_until": _fmt_bj(state.get("snooze_until")),
            "last_master_wake_at": _fmt_bj(state.get("last_master_wake_at")),
            "today_wake_count": _effective_daily_count(state, now),
            "daily_wake_cap": state.get("daily_wake_cap"),
            "scene_api_wakes_today": _effective_scene_api_count(state, now),
            "scene_api_wake_daily_cap": state.get("scene_api_wake_daily_cap"),
            "wake_live": bool(state.get("wake_live")),

        })

    if action == "set_interval":
        minutes = args.get("minutes")
        if not isinstance(minutes, (int, float)) or not (5 <= int(minutes) <= 240):
            return _json({"error": "minutes must be 5-240"})
        state["patrol_interval_min"] = int(minutes)
        await _save_state(storage, state)
        return _json({"ok": True, "auto_wake_check_interval_min": int(minutes)})

    if action == "set_alarm":
        at = _parse_dt(args.get("at"), naive_tz=_TZ)
        if at is None:
            return _json({"error": "invalid 'at' — ISO time required, e.g. 2026-07-04T07:30+08:00"})
        if at <= now:
            return _json({"error": "alarm time is in the past", "now_utc8": now.astimezone(_TZ).isoformat(timespec="minutes")})
        alarm = {"id": uuid.uuid4().hex[:8], "at": at.astimezone(_TZ).isoformat(timespec="minutes"),
                 "note": str(args.get("note") or "")[:200], "created_at": _iso(now)}
        state.setdefault("alarms", []).append(alarm)
        state["alarms"].sort(key=lambda a: str(a.get("at") or ""))
        await _save_state(storage, state)
        return _json({"ok": True, "alarm": alarm})

    if action == "clear_alarm":
        target = str(args.get("alarm_id") or "").strip()
        if not target:
            return _json({"error": "alarm_id required ('all' clears every pending alarm)"})
        before = [a for a in state.get("alarms") or [] if not a.get("fired_at")]
        if target == "all":
            state["alarms"] = [a for a in state.get("alarms") or [] if a.get("fired_at")]
        else:
            state["alarms"] = [a for a in state.get("alarms") or []
                               if a.get("fired_at") or a.get("id") != target]
        after = [a for a in state["alarms"] if not a.get("fired_at")]
        await _save_state(storage, state)
        return _json({"ok": True, "cleared": len(before) - len(after), "pending": after})

    if action == "snooze":
        minutes = args.get("minutes")
        if not isinstance(minutes, (int, float)) or not (5 <= int(minutes) <= 720):
            return _json({"error": "minutes must be 5-720"})
        until = now + timedelta(minutes=int(minutes))
        state["snooze_until"] = _iso(until)
        await _save_state(storage, state, update_snooze=True)
        return _json({"ok": True, "snooze_until_utc8": until.astimezone(_TZ).isoformat(timespec="minutes")})

    if action == "goodnight":
        # 收档当天自动唤醒检查:睡到明天活动时段开始。闹钟与红线不受影响。
        start_h = int((state.get("active_hours") or [9, 24])[0])
        local = now.astimezone(_TZ)
        target = local.replace(hour=start_h, minute=0, second=0, microsecond=0)
        if target <= local:
            target += timedelta(days=1)
        state["snooze_until"] = _iso(target)
        await _save_state(storage, state, update_snooze=True)
        return _json({"ok": True, "goodnight_until_utc8": target.isoformat(timespec="minutes"),
                      "note": "alarms still fire; automatic checks resume at active-hours start"})

    return _json({"error": f"unknown action '{action}'",
                  "actions": ["status", "set_interval", "set_alarm", "clear_alarm",
                              "snooze", "goodnight"]})
