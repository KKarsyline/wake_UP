"""Persistent companion scenes for proactive contact.

The scene engine keeps context available across one-shot model turns.
Chat semantics are classified by the SLM; exact device/location signals are
handled deterministically.  A scene owns its next wake time, so a recurring
check-in does not depend on the model remembering to create another alarm.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

_TZ = timezone(timedelta(hours=8))
_STATE_KEY = "collar_companion"

_SNOOZABLE_DEVICE_SCENES = {"active_without_chat", "morning"}

_SCENE_TYPES = {
    "outing", "work", "bedtime", "morning", "active_without_chat",
    "silence_escalation", "repeat", "private_play", "sulking", "custom",
}

_SCENE_DEFAULTS: dict[str, dict[str, int]] = {
    "outing": {"cadence_min": 30, "duration_min": 8 * 60, "priority": 50},
    "work": {"cadence_min": 60, "duration_min": 10 * 60, "priority": 40},
    "bedtime": {"cadence_min": 45, "duration_min": 10 * 60, "priority": 55},
    "morning": {"cadence_min": 20, "duration_min": 3 * 60, "priority": 60},
    "active_without_chat": {"cadence_min": 30, "duration_min": 4 * 60, "priority": 45},
    "silence_escalation": {"cadence_min": 15, "duration_min": 4 * 60, "priority": 65},
    "repeat": {"cadence_min": 10, "duration_min": 3 * 60, "priority": 70},
    "private_play": {"cadence_min": 10, "duration_min": 60, "priority": 100},
    "sulking": {"cadence_min": 20, "duration_min": 8 * 60, "priority": 75},
    "custom": {"cadence_min": 45, "duration_min": 6 * 60, "priority": 35},
}

# 便条(nudge)是助手专用槽位:只有助手工具与模型回合写得进去,
# 设备事件与 SLM 一律拿不到这支笔。写入后最多被读三次。
_NUDGE_SOURCES = {"master_tool", "model"}
_NUDGE_WAKES = 3


def write_nudge(target, text, now, *, source="master_tool"):
    """Write a short-lived note on a persistent scene."""
    if source not in _NUDGE_SOURCES:
        raise ValueError("only the main model may write a short note")
    text = " ".join(str(text).split())[:800]
    target.update(nudge=text, nudge_at=_iso(now) if text else None,
                  nudge_source=source if text else None,
                  nudge_version=uuid.uuid4().hex if text else None,
                  nudge_wakes_left=_NUDGE_WAKES if text else 0)
    target.pop("nudge_last_receipt", None)


def consume_nudge(target, *, expected=None, receipt=None):
    """Consume the exact rendered note once; never consume its replacement."""
    if not target.get("nudge"):
        return False
    if expected and any(target.get(k) != expected.get(k)
                        for k in ("nudge", "nudge_at", "nudge_version")):
        return False
    if receipt and target.get("nudge_last_receipt") == receipt:
        return False
    left = target.get("nudge_wakes_left")
    left = (_NUDGE_WAKES if left is None else _clamp_int(left, _NUDGE_WAKES, 0, 9)) - 1
    target["nudge_wakes_left"] = max(0, left)
    if left <= 0:
        target.update(nudge="", nudge_at=None, nudge_source=None)
    if receipt:
        target["nudge_last_receipt"] = receipt
    return True

# 夜间睡着判定的默认参数;数据库 state 里的同名键可以覆盖。
_SLEEP_WINDOW_DEFAULT = (22, 11)
_HR_SLEEP_THRESHOLD_DEFAULT = 70
_SLEEP_SILENCE_MIN = 30
_MAC_IDLE_SLEEP_S = 1800

# Bedtime owns a small, persistent sleep episode.  These thresholds apply only
# after that Scene has confirmed sleep; the pre-sleep app-open "grab cat"
# behavior remains unchanged.
_BEDTIME_QUIET_MIN = 30
_WAKE_ACTION_WINDOW_MIN = 30
_WAKE_ACTION_THRESHOLD = 8
_WAKE_CONTINUOUS_MIN = 30
_ACTIVE_PROOF_MAX_GAP_S = 180
_ONLINE_REVIVAL_GAP_S = 300
_CROSS_SOURCE_COALESCE_S = 5
_ASLEEP_WAKE_LEASE_S = 300
_ASLEEP_WAKE_RETRY_MIN = (1, 2, 4, 8, 15)

_DEFAULT_STATE: dict[str, Any] = {
    "version": 1,
    "revision": 0,
    "scenes": [],
    "recreation_pauses": {},
    "processed_message_ids": [],
    "last_classifier": None,
    "last_location_event": None,
}

_STANDING_INVITATION = "Persistent scenes preserve ongoing context and are resumed using explicit user state and scheduled observations."

_SCENE_GUIDANCE: dict[str, str] = {
    "outing": "The user has an ongoing outing context. Use supplied messages and observations to decide whether to continue or end it.",
    "work": "The user has an ongoing work context. Use supplied observations and configured cadence.",
    "bedtime": "The user indicated a sleep period. Apply configured sleep evidence and return behavior.",
    "morning": "A configured device event may indicate the user is active again.",
    "active_without_chat": "Device activity is present while no recent user message exists. Follow configured cadence.",
    "silence_escalation": "This scene has an explicitly configured check-in cadence. Preserve its lifecycle and cadence.",
    "repeat": "This is a recurring check-in agreement. Follow its cadence until it expires or ends.",
    "private_play": "A caller-configured app event created this scene. Do not infer beyond the supplied event.",
    "sulking": "This scene represents ongoing disagreement context. Follow the latest user message and lifecycle.",
    "custom": "Use supplied messages and observations for this ongoing user-defined context.",
}

_SCENE_CLASSIFIER_SYSTEM = """You classify scene changes from the latest genuine user message and active scenes. Return one JSON object matching the final example. You do not speak to the user.

Scene types:
- outing: the user says they are going out, meeting someone, shopping, or staying away for a while.
- work: the user says they will work or study at work, an office, or a lab.
- bedtime: the user clearly says they are going to sleep, says goodnight, or otherwise states that sleep is next. A complaint about feeling tired, such as “I am exhausted”, is not enough without an intention to sleep.
- repeat: the user explicitly asks for recurring check-ins, reminders, or messages at a stated cadence.
- silence_escalation: the user explicitly asks the assistant to continue checking in or increase message frequency when unanswered.
- sulking: the user says they are angry, ending the conversation, or refusing to talk during a disagreement.
- custom: another clearly ongoing situation worth revisiting.
- morning, active_without_chat, and private_play are created only from caller-configured device events, never from ordinary messages without explicit evidence.

Rules:
1. One message may end an old scene and start a new one; return no more than three deltas.
2. End outing when the user explicitly says they returned; end work when they explicitly finish; end bedtime or morning when they explicitly say they woke up; end sulking when the user reconciles, resumes normal conversation, or explicitly asks for space.
3. Return no more than four facts for a delta. For repeat, extract cadence_min and duration_min only when stated. Otherwise use null so the code applies defaults.
4. Return no delta for ordinary chat, a passing emotion, a one-time action, or an unclear situation. Do not invent a continuing scene.
5. Scenes represent situations already underway or just ended. A future plan (tomorrow, next week, a later date or time) must not start a scene early. If a message contains both a current situation and a future plan, classify only the current situation.

Return a JSON object with a deltas array and a short reason. Each delta uses action=start/update/end, scene_type, facts, cadence_min, duration_min, and reason. Use an empty array when nothing changes. Example:
{"deltas":[{"action":"start","scene_type":"bedtime","facts":["User said they are going to sleep"],"cadence_min":null,"duration_min":null,"reason":"Explicit sleep intention"}],"reason":"The user stated a current ongoing sleep period."}"""

def _now(storage) -> datetime:
    return storage.now()


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def _parse_dt(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).strip().replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False)


def _fmt_bj(raw: Any) -> Optional[str]:
    """给眼睛看的时间一律上海时间(用户 2026-07-03 定);机器字段仍存 UTC。"""
    dt = _parse_dt(raw)
    return dt.astimezone(_TZ).isoformat(timespec="minutes") if dt else None


def _fmt_hm(raw: Any) -> Optional[str]:
    dt = _parse_dt(raw)
    return dt.astimezone(_TZ).strftime("%H:%M") if dt else None


_BJ_TIME_FIELDS = (
    "started_at", "created_at", "updated_at", "expires_at", "next_wake_at",
    "ended_at", "last_attempt_at", "last_wake_at", "last_user_reply_at",
    "last_notification_at", "nudge_at", "sleep_confirmed_at",
    "asleep_wake_claimed_at", "asleep_wake_retry_at",
    "asleep_wake_delivered_at",
)


def _bj_view(scene: Any) -> Any:
    """出口视图:深拷贝一份并把时间字段转成上海时间,内部 state 不受影响。"""
    if not isinstance(scene, dict):
        return copy.deepcopy(scene)
    out = copy.deepcopy(scene)
    out.setdefault("cadence_by_master", False)
    if isinstance(out.get("wake_condition"), dict):
        for key in ("set_at", "met_at"):
            out["wake_condition"][key] = _fmt_bj(out["wake_condition"].get(key))
    for key in _BJ_TIME_FIELDS:
        if key in out:
            out[key] = _fmt_bj(out.get(key))
    activity = out.get("wake_activity")
    if isinstance(activity, dict):
        channels = activity.get("channels") if isinstance(activity.get("channels"), dict) else {}
        out["wake_activity"] = {
            "action_count_30m": len(activity.get("actions") or []),
            "active_channels": sorted(
                str(key) for key, value in channels.items()
                if isinstance(value, dict) and value.get("active")),
        }
    return out


def _clamp_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except Exception:
        number = default
    return max(minimum, min(maximum, number))


_FACT_STAMP_RE = re.compile(r"^(\d{1,2}-\d{1,2} \d{1,2}:\d{2})\s+")


def _split_stamp(text: str) -> tuple[str, str]:
    """把事实拆成(出生戳, 正文);没有戳时戳为空串。"""
    match = _FACT_STAMP_RE.match(text)
    return (match.group(1), text[match.end():]) if match else ("", text)


def _clean_facts(raw: Any) -> list[str]:
    values = raw if isinstance(raw, list) else ([raw] if raw else [])
    out: list[str] = []
    seen: dict[str, int] = {}
    for item in values:
        text = " ".join(str(item or "").split())
        if not text:
            continue
        stamp, body = _split_stamp(text)
        body = body[:240]
        if not body:
            continue
        merged = f"{stamp} {body}" if stamp else body
        index = seen.get(body)
        if index is None:
            seen[body] = len(out)
            out.append(merged)
        elif stamp:
            # 同一句事实又被写了一次:换成新戳那版,别把新时间丢掉。
            out[index] = merged
    return out[-12:]


def _stamp_facts(raw: Any, now: datetime) -> list[str]:
    """新写入的事实统一带上海时间出生戳;已经带戳的原样保留。"""
    local = now.astimezone(_TZ)
    stamp = f"{local.month}-{local.day} {local.strftime('%H:%M')}"
    out: list[str] = []
    for item in _clean_facts(raw):
        existing, body = _split_stamp(item)
        out.append(f"{existing or stamp} {body}")
    return out


async def _load_state(storage) -> dict[str, Any]:
    state = copy.deepcopy(_DEFAULT_STATE)
    raw = await storage.get_setting(_STATE_KEY)
    if isinstance(raw, dict):
        state.update(raw)
    if not isinstance(state.get("scenes"), list):
        state["scenes"] = []
    if not isinstance(state.get("recreation_pauses"), dict):
        state["recreation_pauses"] = {}
    if not isinstance(state.get("processed_message_ids"), list):
        state["processed_message_ids"] = []
    return state


async def _save_state(storage, state: dict[str, Any]) -> None:
    state["revision"] = int(state.get("revision") or 0) + 1
    state["updated_at"] = _iso(_now(storage))
    state["processed_message_ids"] = list(state.get("processed_message_ids") or [])[-100:]
    scenes = list(state.get("scenes") or [])
    ended = [s for s in scenes if s.get("status") != "active"]
    if len(ended) > 30:
        keep = {str(s.get("id")) for s in ended[-30:]}
        scenes = [s for s in scenes if s.get("status") == "active" or str(s.get("id")) in keep]
    state["scenes"] = scenes
    await storage.set_setting(_STATE_KEY, state)


def _active_from_state(state: dict[str, Any], now: datetime) -> list[dict[str, Any]]:
    active: list[dict[str, Any]] = []
    for scene in state.get("scenes") or []:
        if not isinstance(scene, dict) or scene.get("status") != "active":
            continue
        expires = _parse_dt(scene.get("expires_at"))
        if expires is not None and expires <= now:
            continue
        active.append(scene)
    return sorted(active, key=lambda s: (-int(s.get("priority") or 0), str(s.get("started_at") or "")))


def _find_active(state: dict[str, Any], scene_type: str,
                 conversation_id: Optional[str] = None) -> Optional[dict[str, Any]]:
    candidates = [s for s in state.get("scenes") or []
                  if isinstance(s, dict) and s.get("status") == "active"
                  and s.get("type") == scene_type]
    if conversation_id:
        exact = [s for s in candidates if s.get("conversation_id") == conversation_id]
        if exact:
            candidates = exact
    return candidates[-1] if candidates else None


def _empty_wake_activity() -> dict[str, Any]:
    return {
        "actions": [],
        "seen_ids": [],
        "semantic_recent": [],
        "channels": {},
        "continuous_started_at": None,
        "last_active_proof_at": None,
    }


def _ensure_bedtime_fields(scene: dict[str, Any]) -> None:
    """Lazily upgrade bedtime Scenes created before the sleep-episode fields."""
    if scene.get("type") != "bedtime":
        return
    scene.setdefault("sleep_confirmed_at", None)
    scene.setdefault("asleep_wake_status", "idle")
    scene.setdefault("asleep_wake_attempts", 0)
    scene.setdefault("asleep_wake_claim_id", None)
    scene.setdefault("asleep_wake_claimed_at", None)
    scene.setdefault("asleep_wake_retry_at", None)
    scene.setdefault("asleep_wake_delivered_at", None)
    activity = scene.get("wake_activity")
    if not isinstance(activity, dict):
        scene["wake_activity"] = _empty_wake_activity()
        return
    empty = _empty_wake_activity()
    for key, value in empty.items():
        activity.setdefault(key, copy.deepcopy(value))


def _prune_wake_activity(activity: dict[str, Any], now: datetime) -> None:
    cutoff = now - timedelta(minutes=_WAKE_ACTION_WINDOW_MIN)
    actions = [item for item in (activity.get("actions") or [])
               if isinstance(item, dict)
               and (_parse_dt(item.get("at")) or now) >= cutoff]
    activity["actions"] = actions[-64:]
    semantic = [item for item in (activity.get("semantic_recent") or [])
                if isinstance(item, dict)
                and (_parse_dt(item.get("at")) or now) >= cutoff]
    activity["semantic_recent"] = semantic[-64:]
    activity["seen_ids"] = [str(v) for v in (activity.get("seen_ids") or [])][-64:]


def _record_wake_action_unlocked(
    scene: dict[str, Any], *, now: datetime, activity_id: str,
    device: str, source: str, kind: str, app: str = "",
    semantic_key: str = "",
) -> bool:
    """Record one post-sleep semantic action.  Return True when it counted."""
    _ensure_bedtime_fields(scene)
    if not scene.get("sleep_confirmed_at"):
        return False
    activity = scene["wake_activity"]
    _prune_wake_activity(activity, now)
    event_id = str(activity_id or "").strip()
    if not event_id or event_id in set(activity.get("seen_ids") or []):
        return False
    key = str(semantic_key or f"{device}:{source}:{kind}:{app}")[:180]
    for previous in reversed(activity.get("semantic_recent") or []):
        if previous.get("key") != key:
            continue
        previous_at = _parse_dt(previous.get("at"))
        if previous_at and abs((now - previous_at).total_seconds()) <= _CROSS_SOURCE_COALESCE_S:
            activity["seen_ids"] = (list(activity.get("seen_ids") or []) + [event_id])[-64:]
            return False
        break
    record = {
        "id": event_id, "at": _iso(now), "device": str(device)[:32],
        "source": str(source)[:32], "kind": str(kind)[:40],
        "app": str(app or "")[:80] or None, "semantic_key": key,
    }
    activity["actions"] = (list(activity.get("actions") or []) + [record])[-64:]
    activity["seen_ids"] = (list(activity.get("seen_ids") or []) + [event_id])[-64:]
    activity["semantic_recent"] = (
        list(activity.get("semantic_recent") or [])
        + [{"key": key, "at": _iso(now)}])[-64:]
    _prune_wake_activity(activity, now)
    scene["updated_at"] = _iso(now)
    return True


def _wake_activity_end_reason(scene: dict[str, Any], now: datetime,
                              *, closed_session_min: Any = None) -> Optional[str]:
    _ensure_bedtime_fields(scene)
    activity = scene["wake_activity"]
    _prune_wake_activity(activity, now)
    if len(activity.get("actions") or []) >= _WAKE_ACTION_THRESHOLD:
        return "bedtime_wake_action_threshold"
    try:
        if closed_session_min is not None and float(closed_session_min) >= _WAKE_CONTINUOUS_MIN:
            return "bedtime_wake_continuous_session"
    except (TypeError, ValueError):
        pass
    for channel in (activity.get("channels") or {}).values():
        if not isinstance(channel, dict) or not channel.get("active"):
            continue
        started = _parse_dt(channel.get("started_at"))
        proof = _parse_dt(channel.get("last_proof_at"))
        if (started and proof
                and (now - proof).total_seconds() <= _ACTIVE_PROOF_MAX_GAP_S
                and (now - started) >= timedelta(minutes=_WAKE_CONTINUOUS_MIN)):
            return "bedtime_wake_continuous_activity"
    return None


async def _handle_bedtime_phone_event(
    storage, body: dict[str, Any], result: dict[str, Any], *, now: datetime, scene_apps=None,
) -> Optional[dict[str, Any]]:
    """Atomically update the bedtime scene from a caller-provided phone event."""
    app_name = _normal_app(body.get("app"))
    event = str(body.get("event") or "").lower()
    device = str(body.get("device") or "iphone").lower()
    is_configured_scene_app = app_name in (scene_apps or {})
    async with storage.state_lock:
        state = await _load_state(storage)
        bedtime = _find_active(state, "bedtime")
        if bedtime is None:
            return None
        _ensure_bedtime_fields(bedtime)
        record = result.get("record") if isinstance(result.get("record"), dict) else {}
        if bedtime.get("sleep_confirmed_at") and event in {"open", "close"}:
            kind = "app_open" if event == "open" else "app_close"
            semantic = f"{device}:{'active' if event == 'open' else 'background'}:{app_name}"
            _record_wake_action_unlocked(
                bedtime, now=now,
                activity_id=str(record.get("activity_id") or result.get("activity_id") or
                                f"phone:{device}:{app_name}:{event}:{_iso(now)}"),
                device=device, source="shortcuts", kind=kind,
                app=app_name, semantic_key=semantic)
            session = result.get("closed_session")
            session_min = session.get("minutes") if isinstance(session, dict) else None
            reason = _wake_activity_end_reason(
                bedtime, now, closed_session_min=session_min)
            if reason:
                ended = _end_unlocked(
                    state, "bedtime", now, reason,
                    scene_id=str(bedtime.get("id") or "") or None)
                await _save_state(storage, state)
                return {"scene": None, "immediate": False,
                        "handled": not is_configured_scene_app,
                        "ended": copy.deepcopy(ended)}

        if is_configured_scene_app:
            await _save_state(storage, state)
            return {"scene": copy.deepcopy(bedtime), "immediate": False,
                    "handled": False}

        if event == "open" and not body.get("is_chat_app"):
            scene = _start_or_update_unlocked(
                state, scene_type="bedtime", now=now,
                conversation_id=bedtime.get("conversation_id"),
                source="phone_event",
                facts=[f"说过晚安后，{device} 又打开了 {app_name}"],
                cadence_min=15, duration_min=8 * 60, immediate=True,
                title="说过晚安后仍在玩")
            await _save_state(storage, state)
            return {"scene": copy.deepcopy(scene), "immediate": (_parse_dt(scene.get("next_wake_at")) or now) <= now,
                    "handled": True}

        await _save_state(storage, state)
        return {"scene": copy.deepcopy(bedtime), "immediate": False,
                "handled": True}


async def handle_presence_event(storage: Any, body: dict[str, Any],
                                result: dict[str, Any], *, device_state=None) -> dict[str, Any]:
    """Feed semantic presence transitions/proofs into confirmed bedtime."""
    if not result.get("stored"):
        return {"ended": []}
    now = _parse_dt(result.get("received_at")) or _now(storage)
    device = str(body.get("device") or "unknown").lower()
    source = str(body.get("source") or "presence").lower()
    semantic = result.get("semantic") if isinstance(result.get("semantic"), dict) else {}
    try:
        await check_wake_conditions(storage, presence_event={**result, "device": device},
                                    device_state=device_state)
    except Exception:
        logger.warning("presence wake condition failed", exc_info=True)
    async with storage.state_lock:
        state = await _load_state(storage)
        bedtime = _find_active(state, "bedtime")
        if bedtime is None:
            return {"ended": []}
        _ensure_bedtime_fields(bedtime)
        if not bedtime.get("sleep_confirmed_at"):
            return {"ended": []}
        activity = bedtime["wake_activity"]
        channels = activity.setdefault("channels", {})
        channel_key = f"{device}:{source}"
        channel = channels.get(channel_key)
        if not isinstance(channel, dict):
            channel = {"active": False, "app": None, "started_at": None,
                       "last_proof_at": None}
        active = bool(semantic.get("active"))
        last_proof = _parse_dt(channel.get("last_proof_at"))
        if active:
            if (not channel.get("active") or last_proof is None
                    or (now - last_proof).total_seconds() > _ACTIVE_PROOF_MAX_GAP_S):
                channel["started_at"] = _iso(now)
            channel["active"] = True
            channel["app"] = semantic.get("app")
            channel["last_proof_at"] = _iso(now)
        else:
            channel.update({"active": False, "app": semantic.get("app"),
                            "started_at": None, "last_proof_at": _iso(now)})
        channels[channel_key] = channel

        if result.get("changed") or result.get("revived"):
            effective = str(semantic.get("effective_state") or "unknown")
            app = str(semantic.get("app") or "")
            _record_wake_action_unlocked(
                bedtime, now=now,
                activity_id=str(result.get("activity_id") or
                                f"presence:{device}:{source}:{_iso(now)}"),
                device=device, source=source,
                kind="device_revived" if result.get("revived") else "presence_transition",
                app=app, semantic_key=f"{device}:{effective}:{app}")
        reason = _wake_activity_end_reason(bedtime, now)
        if reason:
            ended = _end_unlocked(
                state, "bedtime", now, reason,
                scene_id=str(bedtime.get("id") or "") or None)
            await _save_state(storage, state)
            return {"ended": copy.deepcopy(ended)}
        bedtime["updated_at"] = _iso(now)
        await _save_state(storage, state)
        return {"ended": []}


def _start_or_update_unlocked(
    state: dict[str, Any], *, scene_type: str, now: datetime,
    conversation_id: Optional[str] = None, source: str = "model",
    source_message_id: Optional[str] = None, facts: Any = None,
    nudge: str = "", cadence_min: Any = None, duration_min: Any = None,
    immediate: bool = False, title: str = "",
) -> dict[str, Any]:
    if scene_type not in _SCENE_TYPES:
        scene_type = "custom"
    if source in {"model", "master_tool", "slm"}:
        state.setdefault("recreation_pauses", {}).pop(scene_type, None)
    defaults = _SCENE_DEFAULTS[scene_type]
    cadence = _clamp_int(cadence_min, defaults["cadence_min"], 5, 240)
    duration = _clamp_int(duration_min, defaults["duration_min"], 10, 24 * 60)
    scene = _find_active(state, scene_type, conversation_id)
    preserve_rhythm = bool(scene and source not in {"model", "master_tool", "slm"}
                           and (scene.get("cadence_by_master") or scene.get("wake_condition")))
    clean_facts = _stamp_facts(facts, now)
    nudge_text = " ".join(str(nudge or "").split())[:800]
    # 便条槽位只留给助手:设备事件与 SLM 递进来的 nudge 一律不落盘。
    may_nudge = bool(nudge_text) and source in _NUDGE_SOURCES
    if scene is None:
        scene = {
            "id": uuid.uuid4().hex[:10],
            "type": scene_type,
            "title": (title or scene_type)[:80],
            "status": "active",
            "source": source,
            "source_message_id": source_message_id,
            "conversation_id": conversation_id,
            "started_at": _iso(now),
            "created_at": _iso(now),
            "facts": clean_facts,
            "nudge": nudge_text if may_nudge else "",
            "nudge_at": _iso(now) if may_nudge else None,
            "nudge_source": source if may_nudge else None,
            "nudge_wakes_left": _NUDGE_WAKES if may_nudge else 0,
            "cadence_min": cadence,
            "priority": defaults["priority"],
            "wake_attempts": 0,
            "real_wakes": 0,
            "notifications": 0,
        }
        _ensure_bedtime_fields(scene)
        state.setdefault("scenes", []).append(scene)
    else:
        _ensure_bedtime_fields(scene)
        scene["facts"] = _clean_facts(list(scene.get("facts") or []) + clean_facts)
        if may_nudge:
            write_nudge(scene, nudge_text, now, source=source)
        # 只有明确给出新节奏才覆盖;增量没带 cadence 时保住已约定的节奏
        if cadence_min is not None and not preserve_rhythm:
            scene["cadence_min"] = cadence
        else:
            cadence = _clamp_int(scene.get("cadence_min"), defaults["cadence_min"], 5, 240)
        if conversation_id:
            scene["conversation_id"] = conversation_id
        if source_message_id:
            scene["source_message_id"] = source_message_id
        scene["source"] = source or scene.get("source")
    if cadence_min is not None and not preserve_rhythm:
        scene["cadence_by_master"] = source in {"model", "master_tool"}
    scene.setdefault("cadence_by_master", False)
    scene["updated_at"] = _iso(now)
    scene["expires_at"] = _iso(now + timedelta(minutes=duration))
    next_at = now if immediate else now + timedelta(minutes=cadence)
    current_next = _parse_dt(scene.get("next_wake_at"))
    if not preserve_rhythm and (current_next is None or next_at < current_next or immediate):
        scene["next_wake_at"] = _iso(next_at)
    return scene


def _end_unlocked(state: dict[str, Any], scene_type: str, now: datetime,
                  reason: str = "", scene_id: Optional[str] = None,
                  defer_ids: Optional[set] = None) -> list[dict[str, Any]]:
    ended: list[dict[str, Any]] = []
    for scene in state.get("scenes") or []:
        if not isinstance(scene, dict) or scene.get("status") != "active":
            continue
        if scene_id and scene.get("id") != scene_id:
            continue
        if not scene_id and scene.get("type") != scene_type:
            continue
        if defer_ids and scene.get("id") in defer_ids:
            scene["pending_end_reason"] = str(reason)[:240]
            continue
        scene.pop("wake_condition", None)
        scene.pop("pending_end_reason", None)
        scene["status"] = "ended"
        scene["ended_at"] = _iso(now)
        scene["end_reason"] = str(reason or "")[:240]
        scene["next_wake_at"] = None
        scene["updated_at"] = _iso(now)
        ended.append(scene)
    return ended


async def start_scene(storage, *, scene_type: str, conversation_id: Optional[str] = None,
                      source: str = "model", source_message_id: Optional[str] = None,
                      facts: Any = None, nudge: str = "", cadence_min: Any = None,
                      duration_min: Any = None, immediate: bool = False,
                      title: str = "", wake_condition: Any = ...,
                      device_state=None) -> Optional[dict[str, Any]]:
    async with storage.state_lock:
        state = await _load_state(storage)
        now = _now(storage)
        if source == "phone_event":
            until = _parse_dt(state["recreation_pauses"].get(scene_type))
            if until is not None and now < until:
                return None
            if (scene_type in _SNOOZABLE_DEVICE_SCENES
                    and await _device_scenes_snoozed(storage, now)):
                return None
        scene = _start_or_update_unlocked(
            state, scene_type=scene_type, now=now, conversation_id=conversation_id,
            source=source, source_message_id=source_message_id, facts=facts,
            nudge=nudge, cadence_min=cadence_min, duration_min=duration_min,
            immediate=immediate, title=title)
        if wake_condition is not ...:
            if isinstance(wake_condition, dict) and (wake_condition.get("rule") or {}).get("kind") == "device_active":
                snapshot = device_state if isinstance(device_state, dict) else {}
                state["condition_activity"] = _snapshot_device_activity(
                    now, snapshot.get("phone"), snapshot.get("presence"),
                    snapshot.get("previous_usage"))
            scene["wake_condition"] = wake_condition
        await _save_state(storage, state)
        return copy.deepcopy(scene)


async def end_scene(storage, *, scene_type: Optional[str] = None,
                    scene_id: Optional[str] = None, reason: str = "",
                    recreate_after_minutes: Optional[int] = None) -> dict[str, Any]:
    if (recreate_after_minutes is not None
            and (type(recreate_after_minutes) is not int
                 or not 0 <= recreate_after_minutes <= 120)):
        raise ValueError("recreate_after_minutes must be an integer from 0 to 120")
    if scene_type is not None and scene_type not in _SCENE_TYPES:
        raise ValueError("invalid scene_type")
    async with storage.state_lock:
        state = await _load_state(storage)
        if scene_id:
            target = next((s for s in state["scenes"]
                           if isinstance(s, dict) and s.get("id") == scene_id), None)
            if target is None:
                raise ValueError("scene_id not found")
            if scene_type is not None and target.get("type") != scene_type:
                raise ValueError("scene_id and scene_type do not match")
            scene_type = target.get("type")
        scene_type = scene_type or "custom"
        if scene_type not in _SCENE_TYPES:
            raise ValueError("invalid scene_type")
        now = _now(storage)
        ended = _end_unlocked(state, scene_type, now, reason, scene_id)
        result = {}
        if recreate_after_minutes is not None:
            pauses = state["recreation_pauses"]
            until = (_iso(now + timedelta(minutes=recreate_after_minutes))
                     if recreate_after_minutes else None)
            if until:
                pauses[scene_type] = until
            else:
                pauses.pop(scene_type, None)
            result = {"scene_type": scene_type,
                      "recreate_after_at": _fmt_bj(until),
                      "recreate_after_minutes": recreate_after_minutes}
        if ended or recreate_after_minutes is not None:
            await _save_state(storage, state)
        return {**result, "ended": [_bj_view(s) for s in ended]}


async def update_scene_settings(
    storage, *, scene_id: Optional[str] = None, scene_type: Optional[str] = None,
    facts: Any = None, nudge: str = "", cadence_min: Any = None,
    duration_min: Any = None, title: str = "", wake_condition: Any = ...,
    device_state=None,
) -> Optional[dict[str, Any]]:
    async with storage.state_lock:
        state = await _load_state(storage)
        target = None
        for scene in reversed(state.get("scenes") or []):
            if not isinstance(scene, dict) or scene.get("status") != "active":
                continue
            if scene_id and str(scene.get("id")) == str(scene_id):
                target = scene
                break
            if not scene_id and scene_type and scene.get("type") == scene_type:
                target = scene
                break
        if target is None:
            return None
        now = _now(storage)
        clean = _stamp_facts(facts, now)
        if clean:
            target["facts"] = _clean_facts(list(target.get("facts") or []) + clean)
        # 这个入口没有 source 参数,只被助手工具调用,来源写死助手。
        nudge_text = " ".join(str(nudge or "").split())[:800]
        if nudge_text:
            write_nudge(target, nudge_text, now)
        if title:
            target["title"] = str(title)[:80]
        if cadence_min is not None:
            cadence = _clamp_int(cadence_min, int(target.get("cadence_min") or 30), 5, 240)
            target["cadence_min"] = cadence
            target["cadence_by_master"] = True
            target["next_wake_at"] = _iso(now + timedelta(minutes=cadence))
        if duration_min is not None:
            duration = _clamp_int(duration_min, 180, 10, 24 * 60)
            target["expires_at"] = _iso(now + timedelta(minutes=duration))
        if wake_condition is not ...:
            if isinstance(wake_condition, dict) and (wake_condition.get("rule") or {}).get("kind") == "device_active":
                snapshot = device_state if isinstance(device_state, dict) else {}
                state["condition_activity"] = _snapshot_device_activity(
                    now, snapshot.get("phone"), snapshot.get("presence"),
                    snapshot.get("previous_usage"))
            target["wake_condition"] = wake_condition
        target["updated_at"] = _iso(now)
        await _save_state(storage, state)
        return copy.deepcopy(target)


async def get_companion_status(storage) -> dict[str, Any]:
    state = await _load_state(storage)
    now = _now(storage)
    return {
        "version": state.get("version"),
        "revision": state.get("revision"),
        "recreation_pauses": {
            kind: _fmt_bj(until) for kind, until in state["recreation_pauses"].items()
            if kind in _SCENE_TYPES and (_parse_dt(until) or now) > now
        },
        "active_scenes": [_bj_view(s) for s in _active_from_state(state, now)],
        "recent_scenes": [_bj_view(s) for s in list(state.get("scenes") or [])[-10:]],
        "last_classifier": copy.deepcopy(state.get("last_classifier")),
    }


async def process_user_message_scene(storage, slm, *, conversation_id: str,
                                     message_id: str, user_text: str,
                                     classify: bool = True) -> dict[str, Any]:
    """Classify one user message and apply scene deltas. Failures never block chat."""
    text = " ".join(str(user_text or "").split())
    if not text:
        return {"deltas": [], "reason": "empty_message"}
    state = await _load_state(storage)
    if message_id in (state.get("processed_message_ids") or []):
        return {"deltas": [], "reason": "already_processed"}

    # Once sleep has actually been confirmed, any new user message is
    # deterministic wake evidence.  Persist this before the SLM call so a
    # timeout/unavailable classifier cannot leave a stale bedtime behind.
    pre_applied: list[dict[str, Any]] = []
    returned_after_sleep = False
    async with storage.state_lock:
        state = await _load_state(storage)
        for scene in _active_from_state(state, _now(storage)):
            _ensure_bedtime_fields(scene)
            if scene.get("type") == "bedtime" and scene.get("sleep_confirmed_at"):
                ended = _end_unlocked(
                    state, "bedtime", _now(storage), "user_returned_after_sleep",
                    scene_id=str(scene.get("id") or "") or None)
                if ended:
                    returned_after_sleep = True
                    pre_applied.append({"action": "end", "scenes": copy.deepcopy(ended)})
        if returned_after_sleep:
            await _save_state(storage, state)
    if not classify:
        return {"deltas": pre_applied,
                "reason": "user_returned_after_sleep;classification_skipped"}
    if slm is None:
        return {"deltas": pre_applied,
                "reason": "user_returned_after_sleep;slm_unavailable"}

    state = await _load_state(storage)
    active = _active_from_state(state, _now(storage))
    compact = [{"id": s.get("id"), "type": s.get("type"),
                "facts": s.get("facts"), "started_at": s.get("started_at")}
               for s in active]
    try:
        raw = await asyncio.wait_for(
            slm(
                system=_SCENE_CLASSIFIER_SYSTEM,
                user_content=("【当前 Scene】\n" + _json(compact)
                              + "\n\n【当前北京时间】\n"
                              + _now(storage).astimezone(_TZ).isoformat(timespec="minutes")
                              + "\n\n【用户最新消息】\n" + text[:4000]),
                max_tokens=1200, temperature=0.0),
            timeout=15.0)
    except Exception as exc:
        logger.warning("companion scene classifier failed: %s", exc)
        return {"deltas": [], "reason": "classifier_failed"}

    deltas = raw.get("deltas") if isinstance(raw, dict) else []
    if not isinstance(deltas, list):
        deltas = []
    applied: list[dict[str, Any]] = list(pre_applied)
    now = _now(storage)
    async with storage.state_lock:
        state = await _load_state(storage)
        if message_id in (state.get("processed_message_ids") or []):
            return {"deltas": [], "reason": "already_processed"}
        # 用户已经回到对话：所有 Scene 记下回复；纯粹依赖“用户没有回应”的
        # Scene 当场完成，其他 Scene 仍交给语义 delta 决定去留。
        for scene in _active_from_state(state, now):
            scene["last_user_reply_at"] = _iso(now)
            scene["unanswered_streak"] = 0
            if scene.get("type") in {"active_without_chat", "silence_escalation", "morning"}:
                scene.pop("wake_condition", None)
                scene["status"] = "ended"
                scene["ended_at"] = _iso(now)
                scene["end_reason"] = "user_returned_to_chat"
                scene["next_wake_at"] = None
            scene["updated_at"] = _iso(now)
        for delta in deltas[:3]:
            if not isinstance(delta, dict):
                continue
            action = str(delta.get("action") or "").lower()
            scene_type = str(delta.get("scene_type") or "custom").lower()
            if scene_type not in _SCENE_TYPES:
                scene_type = "custom"
            # The message that deterministically ended a confirmed sleep
            # episode cannot reopen bedtime merely because it also says
            # "still tired".
            if returned_after_sleep and scene_type == "bedtime" and action in {"start", "update"}:
                continue
            if action in {"start", "update"}:
                scene = _start_or_update_unlocked(
                    state, scene_type=scene_type, now=now,
                    conversation_id=conversation_id, source="slm",
                    source_message_id=message_id, facts=delta.get("facts"),
                    cadence_min=delta.get("cadence_min"),
                    duration_min=delta.get("duration_min"), immediate=False)
                applied.append({"action": action, "scene": copy.deepcopy(scene)})
            elif action == "end":
                ended = _end_unlocked(
                    state, scene_type, now,
                    str(delta.get("reason") or "user_message"))
                if ended:
                    applied.append({"action": "end", "scenes": copy.deepcopy(ended)})
        state.setdefault("processed_message_ids", []).append(message_id)
        state["last_classifier"] = {
            "at": _iso(now), "message_id": message_id,
            "conversation_id": conversation_id,
            "reason": str(raw.get("reason") or "")[:300],
            "applied": len(applied),
        }
        await _save_state(storage, state)
    return {"deltas": applied, "reason": str(raw.get("reason") or "")[:300]}


def _fmt_age(raw: Any, now: datetime) -> str:
    dt = _parse_dt(raw)
    if dt is None:
        return "未知"
    minutes = max(0, int((now - dt).total_seconds() // 60))
    if minutes < 60:
        return f"{minutes} 分钟"
    hours, minutes = divmod(minutes, 60)
    return f"{hours} 小时 {minutes} 分钟" if minutes else f"{hours} 小时"


def _render_scenes(scenes: list[dict[str, Any]], now: datetime, *, compact: bool) -> str:
    if not scenes:
        return ""
    lines = ["<companion_scene>", "【用户把门留给你】", _STANDING_INVITATION, ""]
    for index, scene in enumerate(scenes[:4]):
        scene_type = str(scene.get("type") or "custom")
        label = str(scene.get("title") or scene_type)
        lines.append(f"【当前情境{'' if index == 0 else '（同时）'}：{label}】")
        lines.append(f"已持续：{_fmt_age(scene.get('started_at'), now)}")
        facts = _clean_facts(scene.get("facts"))
        if facts:
            lines.append("眼下已知：" + "；".join(facts[-4:]))
        nudge = str(scene.get("nudge") or "").strip()
        if nudge:
            marked = _fmt_hm(scene.get("nudge_at"))
            lines.append(f"【于{marked}记下】{nudge}" if marked else nudge)
        else:
            lines.append(_SCENE_GUIDANCE.get(scene_type, _SCENE_GUIDANCE["custom"]))
        next_at = _parse_dt(scene.get("next_wake_at"))
        if next_at:
            lines.append("下一次 collar 探视：" + next_at.astimezone(_TZ).isoformat(timespec="minutes"))
        if not compact:
            lines.append(
                "请依据当前报告中已提供的用户状态与设备信息作判断；"
                "如需查看或调整情境，请使用 collar_companion；需要主动联系用户时，请使用 notify_user。")
        lines.append("")
    lines.append("这些信息用于保持情境连续性。后续行动由当前上下文与判断决定。")
    lines.append("</companion_scene>")
    return "\n".join(lines)


async def render_active_scene_context(storage, *, compact: bool = False) -> str:
    state = await _load_state(storage)
    return _render_scenes(_active_from_state(state, _now(storage)), _now(storage), compact=compact)


def _sleep_window(state: dict[str, Any]) -> tuple[int, int]:
    start, end = _SLEEP_WINDOW_DEFAULT
    raw = state.get("sleep_window")
    if isinstance(raw, dict):
        start = _clamp_int(raw.get("start"), start, 0, 23)
        end = _clamp_int(raw.get("end"), end, 0, 23)
    elif isinstance(raw, (list, tuple)) and len(raw) == 2:
        start = _clamp_int(raw[0], start, 0, 23)
        end = _clamp_int(raw[1], end, 0, 23)
    elif isinstance(raw, str) and "-" in raw:
        head, _, tail = raw.partition("-")
        start = _clamp_int(head, start, 0, 23)
        end = _clamp_int(tail, end, 0, 23)
    return start, end


def _in_sleep_window(now: datetime, window: tuple[int, int]) -> bool:
    start, end = window
    if start == end:
        # 退化配置按"没有夜间窗口"处理,宁可不压制。
        return False
    hour = now.astimezone(_TZ).hour
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


async def _user_asleep(storage, now: Optional[datetime] = None, *, device_state=None) -> bool:
    """Probe sleep quietly.

    Night-time suppression keeps the original strict four-leg rule.  An active
    bedtime Scene may be confirmed at any hour (nap support); in that scoped
    case stale/missing HR is neutral while a fresh high sample still vetoes.

    只读不写。调用方在锁外算好这个布尔再传进 claim,免得把设备与健康的
    文件 IO 拖进 _STATE_LOCK 里。
    """
    now = now or _now(storage)
    try:
        state = await _load_state(storage)
        active_bedtime = any(
            scene.get("type") == "bedtime"
            for scene in _active_from_state(state, now))
        if not active_bedtime and not _in_sleep_window(now, _sleep_window(state)):
            # 普通白天静默不压制其它 Scene；只有 active bedtime 能在任意
            # 时刻开启午睡探针。morning 自己的时间门完全独立。
            return False
        threshold = _clamp_int(
            state.get("hr_sleep_threshold"), _HR_SLEEP_THRESHOLD_DEFAULT, 30, 120)

        snapshot = device_state if isinstance(device_state, dict) else {}
        phone = snapshot.get("phone")
        phone = phone if isinstance(phone, dict) else None
        presence = snapshot.get("presence")
        presence = presence if isinstance(presence, dict) else None

        # 第一条腿:Mac 关着、锁着或已经闲置半小时。
        mac = ((presence or {}).get("devices") or {}).get("macbook")
        if not isinstance(mac, dict):
            return False
        idle_raw = mac.get("idle_seconds")
        try:
            idle_s = float(idle_raw) if idle_raw is not None else None
        except (TypeError, ValueError):
            idle_s = None
        if not ((not mac.get("online")) or bool(mac.get("screen_locked"))
                or (idle_s is not None and idle_s >= _MAC_IDLE_SLEEP_S)):
            return False

        # 第二条腿:手机/平板手上没有前台 app,且最后一次事件已过半小时。
        # current_app 是按设备的 dict,只有收到同名 app 的 close 才会被清掉,
        # 丢一条 close 就留下一条永不消失的孤儿 session;collar_phone 超过封顶
        # 时长会给它打 stale。这里照 collar_presence 的口径把 stale 条目当作
        # 已经散场——否则一条孤儿就能让整套睡着压制永远不成立,而且毫无声响。
        if phone is None:
            return False
        current = phone.get("current_app")
        if current and not isinstance(current, dict):
            return False                      # 认不出的格式一律按活跃处理
        holding = [held for held in (current or {}).values()
                   if held and not (isinstance(held, dict) and held.get("stale"))]
        if holding:
            if not active_bedtime:
                return False
            # Bedtime follows fresh activity rather than a close event that may
            # never arrive.  A held app with no new event for 30 minutes is an
            # orphan for sleep-confirmation purposes; a recent one still keeps
            # user awake.
            for held in holding:
                if not isinstance(held, dict):
                    return False
                minutes = held.get("minutes")
                try:
                    if minutes is not None and float(minutes) < _BEDTIME_QUIET_MIN:
                        return False
                except (TypeError, ValueError):
                    return False
                since = _parse_dt(held.get("since"))
                if minutes is None and (since is None or
                        (now - since) < timedelta(minutes=_BEDTIME_QUIET_MIN)):
                    return False
        last_event = phone.get("last_event")
        age_min = last_event.get("age_min") if isinstance(last_event, dict) else None
        if isinstance(age_min, bool) or not isinstance(age_min, (int, float)):
            return False
        if age_min < _SLEEP_SILENCE_MIN:
            return False

        # 第三项条件：用户已经半小时没有发言。
        last_user = await _last_user_message_at(storage)
        if last_user is None or (now - last_user) < timedelta(minutes=_SLEEP_SILENCE_MIN):
            return False

        # 第四条腿:手表最近的心率落进睡眠低位。
        samples = snapshot.get("health_samples") or []
        fresh = []
        for sample in samples:
            if not isinstance(sample, dict) or sample.get("type") != "heart_rate":
                continue
            at = _parse_dt(sample.get("at"))
            try:
                value = float(sample.get("value"))
            except (TypeError, ValueError):
                continue
            if at and timedelta(0) <= now - at <= timedelta(minutes=40):
                fresh.append((at, value))
        if not fresh:
            return active_bedtime
        return max(fresh, key=lambda row: row[0])[1] <= threshold
    except Exception:
        logger.warning("collar_companion: sleep probe failed", exc_info=True)
        return False


async def _device_scenes_snoozed(storage, now: datetime) -> bool:
    state = await storage.get_setting("collar_wake")
    until = _parse_dt(state.get("snooze_until")) if isinstance(state, dict) else None
    return until is not None and now < until


async def revalidate_scene_wake(storage, bundle: dict[str, Any]) -> bool:
    """Final submission check; mutate the shared bundle so completion matches it."""
    async with storage.state_lock:
        state = await _load_state(storage)
        now = _now(storage)
        snoozed = await _device_scenes_snoozed(storage, now)
        live_ids = {s["id"] for s in _active_from_state(state, now)}
        def allowed(scene):
            return (scene.get("type") not in _SNOOZABLE_DEVICE_SCENES
                    or (not snoozed and scene.get("id") in live_ids))
        due = [s for s in bundle.get("due") or [] if allowed(s)]
        bundle["due"] = due
        bundle["active"] = [s for s in bundle.get("active") or [] if allowed(s)]
        bundle["primary"] = due[0] if due else None
        bundle["conversation_id"] = due[0].get("conversation_id") if due else None
        return bool(due)


async def claim_due_scene_wake(storage, now: Optional[datetime] = None,
                               asleep: bool = False) -> Optional[dict[str, Any]]:
    """Atomically claim all currently due scenes and advance their recurrence."""
    now = now or _now(storage)
    async with storage.state_lock:
        state = await _load_state(storage)
        changed = False
        for scene in state.get("scenes") or []:
            if not isinstance(scene, dict) or scene.get("status") != "active":
                continue
            expires = _parse_dt(scene.get("expires_at"))
            if expires is not None and expires <= now:
                scene.pop("wake_condition", None)
                scene.pop("pending_end_reason", None)
                scene["status"] = "expired"
                scene["ended_at"] = _iso(now)
                scene["next_wake_at"] = None
                changed = True
        active = _active_from_state(state, now)

        # Persist the first active-bedtime sleep episode and claim its single
        # wake immediately.  This special claim ignores ordinary next_wake_at
        # and contains no other Scene; all normal sleep suppression stays put.
        bedtimes = [scene for scene in active if scene.get("type") == "bedtime"]
        for scene in bedtimes:
            _ensure_bedtime_fields(scene)
        if asleep and bedtimes:
            target = sorted(
                bedtimes, key=lambda s: str(s.get("started_at") or ""))[-1]
            if not target.get("sleep_confirmed_at"):
                target["sleep_confirmed_at"] = _iso(now)
                target["asleep_wake_status"] = "pending"
                target["asleep_wake_retry_at"] = _iso(now)
                target["asleep_wake_claim_id"] = None
                target["asleep_wake_claimed_at"] = None
                target["wake_activity"] = _empty_wake_activity()
                target["facts"] = _clean_facts(
                    list(target.get("facts") or [])
                    + _stamp_facts(["设备与对话已连续安静，确认用户睡着"], now))
                target["updated_at"] = _iso(now)
                changed = True

        special = None
        for scene in sorted(
                bedtimes, key=lambda s: str(s.get("started_at") or ""),
                reverse=True):
            status = str(scene.get("asleep_wake_status") or "idle")
            if status == "claimed":
                claimed_at = _parse_dt(scene.get("asleep_wake_claimed_at"))
                if claimed_at and (now - claimed_at).total_seconds() >= _ASLEEP_WAKE_LEASE_S:
                    scene["asleep_wake_status"] = "pending"
                    scene["asleep_wake_retry_at"] = _iso(now)
                    scene["asleep_wake_claim_id"] = None
                    scene["asleep_wake_claimed_at"] = None
                    status = "pending"
                    changed = True
            retry_at = _parse_dt(scene.get("asleep_wake_retry_at"))
            if status == "pending" and (retry_at is None or retry_at <= now):
                special = scene
                break

        if special is not None and not any((s.get("wake_condition") or {}).get("met_at") for s in active):
            claim_id = uuid.uuid4().hex
            special["asleep_wake_status"] = "claimed"
            special["asleep_wake_claim_id"] = claim_id
            special["asleep_wake_claimed_at"] = _iso(now)
            special["asleep_wake_attempts"] = int(
                special.get("asleep_wake_attempts") or 0) + 1
            special["updated_at"] = _iso(now)
            await _save_state(storage, state)
            active_view = _active_from_state(state, now)
            special_view = copy.deepcopy(special)
            return {
                "claimed_at": _iso(now),
                "wake_reason": "bedtime_asleep_confirmed",
                "asleep_claim": {
                    "scene_id": special.get("id"), "claim_id": claim_id,
                    "confirmed_at": special.get("sleep_confirmed_at"),
                },
                "primary": special_view,
                "due": [special_view],
                "active": copy.deepcopy(active_view),
                "conversation_id": special.get("conversation_id"),
            }

        snoozed = await _device_scenes_snoozed(storage, now)
        due = [s for s in active
               if (_parse_dt(s.get("next_wake_at")) or now) <= now
               and not (snoozed and s.get("type") in _SNOOZABLE_DEVICE_SCENES)]
        if not due:
            if changed:
                await _save_state(storage, state)
            return None
        if asleep:
            # 用户处于睡眠状态：本轮整体顺延，不 claim，也不计 wake_attempts。
            # 会在顺延期内到期的场景连寿命一起挪 —— 睡着的时间不烧场景寿命,
            # 用户恢复活动后，sulking 情境仍然有效。
            for scene in due:
                if (scene.get("wake_condition") or {}).get("met_at"):
                    continue
                cadence = _clamp_int(
                    scene.get("cadence_min"),
                    _SCENE_DEFAULTS.get(str(scene.get("type")), _SCENE_DEFAULTS["custom"])["cadence_min"],
                    5, 240)
                deferred = now + timedelta(minutes=cadence)
                expires = _parse_dt(scene.get("expires_at"))
                if expires is not None and expires <= deferred:
                    scene["expires_at"] = _iso(expires + (deferred - now))
                scene["next_wake_at"] = _iso(deferred)
                scene["sleep_suppressed_count"] = int(
                    scene.get("sleep_suppressed_count") or 0) + 1
                scene["updated_at"] = _iso(now)
            logger.info(
                "collar_companion: sleep_suppressed scenes=%d ids=%s",
                len(due), ",".join(str(s.get("id")) for s in due))
            await _save_state(storage, state)
            due = [s for s in due if (s.get("wake_condition") or {}).get("met_at")]
            if not due:
                return None
        due.sort(key=lambda s: not bool((s.get("wake_condition") or {}).get("met_at")))
        for scene in due:
            cadence = _clamp_int(
                scene.get("cadence_min"),
                _SCENE_DEFAULTS.get(str(scene.get("type")), _SCENE_DEFAULTS["custom"])["cadence_min"],
                5, 240)
            scene["last_attempt_at"] = _iso(now)
            scene["wake_attempts"] = int(scene.get("wake_attempts") or 0) + 1
            scene["next_wake_at"] = _iso(now + timedelta(minutes=cadence))
            scene["updated_at"] = _iso(now)
        await _save_state(storage, state)
        active = _active_from_state(state, now)
        return {
            "claimed_at": _iso(now),
            "primary": copy.deepcopy(due[0]),
            "due": copy.deepcopy(due),
            "active": copy.deepcopy(active),
            "conversation_id": due[0].get("conversation_id"),
        }


async def complete_scene_wake(storage, bundle: dict[str, Any], result: dict[str, Any]) -> None:
    special_claim = bundle.get("asleep_claim") if isinstance(bundle, dict) else None
    if isinstance(special_claim, dict):
        async with storage.state_lock:
            state = await _load_state(storage)
            now = max(_now(storage), _parse_dt(bundle.get("claimed_at")) or _now(storage))
            target = next(
                (scene for scene in state.get("scenes") or []
                 if isinstance(scene, dict)
                 and str(scene.get("id")) == str(special_claim.get("scene_id"))),
                None)
            if (isinstance(target, dict)
                    and target.get("asleep_wake_status") == "claimed"
                    and str(target.get("asleep_wake_claim_id") or "")
                    == str(special_claim.get("claim_id") or "")):
                if isinstance(result, dict) and result.get("ok"):
                    target["asleep_wake_status"] = "delivered"
                    target["asleep_wake_delivered_at"] = _iso(now)
                    target["asleep_wake_retry_at"] = None
                    target["asleep_wake_last_error"] = None
                else:
                    attempts = max(1, int(target.get("asleep_wake_attempts") or 1))
                    delay = _ASLEEP_WAKE_RETRY_MIN[
                        min(attempts - 1, len(_ASLEEP_WAKE_RETRY_MIN) - 1)]
                    target["asleep_wake_status"] = "pending"
                    target["asleep_wake_retry_at"] = _iso(
                        now + timedelta(minutes=delay))
                    if isinstance(result, dict):
                        target["asleep_wake_last_error"] = str(
                            result.get("error") or result.get("skipped") or
                            "wake_not_delivered")[:160]
                    else:
                        target["asleep_wake_last_error"] = "invalid_wake_result"
                target["asleep_wake_claim_id"] = None
                target["asleep_wake_claimed_at"] = None
                target["updated_at"] = _iso(now)
                await _save_state(storage, state)
    async with storage.state_lock:
        state = await _load_state(storage)
        for claimed in bundle.get("due") or []:
            scene = next((s for s in state["scenes"] if s.get("id") == claimed.get("id")), None)
            if not scene or scene.get("status") != "active":
                continue
            condition = claimed.get("wake_condition")
            if condition and condition.get("met_at") and scene.get("wake_condition") == condition:
                if isinstance(result, dict) and result.get("ok"):
                    scene.pop("wake_condition", None)
                else:
                    scene["next_wake_at"] = _iso(_now(storage) + timedelta(minutes=5))
            if claimed.get("pending_end_reason") and scene.get("pending_end_reason") == claimed["pending_end_reason"]:
                _end_unlocked(state, scene["type"], _now(storage), claimed["pending_end_reason"], scene_id=scene["id"])
        await _save_state(storage, state)
    if not isinstance(result, dict) or not result.get("ok"):
        return
    ids = {str(s.get("id")) for s in (bundle.get("due") or []) if isinstance(s, dict)}
    # 便条只扣"真被读到"的那一份:_render_scenes 渲染的就是 active 前 4 个,
    # Only consume note versions that were rendered in this wake report.
    substantive = bool(result.get("substantive", True))
    rendered = {str(s.get("id")) for s in (bundle.get("active") or [])[:4]
                if isinstance(s, dict)}
    async with storage.state_lock:
        state = await _load_state(storage)
        now = _now(storage)
        for scene in state.get("scenes") or []:
            scene_id = str(scene.get("id"))
            if scene_id in ids:
                scene["real_wakes"] = int(scene.get("real_wakes") or 0) + 1
                scene["last_wake_at"] = _iso(now)
                scene["updated_at"] = _iso(now)
            if (substantive and scene_id in rendered and scene.get("nudge")
                    and not result.get("notes_settled")):
                consume_nudge(scene)
                scene["updated_at"] = _iso(now)
        await _save_state(storage, state)


async def record_notification(storage, *, reason: str = "") -> None:
    """Record a proactive message without ending the current recurring scene."""
    last_user = await _last_user_message_at(storage)
    async with storage.state_lock:
        state = await _load_state(storage)
        scenes = _active_from_state(state, _now(storage))
        if not scenes:
            return
        primary = scenes[0]
        previous_notification = _parse_dt(primary.get("last_notification_at"))
        unanswered = bool(
            previous_notification
            and (last_user is None or last_user <= previous_notification))
        primary["notifications"] = int(primary.get("notifications") or 0) + 1
        primary["unanswered_streak"] = (
            int(primary.get("unanswered_streak") or 0) + 1 if unanswered else 1)
        primary["last_notification_at"] = _iso(_now(storage))
        if reason:
            primary["last_notification_reason"] = str(reason)[:240]
        # 持续没有回复时，让探视自然变密；明确 repeat 保持约定的精确节奏。
        if (unanswered and not primary.get("cadence_by_master") and primary.get("type") != "repeat"
                and int(primary.get("unanswered_streak") or 0) >= 2):
            current = _clamp_int(primary.get("cadence_min"), 30, 5, 240)
            # 吵架时哄得再勤也别短过十分钟,免得把哄变成刷屏。
            faster = max(10 if primary.get("type") == "sulking" else 5,
                         int(round(current * 0.7)))
            primary["cadence_min"] = faster
            primary["next_wake_at"] = _iso(_now(storage) + timedelta(minutes=faster))
            primary["facts"] = _clean_facts(
                list(primary.get("facts") or [])
                + _stamp_facts(
                    [f"助手连续 {primary['unanswered_streak']} 次主动联系仍未等到回复，探视节奏已加快到约 {faster} 分钟"],
                    _now(storage)))
        primary["updated_at"] = _iso(_now(storage))
        await _save_state(storage, state)


async def _last_user_message_at(storage) -> Optional[datetime]:
    return _parse_dt(await storage.latest_user_message_at())


def _normal_app(raw: Any) -> str:
    return str(raw or "").strip().lower().replace(" ", "")


async def handle_phone_event(storage: Any, body: dict[str, Any], result: dict[str, Any], *,
                             device_state=None, scene_apps=None) -> dict[str, Any]:
    matched = set()
    if storage is not None and result.get("stored"):
        try:
            matched = await check_wake_conditions(
                storage, event={**body, "at": (result.get("record") or {}).get("at")},
                device_state=device_state)
        except Exception:
            logger.warning("phone wake condition failed", exc_info=True)
    response = await _handle_phone_scene_event(storage, body, result,
                                               device_state=device_state, scene_apps=scene_apps)
    if matched:
        response["immediate"] = True
    return response


async def _handle_phone_scene_event(storage: Any, body: dict[str, Any], result: dict[str, Any], *,
                                    device_state=None, scene_apps=None) -> dict[str, Any]:
    """Translate exact app telemetry into scene signals and optional immediate wake."""
    if not result.get("stored"):
        return {"scene": None, "immediate": False}
    event = str(body.get("event") or "").lower()
    app_name = _normal_app(body.get("app"))
    device = str(body.get("device") or "iphone").lower()
    now = _now(storage)

    bedtime_result = await _handle_bedtime_phone_event(
        storage, body, result, now=now, scene_apps=scene_apps)
    if isinstance(bedtime_result, dict) and bedtime_result.get("handled"):
        return bedtime_result

    configured_scene = (scene_apps or {}).get(app_name)
    if configured_scene:
        scene_type = configured_scene if isinstance(configured_scene, str) else "private_play"
        if event == "open":
            scene = await start_scene(
                storage, scene_type=scene_type, source="phone_event",
                facts=[f"{device} app opened at {now.astimezone(_TZ).strftime('%H:%M')}"],
                cadence_min=10, duration_min=60, immediate=True)
            return {"scene": scene, "immediate": bool(scene and (_parse_dt(scene.get("next_wake_at")) or now) <= now)}
        if event == "close":
            scene = await start_scene(
                storage, scene_type=scene_type, source="phone_event",
                facts=[f"{device} app closed at {now.astimezone(_TZ).strftime('%H:%M')}"],
                cadence_min=10, duration_min=45, immediate=False)
            return {"scene": scene, "immediate": False}

    if event != "open":
        return {"scene": None, "immediate": False}

    # 脆弱点:get_companion_status 的时间字段已经是上海时间字符串(项 2),
    # 这里只取 conversation_id;将来要读时间必须回原始 state 拿 UTC。
    status = await get_companion_status(storage)
    active = status.get("active_scenes") or []

    # outing/work/repeat 等已有情境已经拥有自己的探视节奏；普通 app open
    # 不再额外叠一层 active_without_chat，避免工具提示被次级 Scene 淹没。
    if active:
        return {"scene": None, "immediate": False}

    last_user = await _last_user_message_at(storage)
    age_min = int((now - last_user).total_seconds() // 60) if last_user else 9999
    local_hour = now.astimezone(_TZ).hour
    if 5 <= local_hour < 12 and age_min >= 240:
        scene = await start_scene(
            storage, scene_type="morning", source="phone_event",
            facts=[f"早晨 {device} 打开了 {app_name}", f"用户已有约 {age_min} 分钟没来找助手"],
            cadence_min=10, duration_min=180, immediate=False,
            title="醒了还没来道早安")
        return {"scene": scene, "immediate": False}
    if age_min >= 30:
        scene = await start_scene(
            storage, scene_type="active_without_chat", source="phone_event",
            facts=[f"{device} 正在使用 {app_name}", f"距用户上次消息约 {age_min} 分钟"],
            cadence_min=30, duration_min=240, immediate=False,
            title="设备活跃但没来找助手")
        return {"scene": scene, "immediate": False}
    return {"scene": None, "immediate": False}


def _event_accuracy_m(event: dict[str, Any]) -> Optional[float]:
    """定位事件的水平精度(米);字段缺失或读不懂时返回 None(按放行处理)。"""
    icloud = event.get("icloud")
    raw = icloud.get("horizontal_accuracy_m") if isinstance(icloud, dict) else None
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _event_hour_shanghai(event: dict[str, Any]) -> Optional[int]:
    """定位事件的上海时间小时数;时间读不出来时返回 None。"""
    dt = _parse_dt(event.get("detected_at") or event.get("occurred_at") or event.get("at"))
    return dt.astimezone(_TZ).hour if dt else None


async def process_location_events(storage, *, location_events=None, known_places=None) -> list[dict[str, Any]]:
    """Apply new standardized location events once, preserving the persisted watermark."""
    events = [e for e in (location_events or []) if isinstance(e, dict)]
    if not events:
        return []
    applied = []
    async with storage.state_lock:
        state = await _load_state(storage)
        last = state.get("last_location_event")
        signatures = [(hashlib.sha256(_json(e).encode()).hexdigest()[:16], e) for e in events]
        if not last:
            state["last_location_event"] = signatures[-1][0]
            await _save_state(storage, state)
            return []
        found = False
        pending = []
        for sig, event in signatures:
            if not found:
                if sig == last:
                    found = True
                continue
            pending.append((sig, event))
        if not found:
            state["last_location_event"] = signatures[-1][0]
            await _save_state(storage, state)
            return []
        now = storage.now()
        places = known_places or {}
        for sig, event in pending:
            event = dict(event)
            verb = str(event.get("event") or event.get("kind") or "").lower()
            place = event.get("place")
            name = place if isinstance(place, str) else (place or {}).get("name")
            place_info = places.get(name, {}) if isinstance(name, str) else {}
            role = place_info.get("role")
            matched = _match_conditions(state, now, event=event)
            if verb == "arrive" and role == "home":
                ended = _end_unlocked(state, "outing", now, "arrived_home", defer_ids=matched)
                if ended: applied.append({"event": "arrived_home", "ended": len(ended)})
            if verb == "leave" and role == "work":
                ended = _end_unlocked(state, "work", now, "left_work", defer_ids=matched)
                if ended: applied.append({"event": "left_work", "ended": len(ended)})
            if verb == "leave" and role == "home":
                accuracy = _event_accuracy_m(event)
                hour = _event_hour_shanghai(event)
                if (accuracy is None or accuracy <= 100) and hour is not None and 6 <= hour < 14:
                    ended = _end_unlocked(state, "bedtime", now, "left_home", defer_ids=matched)
                    if ended: applied.append({"event": "left_home", "ended": len(ended)})
            state["last_location_event"] = sig
        await _save_state(storage, state)
    return applied


WAKE_WHEN_TRANSLATOR_SYSTEM = """把助手写的一句话翻译成一个固定格式的唤醒条件。只输出JSON，不写解释。

可选规则：
- arrive：用户到达地点，需要place。
- leave：用户离开地点，需要place。
- app_open：用户打开或正在使用某个App，但未说明时长，需要app。未说明时长时选app_open。
- app_use：用户连续使用某个App超过明确时长，需要app和minutes。
- device_active：设备安静一段时间后恢复活动，minutes表示此前至少安静多久，未说明时默认30。
- all_quiet：用户所有受监测设备均安静了minutes分钟。

字段约束：
- 返回成功对象时必须包含ok、kind、place、app、device、minutes、between。未使用的place、app、minutes或between填null；未知名称拒绝。
- device只能是iphone、ipad、macbook或any。未指定设备用any；手机对应iphone，平板对应ipad，电脑对应macbook。
- minutes必须是5–720的整数；不能从未说明的时长猜测一个数字。
- between只能为null或HH:MM-HH:MM，使用UTC+08:00本地时间，可跨午夜：早上06:00-12:00、下午12:00-18:00、晚上18:00-00:00、半夜00:00-06:00。

判断规则：
1. 只翻译用户要等待的那一个动作。背景叙述（例如先说晚安、人在外面或下班以后）只解释情境，不算第二个动作。
2. 一句话包含两个或更多待观察动作时拒绝，并说明一次只能等一件事。
3. 等待用户发消息、回消息或对话，或等待心率、情绪、天气等不支持的条件时拒绝。背景里提及用户已经说过的话不算等待消息。
4. 纯钟点或固定时长应拒绝，并说明应使用闹钟或cadence。
5. 拿不准时拒绝，不猜测。

成功输出示例：
{"ok":true,"kind":"app_use","place":null,"app":"chat-app","device":"iphone","minutes":20,"between":null}
失败输出示例：
{"ok":false,"reason":"一次只能等待一个动作"}"""

# A sentence is compiled once at the tool boundary; event evaluation is local.
_WAKE_SUPPORTED = ["用户到达{地点}", "用户离开{地点}", "用户在{设备}上打开{App}",
                   "用户在{设备}上连续使用{App}超过 N 分钟",
                   "用户的设备静默至少 N 分钟后重新开始使用", "用户所有设备均静默 N 分钟"]

_DEVICE_LABELS = {"iphone": "iPhone", "ipad": "iPad", "macbook": "Mac", "any": "任意设备"}

TOOL_SCHEMA = {
    "name": "collar_companion",
    "description": "Read and manage scenes, scene notes, alarms, repeat cadence, snooze, and bedtime settings.",
    "inputSchema": {
        "type": "object", "additionalProperties": False,
        "properties": {
            "action": {"type": "string", "enum": [
                "status", "start_scene", "update_scene", "end_scene", "set_repeat",
                "set_rhythm", "set_alarm", "clear_alarm", "snooze", "goodnight",
                "set_interval", "set_note", "clear_note",
            ]},
            "scene_type": {"type": "string", "enum": sorted(_SCENE_TYPES)},
            "scene_id": {"type": "string", "description": "Required for set_note and clear_note"},
            "title": {"type": "string", "maxLength": 80},
            "note": {"type": "string", "maxLength": 2000,
                     "description": "set_note requires non-empty text, 1-800 characters"},
            "facts": {"type": "array", "items": {"type": "string", "maxLength": 2000}},
            "nudge": {"type": "string", "maxLength": 800},
            "cadence_min": {"type": "integer", "minimum": 5, "maximum": 240},
            "duration_min": {"type": "integer", "minimum": 10, "maximum": 1440},
            "wake_when": {"type": "string", "maxLength": 500},
            "recreate_after_minutes": {"type": "integer", "minimum": 0, "maximum": 120},
            "at": {"type": "string"},
            "minutes": {
                "type": "integer", "minimum": 5, "maximum": 720,
                "description": "set_interval accepts 5-240; snooze accepts 5-720",
            },
            "alarm_id": {"type": "string"},
        },
        "required": ["action"],
    },
}


def _condition_vocabulary(now, *, known_places=None, known_apps=None):
    return known_places or {}, list(known_apps or [])


def _validate_condition(raw, places, apps):
    if not isinstance(raw, dict) or raw.get("ok") is not True:
        raise ValueError(str(raw.get("reason") or "条件翻译失败，请换个说法或稍后再试") if isinstance(raw, dict) else "条件翻译失败，请换个说法或稍后再试")
    rule = {k: raw.get(k) for k in ("kind", "place", "app", "device", "minutes", "between")}
    kind = rule["kind"]
    if kind not in {"arrive", "leave", "app_open", "app_use", "device_active", "all_quiet"}:
        raise ValueError("条件翻译失败，请换个说法或稍后再试")
    rule["device"] = rule["device"] or "any"
    if rule["device"] not in _DEVICE_LABELS:
        raise ValueError("未知设备")
    if kind in {"arrive", "leave"} and rule["place"] not in places:
        raise ValueError("找不到这个地点")
    if kind in {"app_open", "app_use"}:
        if rule["app"] not in apps:
            raise ValueError("找不到这个 App")
        if rule["device"] == "macbook":
            raise ValueError("Mac 没有可靠的 App 打开或连续使用记录")
    if kind == "device_active" and rule["minutes"] is None:
        rule["minutes"] = 30
    minutes = rule["minutes"]
    if (minutes is not None and (type(minutes) is not int or not 5 <= minutes <= 720)
            or kind in {"app_use", "device_active", "all_quiet"} and minutes is None):
        raise ValueError("minutes 必须为 5–720 的整数")
    between = rule["between"]
    if between is not None:
        if not isinstance(between, str) or not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d-(?:[01]\d|2[0-3]):[0-5]\d", between):
            raise ValueError("between 必须为 HH:MM-HH:MM")
        if between[:5] == between[6:]:
            raise ValueError("between 起止时间不能相同")
    return rule


def _condition_summary(rule, places):
    kind, minutes = rule["kind"], rule["minutes"]
    device = _DEVICE_LABELS[rule["device"]]
    label = (places.get(rule["place"]) or {}).get("label") or rule["place"]
    if kind == "arrive": text = f"用户到达{label}"
    elif kind == "leave": text = f"用户离开{label}"
    elif kind == "app_open": text = f"用户在{device}上打开{rule['app']}"
    elif kind == "app_use": text = f"用户在{device}上连续使用{rule['app']}超过 {minutes} 分钟"
    elif kind == "device_active":
        text = f"用户的{'设备' if rule['device'] == 'any' else device}静默至少 {minutes} 分钟后重新开始使用"
    else: text = f"用户所有设备均静默 {minutes} 分钟"
    if rule["between"]:
        text += f"（只在 {rule['between'][:5]}–{rule['between'][6:]} 之间算数）"
    return text


async def translate_wake_when(slm, text, *, known_places=None, known_apps=None, now=None):
    if not isinstance(text, str):
        raise ValueError("wake_when 必须为字符串")
    if text == "":
        return None
    try:
        if slm is None:
            raise RuntimeError("slm unavailable")
        now = now or datetime.now(timezone.utc)
        places, apps = _condition_vocabulary(now, known_places=known_places, known_apps=known_apps)
        user = ("【已知地点】\n" + "\n".join(f"{name}：{p.get('label') or name}" for name, p in places.items())
                + "\n\n【已知 App】\n" + "，".join(apps)
                + "\n\n【当前北京时间】\n" + now.astimezone(_TZ).isoformat()
                + "\n\n【assistant 写的条件】\n" + text)
        raw = await asyncio.wait_for(slm(
            system=WAKE_WHEN_TRANSLATOR_SYSTEM, user_content=user, max_tokens=300,
            temperature=0.0), timeout=15.0)
    except Exception:
        raise ValueError("条件翻译失败，请换个说法或稍后再试") from None
    rule = _validate_condition(raw, places, apps)
    return {"text": text, "rule": rule, "summary": _condition_summary(rule, places),
            "set_at": _iso(now), "met_at": None, "met_event": None}


def _condition_window(rule, now):
    window = rule.get("between")
    if not window:
        return True
    start, end = window.split("-")
    hm = now.astimezone(_TZ).strftime("%H:%M")
    return start <= hm < end if start < end else hm >= start or hm < end


def _match_conditions(state, now, *, event=None, phone=None, presence=None, activity=None):
    matched = set()
    for scene in _active_from_state(state, now):
        condition = scene.get("wake_condition")
        if not condition or condition.get("met_at"):
            continue
        rule = condition["rule"]
        event_at = _parse_dt((event or {}).get("detected_at") or (event or {}).get("at")) or now
        if event and event_at < (_parse_dt(condition.get("set_at")) or now):
            continue
        if not _condition_window(rule, event_at if event else now):
            continue
        kind, device = rule["kind"], rule["device"]
        hm, detail = event_at.astimezone(_TZ).strftime("%H:%M"), None
        if event and kind in {"arrive", "leave"}:
            place = event.get("place") or {}
            if isinstance(place, str):
                place = {"name": place, "label": place}
            if event.get("event") == kind and place.get("name") == rule["place"]:
                detail = f"{hm} {'进入' if kind == 'arrive' else '离开'}{place.get('label') or place['name']}"
        if event and kind == "app_open" and event.get("event") == "open":
            dev = event.get("device") or "iphone"
            if dev in {"iphone", "ipad"} and (device == "any" or device == dev) and _normal_app(event.get("app")) == rule["app"]:
                detail = f"{hm} {_DEVICE_LABELS[dev]} 打开了 {rule['app']}"
        if kind == "device_active" and activity:
            dev, quiet, *global_value = activity
            global_quiet = global_value[0] if global_value else quiet
            if device == "any":
                # device_active-any measures global quiet before this device resumed.
                quiet = global_quiet
                dev_label = "设备"
            else:
                dev_label = _DEVICE_LABELS[dev]
            if (device == "any" or device == dev) and quiet >= rule["minutes"]:
                detail = f"{hm} {dev_label} 开始被使用，此前安静了 {int(quiet)} 分钟"
        if phone is not None and kind == "app_use":
            set_at = _parse_dt(condition.get("set_at")) or now
            for dev, held in (phone.get("current_app") or {}).items():
                if dev not in {"iphone", "ipad"} or device not in {"any", dev}:
                    continue
                held_since = _parse_dt(held.get("since")) if isinstance(held, dict) else None
                effective_since = max(filter(None, (held_since, set_at)), default=set_at)
                elapsed = max(0, (now - effective_since).total_seconds() / 60)
                if (isinstance(held, dict) and held_since and not held.get("stale")
                        and _normal_app(held.get("app")) == rule["app"]
                        and elapsed >= rule["minutes"]):
                    detail = f"{_fmt_hm(effective_since)} 起 {_DEVICE_LABELS[dev]} 一直在用 {rule['app']}，已经 {int(elapsed)} 分钟"
                    break
        if phone is not None and presence is not None and kind == "all_quiet":
            # Mac quiet can only be established when presence has a Mac device record.
            set_at = _parse_dt(condition.get("set_at")) or now
            mac = (presence.get("devices") or {}).get("macbook")
            last_event = phone.get("last_event") or {}
            age = last_event.get("age_min")
            last_event_at = _parse_dt(last_event.get("at")) or (now - timedelta(minutes=age) if isinstance(age, (int, float)) else now)
            held = phone.get("current_app") or {}
            tracks = state.get("condition_activity") or {}
            last_used = [_parse_dt(item.get("last_used")) for item in tracks.values()
                         if isinstance(item, dict)]
            last_used = [at for at in last_used if at is not None]
            mac_idle = mac.get("idle_seconds") if isinstance(mac, dict) else None
            if isinstance(mac_idle, (int, float)) and mac.get("online"):
                last_used.append(now - timedelta(seconds=max(0, mac_idle)))
            quiet_since = max([set_at, last_event_at, *last_used])
            quiet_min = max(0, (now - quiet_since).total_seconds() / 60)
            if (isinstance(mac, dict) and isinstance(age, (int, float))
                    and quiet_min >= rule["minutes"] and isinstance(held, dict)
                    and not any(v and not v.get("stale") for v in held.values())
                    and (not mac.get("online") or mac.get("screen_locked") or
                         isinstance(mac_idle, (int, float)) and mac_idle >= rule["minutes"] * 60)):
                n = quiet_min
                if mac.get("online") and isinstance(mac_idle, (int, float)):
                    n = min(n, mac_idle / 60)
                detail = f"所有设备已安静 {int(n)} 分钟"
        if detail:
            condition.update(met_at=_iso(now), met_event=detail)
            scene["next_wake_at"] = _iso(now)
            matched.add(scene["id"])
    return matched


def _previous_phone_usage(now, event, device_state=None):
    return (device_state or {}).get("previous_usage", {})


def _snapshot_device_activity(now, phone, presence, previous=None, preserve_device=None):
    tracks = {}
    if previous is None:
        previous = _previous_phone_usage(now, None)
    devices = (presence or {}).get("devices") or {}
    held_apps = (phone or {}).get("current_app") or {}
    for dev in ("iphone", "ipad", "macbook"):
        old = previous.get(dev) or {}
        last = _parse_dt(old.get("last_used"))
        entry = devices.get(dev) or {}
        held = held_apps.get(dev) or {}
        if dev == "macbook":
            idle = entry.get("idle_seconds")
            active = bool(entry.get("online") and not entry.get("screen_locked")
                          and isinstance(idle, (int, float)) and idle < _MAC_IDLE_SLEEP_S)
            if isinstance(idle, (int, float)):
                last = now - timedelta(seconds=max(0, idle))
        else:
            since = _parse_dt(held.get("since")) if isinstance(held, dict) else None
            active = bool(held and not held.get("stale") and since
                          and now-since < timedelta(minutes=30))
            if active:
                last = now
        if preserve_device == dev:
            active = bool(old.get("active"))
            last = _parse_dt(old.get("last_used"))
        tracks[dev] = {"active": active, "last_used": _iso(last) if last else None}
    return tracks


async def check_wake_conditions(storage, *, event=None, presence_event=None, device_state=None,
                                location_events=None, known_places=None, known_apps=None):
    """Observe activity and evaluate rules; callers isolate telemetry failures."""
    now = _now(storage)
    async with storage.state_lock:
        state = await _load_state(storage)
        active = _active_from_state(state, now)
        if not any(isinstance(scene.get("wake_condition"), dict)
                   and not scene["wake_condition"].get("met_at") for scene in active):
            return set()
    device_state = device_state if isinstance(device_state, dict) else {}
    phone = device_state.get("phone")
    presence = device_state.get("presence")
    async with storage.state_lock:
        state = await _load_state(storage)
        before = copy.deepcopy(state)
        matched = _match_conditions(state, now, event=event, phone=phone, presence=presence)
        for location in location_events or []:
            if isinstance(location, dict):
                matched |= _match_conditions(state, now, event=location, phone=phone, presence=presence)
        if device_state is None and event is None and presence_event is None and not location_events:
            return set()
        tracks = state.setdefault("condition_activity", {})
        forced_device = None
        if event and event.get("event") == "open":
            forced_device = event.get("device") or "iphone"
        if presence_event and (presence_event.get("semantic") or {}).get("active"):
            forced_device = presence_event.get("device")
        if not isinstance(device_state, dict) or not any(
                isinstance(device_state.get(k), dict) for k in ("phone", "presence")):
            if state != before:
                await _save_state(storage, state)
            return matched
        if not tracks:
            previous = _previous_phone_usage(now, event, device_state)
            tracks.update(_snapshot_device_activity(now, phone, presence, previous,
                                                    preserve_device=forced_device))
        elif forced_device and (device_state or {}).get("previous_usage", {}).get(forced_device):
            tracks[forced_device] = copy.deepcopy((device_state or {})["previous_usage"][forced_device])
        previous_last_used = [dt for item in tracks.values() if isinstance(item, dict)
                              if (dt := _parse_dt(item.get("last_used"))) is not None]
        global_last_used = max(previous_last_used, default=None)
        # Include currently held phone sessions from other devices, but exclude
        # the device whose resume event is being evaluated.
        for dev, held in ((phone or {}).get("current_app") or {}).items():
            since = _parse_dt(held.get("since")) if isinstance(held, dict) else None
            if (dev != forced_device and dev in {"iphone", "ipad"} and since
                    and not held.get("stale") and now - since < timedelta(minutes=30)):
                global_last_used = now
        mac_entry = ((presence or {}).get("devices") or {}).get("macbook") or {}
        mac_idle = mac_entry.get("idle_seconds")
        if (forced_device != "macbook" and mac_entry.get("online")
                and not mac_entry.get("screen_locked") and isinstance(mac_idle, (int, float))):
            mac_last_used = now - timedelta(seconds=max(0, mac_idle))
            global_last_used = max(global_last_used, mac_last_used) if global_last_used else mac_last_used
        for dev in ("iphone", "ipad", "macbook"):
            old = tracks.get(dev) or {}
            last = _parse_dt(old.get("last_used"))
            entry = ((presence or {}).get("devices") or {}).get(dev) or {}
            held = ((phone or {}).get("current_app") or {}).get(dev) or {}
            if dev == "macbook":
                idle = entry.get("idle_seconds")
                active = bool(entry.get("online") and not entry.get("screen_locked")
                              and isinstance(idle, (int, float)) and idle < _MAC_IDLE_SLEEP_S)
            else:
                since = _parse_dt(held.get("since")) if isinstance(held, dict) else None
                active = bool(held and not held.get("stale") and since and now-since < timedelta(minutes=30))
            if not last and dev == "macbook" and isinstance(idle, (int, float)):
                last = now - timedelta(seconds=idle)
            active = active or forced_device == dev
            if active and not old.get("active") and last:
                global_quiet = (now-global_last_used).total_seconds()/60 if global_last_used else (now-last).total_seconds()/60
                device_quiet = (now-last).total_seconds()/60
                matched |= _match_conditions(state, now, activity=(dev, max(0, device_quiet), max(0, global_quiet)))
            # Fresh usage is conservative: no invented historical quiet interval.
            if active or not last:
                last = (now - timedelta(seconds=idle)) if dev == "macbook" and isinstance(idle, (int, float)) else now
            if event and dev == (event.get("device") or "iphone") and event.get("event") in {"open", "close"}:
                last = now
            tracks[dev] = {"active": active, "last_used": _iso(last)}
        if state != before:
            await _save_state(storage, state)
        return matched


async def execute_collar_companion(storage, arguments: dict[str, Any],
                                   *, conversation_id: Optional[str] = None, slm=None,
                                   known_places=None, known_apps=None, device_state=None) -> str:
    args = arguments or {}
    action = str(args.get("action") or "status").strip().lower()
    if "recreate_after_minutes" in args:
        minutes = args["recreate_after_minutes"]
        if action != "end_scene":
            return _json({"error": "recreate_after_minutes is only valid for end_scene"})
        if type(minutes) is not int or not 0 <= minutes <= 120:
            return _json({"error": "recreate_after_minutes must be an integer from 0 to 120"})
    condition = ...
    if "wake_when" in args:
        if action not in {"start_scene", "update_scene"}:
            return _json({"error": "wake_when is only valid for start_scene/update_scene"})
        try:
            condition = await translate_wake_when(slm, args["wake_when"],
                known_places=known_places, known_apps=known_apps, now=storage.now())
        except Exception as exc:
            return _json({"error": "wake_when_rejected", "reason": str(exc) or
                          "条件翻译失败，请换个说法或稍后再试", "supported": _WAKE_SUPPORTED})
    if action == "status":
        status = await get_companion_status(storage)
        try:
            from .scheduler import execute_collar_wake
            status["wake"] = json.loads(await execute_collar_wake(storage, {"action": "status"}))
        except Exception:
            pass
        return _json({"ok": True, **status})
    if action in {"set_note", "clear_note"}:
        scene_id = str(args.get("scene_id") or "").strip()
        if not scene_id:
            return _json({"error": "scene_id is required for a scene note"})
        note = args.get("note", "")
        if action == "set_note":
            if not isinstance(note, str) or not note.strip():
                return _json({"error": "note is required for set_note"})
            if len(note) > 800:
                return _json({"error": "note must be at most 800 characters"})
        elif note not in ("", None):
            return _json({"error": "clear_note takes no note text"})
        async with storage.state_lock:
            state = await _load_state(storage)
            scene = next((item for item in _active_from_state(state, storage.now())
                          if item.get("id") == scene_id), None)
            if scene is None:
                return _json({"error": "active scene not found"})
            write_nudge(scene, note if action == "set_note" else "", storage.now())
            await _save_state(storage, state)
            return _json({"ok": True, "scene_id": scene_id,
                          "note": scene.get("nudge"),
                          "remaining_reads": scene.get("nudge_wakes_left", 0)})
    if action in {"start_scene", "set_repeat"}:
        scene_type = "repeat" if action == "set_repeat" else str(args.get("scene_type") or "custom")
        scene = await start_scene(
            storage, scene_type=scene_type, conversation_id=conversation_id,
            source="master_tool", facts=args.get("note") or args.get("facts"),
            nudge=str(args.get("nudge") or ""), cadence_min=args.get("cadence_min"),
            duration_min=args.get("duration_min"), immediate=False,
            title=str(args.get("title") or ""), wake_condition=condition,
            device_state=device_state)
        return _json({"ok": True, "scene": _bj_view(scene), **({"wake_when_summary": (condition or {}).get("summary")} if condition is not ... else {})})
    if action in {"update_scene", "set_rhythm"}:
        scene = await update_scene_settings(
            storage,
            scene_id=str(args.get("scene_id") or "").strip() or None,
            scene_type=str(args.get("scene_type") or "").strip() or None,
            facts=args.get("note") or args.get("facts"),
            nudge=str(args.get("nudge") or ""),
            cadence_min=args.get("cadence_min"),
            duration_min=args.get("duration_min"),
            title=str(args.get("title") or ""), wake_condition=condition,
            device_state=device_state)
        if scene is None:
            return _json({"error": "active scene not found; pass scene_id or scene_type"})
        return _json({"ok": True, "scene": _bj_view(scene), **({"wake_when_summary": (condition or {}).get("summary")} if condition is not ... else {})})
    if action == "end_scene":
        try:
            result = await end_scene(
                storage, scene_type=(str(args["scene_type"]) if "scene_type" in args else None),
                scene_id=str(args.get("scene_id") or "").strip() or None,
                reason=str(args.get("note") or "master_ended"),
                recreate_after_minutes=args.get("recreate_after_minutes"))
        except ValueError as exc:
            return _json({"error": str(exc)})
        return _json({"ok": True, **result})

    schedule_actions = {
        "set_alarm", "clear_alarm", "snooze", "goodnight", "set_interval"
    }
    if action in schedule_actions:
        from .scheduler import execute_collar_wake
        legacy = dict(args)
        legacy["minutes"] = args.get("minutes", args.get("cadence_min"))
        return await execute_collar_wake(storage, legacy)
    return _json({
        "error": f"unknown action '{action}'",
        "actions": ["status", "start_scene", "update_scene", "end_scene",
                    "set_rhythm", "set_repeat", "set_alarm", "clear_alarm",
                    "snooze", "goodnight", "set_interval", "set_note", "clear_note"],
    })
