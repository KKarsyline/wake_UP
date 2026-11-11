"""Host glue for user messages, observations, tools, and native wake runners."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from datetime import timezone
from uuid import uuid4

from . import companion, receipts, report, scheduler

NOTIFY_USER_SCHEMA = {
    "name": "notify_user",
    "description": "Send a message to the user in the current conversation. Use only when you explicitly choose to contact them.",
    "inputSchema": {"type": "object", "additionalProperties": False,
                    "properties": {"message": {"type": "string", "minLength": 1, "maxLength": 4000}},
                    "required": ["message"]},
}


class WakeEngine:
    def __init__(self, store, *, slm=None, runner, notifier=None,
                 known_places=None, known_apps=None, scene_apps=None):
        self.store, self.slm, self.runner, self.notifier = store, slm, runner, notifier
        self.known_places = known_places or {}
        self.known_apps = known_apps or []
        self.scene_apps = scene_apps or []
        self._wake_lock = asyncio.Lock()
        self._telemetry_lock = asyncio.Lock()
        self._turn_locks = {}
        self._active = {}
        self._closing = False
        self._stop = asyncio.Event()

    @property
    def tool_schemas(self):
        return [copy.deepcopy(companion.TOOL_SCHEMA), copy.deepcopy(NOTIFY_USER_SCHEMA)]

    async def initialize(self):
        await self.store.initialize()
        if await self.store.get_setting("collar_wake") is None:
            await self.store.set_setting("collar_wake", {"enabled": False, "wake_live": False})
        if await self.store.get_setting("telemetry") is None:
            await self.store.set_setting("telemetry", {"phone": {}, "presence": {"devices": {}}, "health_samples": [], "location_events": []})
        return self

    async def close(self):
        self._closing = True
        self._stop.set()
        active = list(self._active.values())
        for record in active:
            record["interrupted"].set()
        if active:
            await asyncio.gather(*(r["done"].wait() for r in active))
        await self.store.close()

    async def configure(self, **wake_settings):
        current = await self.store.get_setting("collar_wake") or {"enabled": False, "wake_live": False}
        current.update(copy.deepcopy(wake_settings))
        await self.store.set_setting("collar_wake", current)
        return current

    async def assistant_activity(self, at=None):
        """Record a normal assistant reply without affecting user routing."""
        stamp = at or self.store.now()
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        stamp = stamp.astimezone(timezone.utc).isoformat(timespec="microseconds")
        current = await self.store.get_setting("chat_activity") or {}
        previous = companion._parse_dt(current.get("last_assistant_at"))
        if previous is None or companion._parse_dt(stamp) > previous:
            current["last_assistant_at"] = stamp
            await self.store.set_setting("chat_activity", current)
        return current

    async def _device_snapshot(self, now=None):
        state = copy.deepcopy(await self.store.get_setting("telemetry") or {})
        now = now or self.store.now()
        phone = state.get("phone")
        if isinstance(phone, dict):
            last = phone.get("last_event")
            if isinstance(last, dict) and (parsed := companion._parse_dt(last.get("at"))):
                last["age_min"] = max(0, int((now - parsed).total_seconds() // 60))
            apps = phone.get("current_app")
            if isinstance(apps, dict):
                for app in apps.values():
                    if not isinstance(app, dict) or not (since := companion._parse_dt(app.get("since"))):
                        continue
                    minutes = max(0, int((now - since).total_seconds() // 60))
                    app["minutes"] = minutes
                    app["stale"] = minutes > 120
        return state

    async def user_message(self, conversation_id, message_id, text, at=None):
        """Persist one real user turn after cooperative wake interruption."""
        current = self._active.get(conversation_id)
        if current:
            current["interrupted"].set()
        turn_lock = self._turn_locks.setdefault(conversation_id, asyncio.Lock())
        async with turn_lock:
            if await self.store.get_conversation(conversation_id) is None:
                await self.store.upsert_conversation(conversation_id)
            result = await companion.process_user_message_scene(
                self.store, self.slm, conversation_id=conversation_id,
                message_id=message_id, user_text=text, classify=self.slm is not None)
            await self.store.record_user_message(conversation_id, message_id,
                                                  at or self.store.now())
            return result

    async def observe(self, event):
        kind = event.get("kind")
        if kind not in {"phone", "presence", "location", "health"}:
            raise ValueError("event.kind must be phone, presence, location, or health")
        tick_now = False
        samples = None
        async with self._telemetry_lock:
            state = await self.store.get_setting("telemetry") or {
                "phone": {}, "presence": {"devices": {}}, "health_samples": [], "location_events": []}
            body = copy.deepcopy(event.get("body") or {})
            batch = event.get("samples") or event.get("location_events") or []
            sample_stamp = next((item.get("at") or item.get("timestamp") for item in batch
                                 if isinstance(item, dict) and (item.get("at") or item.get("timestamp"))), None)
            source_stamp = body.get("at") or body.get("timestamp") or body.get("created_at") or sample_stamp
            stamp = source_stamp or self.store.now().isoformat()
            event_id = body.get("event_id") or body.get("id")
            payload = {"kind": kind, "source": body.get("source"), "device": body.get("device")}
            if event_id:
                payload["id"] = event_id
            elif source_stamp:
                payload.update(at=stamp, body=body,
                               samples=event.get("samples"),
                               location_events=event.get("location_events"))
            else:
                payload["arrival"] = uuid4().hex
            event_key = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()
            receipts = await self.store.get_setting("telemetry_event_ids") or []
            if event_key in receipts:
                return {"stored": False, "duplicate": True}
            if kind == "phone":
                phone = state.setdefault("phone", {"current_app": {}, "last_event": {}})
                phone.setdefault("current_app", {})
                app, action = str(body.get("app") or ""), str(body.get("event") or "").lower()
                device = str(body.get("device") or "iphone").lower()
                previous_at = companion._parse_dt((phone.get("last_event") or {}).get("at"))
                event_at = companion._parse_dt(stamp)
                is_latest = previous_at is None or event_at is None or event_at >= previous_at
                held = phone["current_app"].get(device)
                device_at = companion._parse_dt((phone.get("device_last_event_at") or {}).get(device))
                if device_at is None and isinstance(held, dict):
                    device_at = companion._parse_dt(held.get("since"))
                is_latest_for_device = device_at is None or event_at is None or event_at >= device_at
                if is_latest_for_device and app and action in {"open", "opened", "app_open", "foreground"}:
                    phone["current_app"][device] = {"app": app, "since": stamp, "minutes": 0, "stale": False}
                elif is_latest_for_device and app and action in {"close", "closed", "app_close", "background"}:
                    if isinstance(held, dict) and str(held.get("app", "")).lower() == app.lower():
                        phone["current_app"].pop(device, None)
                if is_latest:
                    phone["last_event"] = {"at": stamp, "age_min": 0, "event": action, "device": device}
                if is_latest_for_device:
                    phone.setdefault("device_last_event_at", {})[device] = stamp
                state["phone"] = phone
                result = await companion.handle_phone_event(
                    self.store, body, {"stored": True, "record": {"at": stamp}},
                    device_state=state, scene_apps=self.scene_apps)
                result = {"stored": True, **result}
                tick_now = bool(result.get("immediate"))
            elif kind == "presence":
                devices = state.setdefault("presence", {}).setdefault("devices", {})
                device = str(body.get("device") or "unknown").lower()
                previous = devices.get(device, {})
                previous_at = companion._parse_dt(previous.get("last_seen_at") or previous.get("last_seen")
                                                  or previous.get("updated_at") or previous.get("at"))
                event_at = companion._parse_dt(stamp)
                if previous_at is None or event_at is None or event_at >= previous_at:
                    devices[device] = {**previous, **{k: v for k, v in body.items()
                                                      if isinstance(v, (str, int, float, bool))}}
                    devices[device]["last_seen_at"] = stamp
                result = await companion.handle_presence_event(
                    self.store, body, {"stored": True, "received_at": stamp,
                                       "semantic": event.get("semantic") or {}}, device_state=state)
                result = {"stored": True, **result}
            elif kind == "location":
                records = event.get("location_events")
                if records is None:
                    records = [body]
                state.setdefault("location_events", []).extend(copy.deepcopy(records))
                state["location_events"] = state["location_events"][-20:]
                result = await companion.process_location_events(
                    self.store, location_events=records, known_places=self.known_places)
                result = {"stored": True, "events": result}
            else:
                samples = event.get("samples") or [body]
                state.setdefault("health_samples", []).extend(copy.deepcopy(samples))
                state["health_samples"] = state["health_samples"][-20:]
            receipts = (receipts + [event_key])[-500:]
            await self.store.set_setting("telemetry", state)
            await self.store.set_setting("telemetry_event_ids", receipts)
        if kind == "health":
            result = await scheduler.check_redline(
                self.store, samples, wake=self._dispatch_wake, now=self.store.now())
            result = {"stored": len(samples), **(result or {})}
        if tick_now:
            result["wake"] = await self.tick()
        return result

    async def tool(self, name, arguments, conversation_id=None, *, _wake_id=None, _wake_record=None):
        if name == "collar_companion":
            return await companion.execute_collar_companion(
                self.store, arguments, conversation_id=conversation_id, slm=self.slm,
                known_places=self.known_places, known_apps=self.known_apps,
                device_state=await self._device_snapshot())
        if name == "notify_user":
            message = str((arguments or {}).get("message") or "").strip()
            if not message or not conversation_id or self.notifier is None:
                return {"sent": False, "error": "message, conversation, and notifier are required"}
            if not _wake_id or not _wake_record or not _wake_record.get("accepted"):
                return {"sent": False, "error": "notification requires an accepted wake"}
            delivered = await self.notifier(conversation_id, message)
            ok = isinstance(delivered, dict) and delivered.get("delivered") is True
            if ok and _wake_id:
                _wake_record["notified"] = True
                await self.store.mark_notified(_wake_id)
            return {"sent": ok, "result": delivered}
        raise ValueError(f"unknown tool: {name}")

    async def _dispatch_wake(self, wake):
        if self._closing:
            return {"accepted": False, "completed": False, "error": "engine_closing"}
        redline = wake.get("trigger") == "redline"
        if self._wake_lock.locked() and not redline:
            return {"accepted": False, "completed": False, "skipped": "wake_busy"}
        await self._wake_lock.acquire()
        conversation_id = None
        record = None
        try:
            while True:
                if self._closing:
                    return {"accepted": False, "completed": False, "error": "engine_closing"}
                conversation = await self.store.get_latest_wake_eligible_conversation()
                if not conversation:
                    return {"accepted": False, "completed": False, "error": "no_eligible_conversation"}
                conversation_id = conversation["id"]
                record = {"interrupted": asyncio.Event(), "accepted": False,
                          "notified": False, "done": asyncio.Event()}
                self._active[conversation_id] = record
                turn_lock = self._turn_locks.setdefault(conversation_id, asyncio.Lock())
                retry_target = False
                async with turn_lock:
                    latest = await self.store.get_latest_wake_eligible_conversation()
                    if not latest:
                        return {"accepted": False, "completed": False, "error": "no_eligible_conversation"}
                    if latest["id"] != conversation_id:
                        retry_target = True
                    elif self._closing or record["interrupted"].is_set():
                        return {"accepted": False, "completed": False, "error": "interrupted_before_start"}
                    else:
                        return await self._run_wake(conversation, wake, record)
                record["done"].set()
                if self._active.get(conversation_id) is record:
                    self._active.pop(conversation_id, None)
                if retry_target:
                    continue
        finally:
            if record is not None:
                record["done"].set()
                if self._active.get(conversation_id) is record:
                    self._active.pop(conversation_id, None)
            self._wake_lock.release()

    async def _run_wake(self, conversation, wake, record):
        conversation_id = conversation["id"]
        thread_id = conversation.get("native_thread_id")
        wake_id = wake.get("wake_id") or uuid4().hex
        wake_index = await self.store.wake_count(thread_id) + 1 if thread_id else 1
        full = (wake_index - 1) % 5 == 0
        scene_bundle = wake.get("scene_bundle") or {}
        now = self.store.now()
        async with self.store.state_lock:
            scene_state = await companion._load_state(self.store)
            raw_scenes = companion._active_from_state(scene_state, now)
        # Render raw timestamps and note version identities. Status projections
        # format timestamps for display and cannot safely settle note receipts.
        scenes = scene_bundle.get("active") or raw_scenes
        telemetry = await self._device_snapshot(now)
        rendered, refs = report.render_report(
            now=now, full=full, trigger=wake.get("trigger", "wake"), scenes=scenes,
            device_state=telemetry, alarms=wake.get("alarms"),
            scanner=wake.get("scanner"), primary_scene=scene_bundle.get("primary") or scene_bundle.get("primary_scene"),
            redline=wake.get("redline"))

        latch = {"accepted": False}

        async def on_accepted(native_thread_id):
            if not native_thread_id:
                raise ValueError("accepted wake requires native thread id")
            latch["accepted"] = True
            record["accepted"] = True
            current = await self.store.get_conversation(conversation_id)
            if current is None:
                await self.store.upsert_conversation(conversation_id, native_thread_id=native_thread_id)
            elif current.get("native_thread_id") != native_thread_id:
                await self.store.bind_native_thread(conversation_id, native_thread_id)
            await self.store.accept_wake(wake_id, native_thread_id)

        async def tools(name, arguments):
            return await self.tool(name, arguments, conversation_id,
                                   _wake_id=wake_id, _wake_record=record)

        request = {"wake_id": wake_id, "trigger": wake.get("trigger", "wake"),
                   "conversation_id": conversation_id, "native_thread_id": thread_id,
                   "report": rendered, "full_report": full}
        try:
            result = await self.runner(request, on_accepted, tools, record["interrupted"])
            result = result if isinstance(result, dict) else {"text": str(result)}
        except asyncio.CancelledError:
            result = {"error": "interrupted", "completed": False}
        except Exception as exc:
            result = {"error": str(exc), "completed": False}
        accepted = True if latch["accepted"] else (False if result.get("accepted") is False else None)
        error = result.get("error")
        if result.get("accepted") is True and not accepted:
            error = error or "runner_reported_acceptance_without_on_accepted"
        if result.get("completed") and not latch["accepted"]:
            error = error or "runner_completed_without_on_accepted"
        completed = bool(result.get("completed")) and not error and latch["accepted"]
        substantive = bool(result.get("substantive") or record["notified"])
        notified = record["notified"] or bool((await self.store.read_wake(wake_id) or {}).get("notified"))
        if accepted is True:
            if completed:
                await receipts.complete_wake(self.store, wake_id, refs, ok=True,
                                             substantive=substantive)
            else:
                await self.store.finish_wake(wake_id, completed=False,
                                             substantive=substantive, error=error)
        return {"accepted": accepted, "completed": completed, "ok": accepted is True and completed and not error,
                "notified": notified, "substantive": substantive,
                "notes_settled": accepted is True and completed,
                "text": result.get("text", ""), "error": error, "wake_id": wake_id,
                "trigger": wake.get("trigger", "wake")}

    async def tick(self):
        settings = await self.store.get_setting("collar_wake") or {"enabled": False, "wake_live": False}
        if not settings.get("enabled") or not settings.get("wake_live"):
            return {"skipped": "wake_disabled"}
        now = self.store.now()
        telemetry = await self._device_snapshot(now)
        return await scheduler.tick(
            self.store, slm=self.slm, wake=self._dispatch_wake,
            device_state=telemetry,
            location_events=telemetry.get("location_events"),
            known_places=self.known_places, known_apps=self.known_apps, now=now)

    async def run(self, interval_seconds=60):
        while not self._closing:
            await self.tick()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval_seconds)
            except TimeoutError:
                pass
