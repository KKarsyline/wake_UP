import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from deamo_wake import companion
from deamo_wake import scheduler
from deamo_wake.engine import WakeEngine
from deamo_wake.storage import SQLiteStore


class RuntimeEngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_message_interrupts_wake_inside_long_running_engine_loop(self):
        with tempfile.TemporaryDirectory() as tmp:
            now = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
            store = SQLiteStore(Path(tmp) / "state.sqlite3", now=lambda: now)
            entered = asyncio.Event()

            async def runner(request, on_accepted, tools, interrupted):
                await on_accepted(request["native_thread_id"])
                entered.set()
                await asyncio.wait_for(interrupted.wait(), timeout=2)
                return {"accepted": True, "completed": False, "error": "interrupted"}

            engine = WakeEngine(store, runner=runner)
            await engine.initialize()
            await store.upsert_conversation("chat", native_thread_id="native")
            await store.record_user_message("chat", "m0", now)
            await engine.configure(enabled=True, wake_live=True)
            await store.set_setting("collar_wake", {"enabled": True, "wake_live": True,
                                                      "alarms": [{"id": "a", "at": now.isoformat()}]})
            loop_task = asyncio.create_task(engine.run(interval_seconds=3600))
            await asyncio.wait_for(entered.wait(), timeout=1)
            response = await asyncio.wait_for(
                engine.user_message("chat", "m1", "new turn", now), timeout=1)
            self.assertIsInstance(response, dict)
            self.assertFalse(engine._stop.is_set())
            self.assertEqual((await store.get_latest_wake_eligible_conversation())["id"], "chat")
            await engine.close()
            await asyncio.wait_for(loop_task, timeout=1)

    async def test_same_time_different_phone_apps_apply_and_old_event_does_not_rewind(self):
        with tempfile.TemporaryDirectory() as tmp:
            now = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
            engine = WakeEngine(SQLiteStore(Path(tmp) / "state.sqlite3", now=lambda: now), runner=None)
            await engine.initialize()
            stamp = now.isoformat()
            await engine.observe({"kind": "phone", "body": {"event": "open", "device": "iphone",
                "app": "one", "at": stamp, "event_id": "1"}})
            result = await engine.observe({"kind": "phone", "body": {"event": "open", "device": "iphone",
                "app": "two", "at": stamp, "event_id": "2"}})
            self.assertTrue(result["stored"])
            await engine.observe({"kind": "phone", "body": {"event": "open", "device": "iphone",
                "app": "old", "at": "2026-11-11T11:59:00+00:00", "event_id": "old"}})
            phone = (await engine.store.get_setting("telemetry"))["phone"]
            self.assertEqual(phone["current_app"]["iphone"]["app"], "two")
            await engine.close()

    async def test_same_time_distinct_health_batches_are_not_deduplicated(self):
        with tempfile.TemporaryDirectory() as tmp:
            now = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
            engine = WakeEngine(SQLiteStore(Path(tmp) / "state.sqlite3", now=lambda: now), runner=None)
            await engine.initialize()
            at = now.isoformat()
            first = {"kind": "health", "samples": [{"type": "heart_rate", "value": 70, "at": at}]}
            second = {"kind": "health", "samples": [{"type": "heart_rate", "value": 72, "at": at}]}
            await engine.observe(first)
            await engine.observe(second)
            self.assertTrue((await engine.observe(second)).get("duplicate"))
            telemetry = await engine.store.get_setting("telemetry")
            self.assertEqual(len(telemetry["health_samples"]), 2)
            await engine.close()

    async def test_phone_app_duration_and_bedtime_orphan_snapshot_refresh(self):
        with tempfile.TemporaryDirectory() as tmp:
            clock = [datetime(2026, 11, 11, 7, tzinfo=timezone.utc)]
            engine = WakeEngine(SQLiteStore(Path(tmp) / "state.sqlite3", now=lambda: clock[0]), runner=None)
            await engine.initialize()
            await engine.user_message("chat", "m1", "good night", clock[0] - timedelta(minutes=40))
            await companion.start_scene(engine.store, scene_type="bedtime", conversation_id="chat")
            await engine.observe({"kind": "phone", "body": {"event": "open", "device": "iphone",
                "app": "chat-app", "at": clock[0].isoformat(), "event_id": "open"}})
            await engine.store.set_setting("telemetry", {
                **(await engine.store.get_setting("telemetry")),
                "presence": {"devices": {"macbook": {"online": False}}}})

            clock[0] += timedelta(minutes=30)
            snapshot = await engine._device_snapshot()
            app = snapshot["phone"]["current_app"]["iphone"]
            self.assertEqual(app["minutes"], 30)
            self.assertFalse(app["stale"])
            self.assertTrue(await companion._user_asleep(engine.store, clock[0], device_state=snapshot))

            clock[0] += timedelta(minutes=91)
            async def inspect_tool(*args, **kwargs):
                return kwargs["device_state"]
            with patch("deamo_wake.engine.companion.execute_collar_companion", new=inspect_tool):
                tool_snapshot = await engine.tool("collar_companion", {"action": "status"}, "chat")
            stale_app = tool_snapshot["phone"]["current_app"]["iphone"]
            self.assertEqual(stale_app["minutes"], 121)
            self.assertTrue(stale_app["stale"])
            self.assertEqual(tool_snapshot["phone"]["last_event"]["age_min"], 121)
            self.assertTrue(await companion._user_asleep(engine.store, clock[0], device_state=tool_snapshot))
            await engine.close()

    async def test_close_then_late_old_open_does_not_resurrect_phone_app(self):
        with tempfile.TemporaryDirectory() as tmp:
            now = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
            engine = WakeEngine(SQLiteStore(Path(tmp) / "state.sqlite3", now=lambda: now), runner=None)
            await engine.initialize()
            await engine.observe({"kind": "phone", "body": {"event": "open", "device": "iphone",
                "app": "chat-app", "at": now.isoformat(), "event_id": "open"}})
            await engine.observe({"kind": "phone", "body": {"event": "close", "device": "iphone",
                "app": "chat-app", "at": (now + timedelta(minutes=10)).isoformat(), "event_id": "close"}})
            await engine.observe({"kind": "phone", "body": {"event": "open", "device": "iphone",
                "app": "old-app", "at": (now + timedelta(minutes=5)).isoformat(), "event_id": "late"}})
            phone = (await engine.store.get_setting("telemetry"))["phone"]
            self.assertNotIn("iphone", phone["current_app"])
            self.assertEqual(phone["device_last_event_at"]["iphone"],
                             (now + timedelta(minutes=10)).isoformat())
            await engine.close()

    async def test_non_heart_rate_health_observation_is_safe_without_redline(self):
        with tempfile.TemporaryDirectory() as tmp:
            now = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
            engine = WakeEngine(SQLiteStore(Path(tmp) / "state.sqlite3", now=lambda: now), runner=None)
            await engine.initialize()
            result = await engine.observe({"kind": "health", "samples": [
                {"type": "step_count", "value": 12, "at": now.isoformat()}]})
            self.assertEqual(result["stored"], 1)
            self.assertFalse(result.get("redline"))
            await engine.close()

    async def test_hidden_user_message_and_assistant_activity_affect_patrol_without_routing(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
            engine = WakeEngine(SQLiteStore(Path(tmp) / "state.sqlite3", now=lambda: base), runner=None)
            await engine.initialize()
            await engine.store.upsert_conversation("target", native_thread_id="native")
            await engine.user_message("target", "u1", "hello", base)
            target_before = await engine.store.get_latest_wake_eligible_conversation()
            await engine.store.upsert_conversation("hidden", eligible=False, archived=True,
                                                   listable=False, native_thread_id="hidden-native")
            await engine.store.record_user_message("hidden", "u2", base + timedelta(minutes=5))
            self.assertEqual((await engine.store.get_latest_wake_eligible_conversation())["id"], "target")
            self.assertEqual(await engine.store.latest_user_message_at(),
                             (base + timedelta(minutes=5)).isoformat(timespec="microseconds"))
            first = await engine.assistant_activity(base + timedelta(minutes=6))
            earlier = await engine.assistant_activity(base + timedelta(minutes=4))
            self.assertEqual(first["last_assistant_at"], earlier["last_assistant_at"])
            times = await scheduler._last_message_times(engine.store)
            self.assertEqual(times["user"], base + timedelta(minutes=5))
            self.assertEqual(times["assistant"], base + timedelta(minutes=6))
            self.assertEqual((await engine.store.get_latest_wake_eligible_conversation())["id"], target_before["id"])
            await engine.close()


if __name__ == "__main__":
    unittest.main()
