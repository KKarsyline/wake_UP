"""Black-box host coordination tests through WakeEngine.tick/user_message."""

import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from deamo_wake.engine import WakeEngine
from deamo_wake import companion, scheduler
from deamo_wake.storage import SQLiteStore


class EngineAcceptance(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.now = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
        self.store = SQLiteStore(Path(self.tempdir.name) / "wake.sqlite3", now=lambda: self.now)
        self.runner_calls = []
        self.notifier_calls = []

        async def runner(request, on_accepted, tools, interrupted):
            self.runner_calls.append((request, on_accepted, tools, interrupted))
            return {"accepted": False, "completed": False, "substantive": False}

        async def notifier(conversation_id, message):
            self.notifier_calls.append((conversation_id, message))
            return {"delivered": True}

        self.engine = WakeEngine(self.store, runner=runner, notifier=notifier)
        await self.engine.initialize()

    async def asyncTearDown(self):
        await self.engine.close()
        self.tempdir.cleanup()

    async def _due_alarm(self, *, native_thread_id=None):
        await self.store.upsert_conversation("older", native_thread_id="old-thread")
        await self.store.record_user_message("older", "m0", self.now.replace(hour=11))
        await self.store.upsert_conversation("c1", native_thread_id=native_thread_id)
        await self.store.record_user_message("c1", "m1", self.now)
        await self.store.set_setting("collar_wake", {
            "enabled": True, "wake_live": True,
            "alarms": [{"id": "a1", "at": self.now.isoformat(), "note": "test"}],
        })
        await self.engine.configure(wake_live=True, enabled=True)

    async def test_configuration_enables_same_scheduler_wake_settings(self):
        await self.engine.configure(wake_live=True, enabled=True)
        state = await self.store.get_setting("collar_wake")
        self.assertTrue(state["wake_live"])
        self.assertFalse(self.runner_calls)
        outcome = await self.engine.tick()
        self.assertEqual(outcome.get("skipped"), "slm_unavailable")
        self.assertFalse(self.runner_calls)

    async def test_acceptance_completion_and_explicit_notification_are_independent(self):
        await self._due_alarm()
        async def runner(request, on_accepted, tools, interrupted):
            self.assertEqual(request["conversation_id"], "c1")
            self.assertIsNone(request["native_thread_id"])
            await on_accepted("native-1")
            await tools("notify_user", {"message": "Explicit notification"})
            return {"accepted": True, "completed": True, "substantive": True,
                    "notified": False, "text": "runner text"}
        self.engine.runner = runner

        result = await self.engine.tick()
        self.assertTrue(result["accepted"])
        self.assertTrue(result["completed"])
        self.assertTrue(result["notified"])
        self.assertEqual(self.notifier_calls, [("c1", "Explicit notification")])
        receipt = await self.store.read_wake(result["wake_id"])
        self.assertEqual((receipt["completed"], receipt["notified"]), (1, 1))

    async def test_exception_after_acceptance_does_not_clear_acceptance(self):
        await self._due_alarm()
        async def runner(request, on_accepted, tools, interrupted):
            await on_accepted("native-1")
            raise RuntimeError("transport ended after acceptance")
        self.engine.runner = runner

        result = await self.engine.tick()
        self.assertTrue(result["accepted"])
        self.assertFalse(result["completed"])
        receipt = await self.store.read_wake(result["wake_id"])
        self.assertIsNotNone(receipt)
        self.assertEqual(receipt["thread_id"], "native-1")
        self.assertEqual(receipt["completed"], 0)

    async def test_runner_exception_before_acceptance_remains_unknown(self):
        await self.store.upsert_conversation("c1", native_thread_id="native-1")
        await self.store.record_user_message("c1", "m1", self.now)
        await self.engine.configure(enabled=True, wake_live=True)

        async def runner(request, on_accepted, tools, interrupted):
            raise RuntimeError("transport failed before acceptance")

        self.engine.runner = runner
        result = await self.engine._dispatch_wake({"trigger": "alarm"})
        self.assertIsNone(result["accepted"])
        self.assertFalse(result["completed"])
        self.assertEqual(await self.store.read_wake(result["wake_id"]), None)

    async def test_new_real_user_turn_interrupts_and_waits_for_active_runner_cleanup(self):
        await self._due_alarm()
        entered = asyncio.Event()
        interrupted_seen = asyncio.Event()

        async def runner(request, on_accepted, tools, interrupted):
            await on_accepted("native-1")
            entered.set()
            await interrupted.wait()
            interrupted_seen.set()
            return {"accepted": True, "completed": False, "error": "interrupted"}
        self.engine.runner = runner

        run = asyncio.create_task(self.engine.run(interval_seconds=3600))
        await asyncio.wait_for(entered.wait(), timeout=3)
        await self.engine.user_message("c1", "m2", "A fresh user message", at=self.now)
        self.assertTrue(interrupted_seen.is_set())
        await self.engine.close()
        await asyncio.wait_for(run, timeout=3)

    async def test_phone_observation_is_stored_for_condition_matching(self):
        result = await self.engine.observe({"kind": "phone", "body": {
            "event": "open", "device": "iphone", "app": "chat-app",
            "at": self.now.isoformat(), "event_id": "phone-open-1",
        }})
        self.assertTrue(result.get("stored"))
        telemetry = await self.store.get_setting("telemetry")
        current = telemetry["phone"].get("current_app", {}).get("iphone")
        self.assertIsNotNone(current)
        self.assertEqual(current["app"], "chat-app")

        # Distinct apps at the same source timestamp both apply; an older
        # arrival cannot rewind the summary afterwards.
        for event_id, app in (("phone-open-2", "other-app"), ("phone-open-old", "old-app")):
            at = self.now if event_id != "phone-open-old" else self.now - timedelta(minutes=1)
            await self.engine.observe({"kind": "phone", "body": {
                "event": "open", "device": "iphone", "app": app,
                "at": at.isoformat(), "event_id": event_id,
            }})
        telemetry = await self.store.get_setting("telemetry")
        self.assertEqual(telemetry["phone"]["current_app"]["iphone"]["app"], "other-app")

    async def test_health_observation_enters_confirmed_redline_path(self):
        await self._due_alarm()
        # Three fresh threshold breaches satisfy the source's default confirmation rule.
        samples = [{"type": "heart_rate", "value": 150, "unit": "bpm",
                    "at": (self.now - timedelta(seconds=10 - i)).isoformat(), "source": "test"}
                   for i in range(3)]
        result = await self.engine.observe({"kind": "health", "samples": samples})
        self.assertTrue(result.get("redline"))
        self.assertTrue(self.runner_calls, "confirmed redline must invoke the wake runner")
        self.assertEqual(self.runner_calls[-1][0]["trigger"], "redline")

    async def test_alarm_precedes_due_scene_and_patrol(self):
        await self.store.set_setting("collar_wake", {
            "enabled": True, "wake_live": True,
            "alarms": [{"id": "a1", "at": self.now.isoformat(), "note": "first"}],
        })
        await companion.start_scene(self.store, scene_type="outing", cadence_min=5,
                                    conversation_id="c1")
        calls = []
        async def wake(request):
            calls.append(request)
            return {"accepted": False, "completed": False}

        result = await scheduler.tick(self.store, wake=wake, now=self.now + timedelta(minutes=6))
        self.assertEqual(result["trigger"], "alarm")
        self.assertEqual(calls[0]["trigger"], "alarm")
        self.assertEqual(len(calls), 1)

    async def test_report_full_cycle_is_wake_1_6_11(self):
        await self.store.upsert_conversation("c1", native_thread_id="native-1")
        await self.store.record_user_message("c1", "m1", self.now)
        full_flags = []
        async def runner(request, on_accepted, tools, interrupted):
            full_flags.append(request["full_report"])
            await on_accepted("native-1")
            return {"accepted": True, "completed": True, "substantive": True}
        self.engine.runner = runner
        for i in range(11):
            await self.store.set_setting("collar_wake", {
                "enabled": True, "wake_live": True,
                "alarms": [{"id": f"a{i}", "at": self.now.isoformat()}],
            })
            result = await self.engine.tick()
            self.assertTrue(result["accepted"])
            self.assertTrue(result["completed"])
        self.assertEqual(full_flags, [i in {0, 5, 10} for i in range(11)])

    async def test_ordinary_wake_is_skipped_while_runner_holds_global_lock(self):
        await self._due_alarm()
        entered = asyncio.Event()
        release = asyncio.Event()
        async def runner(request, on_accepted, tools, interrupted):
            await on_accepted("native-1")
            entered.set()
            await release.wait()
            return {"accepted": True, "completed": True}
        self.engine.runner = runner
        first_tick = asyncio.create_task(self.engine.tick())
        await asyncio.wait_for(entered.wait(), timeout=3)
        await self.store.set_setting("collar_wake", {
            "enabled": True, "wake_live": True,
            "alarms": [{"id": "a2", "at": self.now.isoformat()}],
        })
        second_tick = asyncio.create_task(self.engine.tick())
        try:
            second_result = await asyncio.wait_for(second_tick, timeout=0.2)
            self.assertEqual(second_result.get("skipped"), "wake_busy")
        finally:
            release.set()
            await asyncio.wait_for(first_tick, timeout=3)

    async def test_redline_waits_for_ordinary_wake_then_runs(self):
        await self._due_alarm()
        entered = asyncio.Event()
        release = asyncio.Event()
        triggers = []
        async def runner(request, on_accepted, tools, interrupted):
            triggers.append(request["trigger"])
            await on_accepted("native-1")
            if request["trigger"] == "alarm":
                entered.set()
                await release.wait()
            return {"accepted": True, "completed": True, "substantive": True}
        self.engine.runner = runner
        first_tick = asyncio.create_task(self.engine.tick())
        await asyncio.wait_for(entered.wait(), timeout=3)
        samples = [{"type": "heart_rate", "value": 150, "unit": "bpm",
                    "at": (self.now - timedelta(seconds=10 - i)).isoformat(), "source": "test"}
                   for i in range(3)]
        redline = asyncio.create_task(self.engine.observe({"kind": "health", "samples": samples}))
        await asyncio.sleep(0)
        self.assertFalse(redline.done())
        release.set()
        await asyncio.wait_for(first_tick, timeout=3)
        observed = await asyncio.wait_for(redline, timeout=3)
        self.assertTrue(observed.get("redline"))
        self.assertEqual(triggers, ["alarm", "redline"])

    async def test_scene_report_contains_rendered_note_and_consumes_it_once(self):
        await self.engine.user_message("c1", "m1", "Start a short scene", at=self.now)
        await self.engine.tool("collar_companion", {
            "action": "start_scene", "scene_type": "outing", "title": "Scene under test",
            "cadence_min": 5, "nudge": "Remember this note",
        }, "c1")
        await self.engine.configure(enabled=True, wake_live=True)
        reports = []
        async def runner(request, on_accepted, tools, interrupted):
            reports.append(request["report"])
            await on_accepted(request["native_thread_id"] or "native-1")
            return {"accepted": True, "completed": True, "substantive": True}
        self.engine.runner = runner
        self.now += timedelta(minutes=6)

        result = await self.engine.tick()
        self.assertTrue(result["completed"])
        self.assertIn("Scene under test", reports[0])
        self.assertIn("Remember this note", reports[0])
        state = await self.store.get_setting("collar_companion")
        scene = next(s for s in state["scenes"] if s["title"] == "Scene under test")
        self.assertEqual(scene["real_wakes"], 1)
        self.assertEqual(scene["nudge_wakes_left"], 2)

    async def test_same_time_health_batches_remain_distinct_and_duplicate_is_idempotent(self):
        at = self.now.isoformat()
        first = {"kind": "health", "samples": [{"type": "heart_rate", "value": 70, "at": at}]}
        second = {"kind": "health", "samples": [{"type": "heart_rate", "value": 72, "at": at}]}
        await self.engine.observe(first)
        await self.engine.observe(second)
        duplicate = await self.engine.observe(second)
        self.assertTrue(duplicate.get("duplicate"))
        telemetry = await self.store.get_setting("telemetry")
        self.assertEqual([s["value"] for s in telemetry["health_samples"]], [70, 72])

    async def test_runner_cannot_claim_notification_without_explicit_delivery(self):
        await self._due_alarm()
        async def runner(request, on_accepted, tools, interrupted):
            await on_accepted("native-1")
            return {"accepted": True, "completed": True, "notified": True,
                    "substantive": True, "text": "ordinary response"}
        self.engine.runner = runner
        result = await self.engine.tick()
        self.assertTrue(result["completed"])
        self.assertFalse(result["notified"])
        self.assertFalse(self.notifier_calls)
        receipt = await self.store.read_wake(result["wake_id"])
        self.assertEqual(receipt["notified"], 0)

    async def test_user_message_start_update_end_deltas_persist_and_dedupe(self):
        outputs = iter([
            {"deltas": [{"action": "start", "scene_type": "work", "facts": ["started"]}]},
            {"deltas": [{"action": "update", "scene_type": "work", "facts": ["updated"]}]},
            {"deltas": [{"action": "end", "scene_type": "work", "reason": "finished"}]},
        ])
        calls = []
        async def slm(**kwargs):
            calls.append(kwargs)
            return next(outputs)
        self.engine.slm = slm
        first = await self.engine.user_message("c1", "m1", "start work", at=self.now)
        self.assertEqual(first["deltas"][0]["action"], "start")
        duplicate = await self.engine.user_message("c1", "m1", "different replay text", at=self.now + timedelta(minutes=1))
        self.assertEqual(duplicate["reason"], "already_processed")
        self.assertEqual(len(calls), 1)
        updated = await self.engine.user_message("c1", "m2", "more work", at=self.now + timedelta(minutes=2))
        self.assertEqual(updated["deltas"][0]["action"], "update")
        ended = await self.engine.user_message("c1", "m3", "done", at=self.now + timedelta(minutes=3))
        self.assertEqual(ended["deltas"][0]["action"], "end")
        state = await self.store.get_setting("collar_companion")
        scene = next(s for s in state["scenes"] if s["type"] == "work")
        self.assertEqual(scene["status"], "ended")
        self.assertTrue(any("updated" in fact for fact in scene["facts"]))

    async def test_classifier_failure_still_persists_real_user_message(self):
        async def broken_slm(**kwargs):
            raise RuntimeError("local classifier unavailable")
        self.engine.slm = broken_slm
        result = await self.engine.user_message("c1", "m1", "a real user turn", at=self.now)
        self.assertEqual(result["reason"], "classifier_failed")
        target = await self.store.get_latest_wake_eligible_conversation()
        self.assertEqual(target["id"], "c1")
        self.assertEqual(target["last_user_at"], self.now.isoformat(timespec="microseconds"))

    async def test_calendar_candidates_without_scene_delta_do_not_start_scene(self):
        async def calendar_only_slm(**kwargs):
            return {"deltas": [], "calendar_candidates": [
                {"title": "Future plan", "starts_at": "2026-11-12T09:00:00+08:00",
                 "ends_at": "2026-11-12T10:00:00+08:00", "precision": "hour",
                 "event_type": "custom", "confidence": 0.8}],
                "reason": "future arrangement"}
        self.engine.slm = calendar_only_slm
        result = await self.engine.user_message("c1", "m1", "tomorrow at work", at=self.now)
        self.assertEqual(result["deltas"], [])
        state = await self.store.get_setting("collar_companion")
        self.assertEqual(state.get("scenes", []), [])
        self.assertIsNotNone(await self.store.get_latest_wake_eligible_conversation())

    async def test_user_configured_cadence_is_not_overwritten_by_later_classification(self):
        outputs = iter([
            {"deltas": [{"action": "start", "scene_type": "repeat", "facts": [], "cadence_min": 15}]},
            {"deltas": [{"action": "update", "scene_type": "repeat", "facts": ["new detail"]}]},
        ])
        async def slm(**kwargs):
            return next(outputs)
        self.engine.slm = slm
        await self.engine.user_message("c1", "m1", "come back every fifteen", at=self.now)
        state = await self.store.get_setting("collar_companion")
        scene = next(s for s in state["scenes"] if s["type"] == "repeat")
        self.assertEqual(scene["cadence_min"], 15)
        self.assertFalse(scene["cadence_by_master"])

        await self.engine.tool("collar_companion", {"action": "update_scene", "scene_type": "repeat",
            "cadence_min": 20}, "c1")
        await self.engine.user_message("c1", "m2", "keep checking", at=self.now + timedelta(minutes=1))
        state = await self.store.get_setting("collar_companion")
        scene = next(s for s in state["scenes"] if s["type"] == "repeat")
        self.assertEqual(scene["cadence_min"], 20)
        self.assertTrue(scene["cadence_by_master"])

    async def test_assistant_activity_and_ineligible_user_activity_have_separate_queries(self):
        await self.store.upsert_conversation("eligible", native_thread_id="thread-1")
        await self.store.record_user_message("eligible", "m1", self.now)
        await self.store.upsert_conversation("disabled", native_thread_id="thread-2", eligible=False)
        await self.store.record_user_message("disabled", "m2", self.now + timedelta(minutes=5))
        latest_target = await self.store.get_latest_wake_eligible_conversation()
        self.assertEqual(latest_target["id"], "eligible")
        self.assertEqual(await self.store.latest_user_message_at(),
                         (self.now + timedelta(minutes=5)).isoformat(timespec="microseconds"))

        assistant_at = self.now + timedelta(minutes=10)
        await self.engine.assistant_activity(assistant_at)
        latest_after_reply = await self.store.get_latest_wake_eligible_conversation()
        self.assertEqual(latest_after_reply["id"], "eligible")
        self.assertEqual(latest_after_reply["last_user_at"], self.now.isoformat(timespec="microseconds"))
        chat_activity = await self.store.get_setting("chat_activity")
        self.assertEqual(chat_activity["last_assistant_at"], assistant_at.isoformat(timespec="microseconds"))

    async def test_scene_api_budget_is_opt_in_counts_failed_attempts_and_resets_at_utc8_day(self):
        await self.store.upsert_conversation("c1", native_thread_id="native-1")
        await self.store.record_user_message("c1", "m1", self.now)
        await self.engine.configure(enabled=True, wake_live=True, scene_api_wake_daily_cap=1)
        await companion.start_scene(self.store, scene_type="outing", cadence_min=5,
                                    conversation_id="c1")
        calls = []
        async def declined(request, on_accepted, tools, interrupted):
            calls.append(request)
            return {"accepted": False, "completed": False, "error": "explicit decline"}
        self.engine.runner = declined
        self.now += timedelta(minutes=6)
        first = await self.engine.tick()  # Budget is disabled by default.
        self.assertFalse(first["accepted"])
        state = await self.store.get_setting("collar_wake")
        self.assertEqual(state.get("scene_api_wake_count", 0), 0)

        await self.engine.configure(scene_api_budget_enabled=True)
        await companion.start_scene(self.store, scene_type="work", cadence_min=5,
                                    conversation_id="c1")
        self.now += timedelta(minutes=6)
        second = await self.engine.tick()
        self.assertFalse(second["accepted"])
        state = await self.store.get_setting("collar_wake")
        local_today = self.now.astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
        self.assertEqual((state["scene_api_wake_date"], state["scene_api_wake_count"]), (local_today, 1))
        self.assertEqual(len(calls), 2)  # A failed dispatch still consumes its attempt.

        budget_state = await scheduler.claim_scene_api_wake_budget(
            self.store, datetime(2026, 11, 11, 15, 59, tzinfo=timezone.utc))
        self.assertFalse(budget_state[0])
        next_day = datetime(2026, 11, 11, 16, 1, tzinfo=timezone.utc)  # 00:01 UTC+08 on Nov 12.
        budget_state = await scheduler.claim_scene_api_wake_budget(self.store, next_day)
        self.assertEqual(budget_state, (True, 1, 1))
        state = await self.store.get_setting("collar_wake")
        self.assertEqual((state["scene_api_wake_date"], state["scene_api_wake_count"]), ("2026-11-12", 1))

    async def test_classifier_prompt_example_field_reaches_real_scene_consumer(self):
        import json
        import re

        def prompt_schema(system):
            decoder = json.JSONDecoder()
            for match in re.finditer(r"\{", system):
                try:
                    candidate, _ = decoder.raw_decode(system[match.start():])
                except json.JSONDecodeError:
                    continue
                if (isinstance(candidate, dict) and isinstance(candidate.get("deltas"), list)
                        and candidate["deltas"] and "scene_type" in candidate["deltas"][0]):
                    return candidate
            self.fail("classifier prompt must include a valid JSON delta example using scene_type")

        async def prompt_conforming_slm(*, system, user_content, max_tokens, temperature):
            example = prompt_schema(system)
            delta = dict(example["deltas"][0])
            delta.update(action="start", scene_type="work", facts=["created from prompt contract"])
            delta.pop("type", None)
            return {"deltas": [delta], "reason": "structured contract fixture"}

        self.engine.slm = prompt_conforming_slm
        result = await self.engine.user_message("c1", "prompt-example-1", "I'm at work", at=self.now)
        self.assertEqual(result["deltas"][0]["scene"]["type"], "work")
        state = await self.store.get_setting("collar_companion")
        self.assertTrue(any(scene["type"] == "work" and scene["status"] == "active"
                            for scene in state["scenes"]))
