"""Scene note tool, lifetime, receipts and replacement safety."""

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from deamo_wake import companion, receipts, report
from deamo_wake.engine import WakeEngine
from deamo_wake.storage import SQLiteStore


class NotesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.now = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
        self.store = SQLiteStore(Path(self.tempdir.name) / "wake.sqlite3", now=lambda: self.now)
        await self.store.initialize()
        await self.store.set_setting("collar_wake", {"enabled": True, "wake_live": True})
        self.engine = WakeEngine(self.store, runner=self._runner)
        self.scene = await companion.start_scene(self.store, scene_type="outing", title="Trip")

    async def _runner(self, request, on_accepted, tools, interrupted):
        return {"accepted": False, "completed": False}

    async def asyncTearDown(self):
        await self.store.close()
        self.tempdir.cleanup()

    async def _note_tool(self, action, **args):
        return json.loads(await self.engine.tool(
            "collar_companion", {"action": action, **args}, "chat-1"))

    async def test_scene_note_tool_is_strict_and_clear_resets(self):
        self.assertIn("set_note", companion.TOOL_SCHEMA["inputSchema"]["properties"]["action"]["enum"])
        self.assertIn("clear_note", companion.TOOL_SCHEMA["inputSchema"]["properties"]["action"]["enum"])
        self.assertEqual((await self._note_tool("set_note", scene_id=self.scene["id"], note="next stop"))["remaining_reads"], 3)
        status = await self._note_tool("status")
        scene = next(s for s in status["active_scenes"] if s["id"] == self.scene["id"])
        self.assertEqual((scene["nudge"], scene["nudge_wakes_left"]), ("next stop", 3))
        self.assertEqual((await self._note_tool("clear_note", scene_id=self.scene["id"]))["remaining_reads"], 0)
        for args in ({"note": "x"}, {"scene_id": self.scene["id"], "note": " "},
                     {"scene_id": "missing", "note": "x"}):
            result = await self._note_tool("set_note", **args)
            self.assertIn("error", result)
        state = await self.store.get_setting("collar_companion")
        target = next(s for s in state["scenes"] if s["id"] == self.scene["id"])
        target.update(nudge="old", nudge_wakes_left=2,
                      expires_at=(self.now - timedelta(seconds=1)).isoformat())
        await self.store.set_setting("collar_companion", state)
        result = await self._note_tool("clear_note", scene_id=self.scene["id"])
        self.assertIn("error", result)
        unchanged = await self.store.get_setting("collar_companion")
        self.assertEqual(next(s for s in unchanged["scenes"] if s["id"] == self.scene["id"])["nudge"], "old")

    async def test_only_successful_substantive_scene_reports_consume_three_reads(self):
        await self._note_tool("set_note", scene_id=self.scene["id"], note="three reads")
        for index, remaining in enumerate((2, 1, 0), start=1):
            state = await self.store.get_setting("collar_companion")
            _, refs = report.render_report(now=self.now, full=True, trigger="companion_scene",
                                           scenes=state["scenes"])
            self.assertEqual(len(refs), 1)
            wake_id = f"read-{index}"
            await self.store.accept_wake(wake_id, "thread-1")
            await receipts.complete_wake(self.store, wake_id, refs, ok=False, substantive=True)
            self.assertEqual((await self.store.get_setting("collar_companion"))["scenes"][0]["nudge_wakes_left"], 3-index+1)
            await receipts.complete_wake(self.store, wake_id, refs, ok=True, substantive=True)
            await receipts.complete_wake(self.store, wake_id, refs, ok=True, substantive=True)
            current = await self.store.get_setting("collar_companion")
            self.assertEqual(current["scenes"][0].get("nudge_wakes_left", 0), remaining)
            self.assertEqual((await self.store.read_wake(wake_id))["completed"], 1)

    async def test_old_scene_note_report_cannot_consume_replacement(self):
        await self._note_tool("set_note", scene_id=self.scene["id"], note="old note")
        before = await self.store.get_setting("collar_companion")
        _, refs = report.render_report(now=self.now, full=True, trigger="companion_scene",
                                       scenes=before["scenes"])
        await self._note_tool("set_note", scene_id=self.scene["id"], note="old note")
        await self.store.accept_wake("stale-report", "thread-1")
        await receipts.complete_wake(self.store, "stale-report", refs, ok=True, substantive=True)
        current = await self.store.get_setting("collar_companion")
        self.assertEqual(current["scenes"][0]["nudge"], "old note")
        self.assertEqual(current["scenes"][0]["nudge_wakes_left"], 3)
        self.assertNotEqual(current["scenes"][0]["nudge_version"], refs[0]["nudge_version"])

    async def test_successful_non_substantive_wake_keeps_note_and_finishes_receipt(self):
        await self._note_tool("set_note", scene_id=self.scene["id"], note="keep until read")
        state = await self.store.get_setting("collar_companion")
        _, refs = report.render_report(now=self.now, full=True, trigger="companion_scene",
                                       scenes=state["scenes"])
        await self.store.accept_wake("empty-success", "thread-1")
        await receipts.complete_wake(
            self.store, "empty-success", refs, ok=True, substantive=False)
        note = (await self.store.get_setting("collar_companion"))["scenes"][0]
        self.assertEqual((note["nudge"], note["nudge_wakes_left"]),
                         ("keep until read", 3))
        self.assertEqual((await self.store.read_wake("empty-success"))["completed"], 1)

    async def test_engine_alarm_renders_raw_scene_note_and_consumes_it(self):
        await self._note_tool("set_note", scene_id=self.scene["id"], note="raw timestamp note")
        await self.store.upsert_conversation("chat-1", native_thread_id="thread-1")
        await self.store.record_user_message("chat-1", "message-1", self.now)
        await self.store.set_setting("collar_wake", {
            "enabled": True, "wake_live": True,
            "alarms": [{"id": "alarm-1", "at": self.now.isoformat(), "note": "test"}],
        })
        reports = []

        async def runner(request, on_accepted, tools, interrupted):
            reports.append(request["report"])
            await on_accepted(request["native_thread_id"])
            return {"accepted": True, "completed": True, "substantive": True}

        self.engine.runner = runner
        result = await self.engine.tick()
        self.assertTrue(result["completed"])
        self.assertIn("raw timestamp note", reports[0])
        state = await self.store.get_setting("collar_companion")
        current = next(s for s in state["scenes"] if s["id"] == self.scene["id"])
        self.assertEqual(current["nudge_wakes_left"], 2)

    async def test_legacy_scene_note_without_version_can_still_be_consumed(self):
        state = await self.store.get_setting("collar_companion")
        scene = next(s for s in state["scenes"] if s["id"] == self.scene["id"])
        scene.update(nudge="legacy note", nudge_at=self.now.isoformat(),
                     nudge_wakes_left=1)
        scene.pop("nudge_version", None)
        await self.store.set_setting("collar_companion", state)
        _, refs = report.render_report(now=self.now, full=True, trigger="companion_scene",
                                       scenes=state["scenes"])
        self.assertIsNone(refs[0]["nudge_version"])
        await self.store.accept_wake("legacy-note", "thread-1")
        await receipts.complete_wake(self.store, "legacy-note", refs, ok=True, substantive=True)
        result = await self.store.get_setting("collar_companion")
        current = next(s for s in result["scenes"] if s["id"] == self.scene["id"])
        self.assertEqual(current["nudge"], "")

    async def test_retired_personal_tool_and_state_are_not_exposed(self):
        self.assertNotIn("personal_activity", [s["name"] for s in self.engine.tool_schemas])
        await self.store.set_setting("personal_activity", {
            "thoughts": [{"title": "legacy secret"}],
            "activity": {"title": "legacy progress"},
        })
        with self.assertRaises(ValueError):
            await self.engine.tool("personal_activity", {"action": "status"}, "chat-1")
        rendered, _ = report.render_report(now=self.now, full=True, trigger="alarm", scenes=[])
        self.assertNotIn("legacy secret", rendered)
        self.assertNotIn("legacy progress", rendered)
        self.assertEqual(await self.store.get_setting("personal_activity"), {
            "thoughts": [{"title": "legacy secret"}],
            "activity": {"title": "legacy progress"},
        })
