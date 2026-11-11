import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from deamo_wake.report import render_report
from deamo_wake.storage import SQLiteStore


class StorageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(Path(self.temp.name) / "state.sqlite3",
                                 now=lambda: datetime(2026, 1, 2, tzinfo=timezone.utc))
        await self.store.initialize()

    async def asyncTearDown(self):
        await self.store.close()
        self.temp.cleanup()

    async def test_user_message_dedupe_and_latest_target(self):
        await self.store.upsert_conversation("old", native_thread_id="t-old")
        await self.store.upsert_conversation("new")  # Native thread may bind later.
        t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 2, tzinfo=timezone.utc)
        self.assertTrue(await self.store.record_user_message("old", "m1", t2))
        self.assertFalse(await self.store.record_user_message("old", "m1", t2))
        self.assertTrue(await self.store.record_user_message("old", "older", t1))
        self.assertEqual((await self.store.get_latest_wake_eligible_conversation())["id"], "old")
        await self.store.record_user_message("new", "m2", t2)
        self.assertEqual((await self.store.get_latest_wake_eligible_conversation())["id"], "new")
        await self.store.bind_native_thread("new", "native-new")
        self.assertEqual((await self.store.get_wake_eligible_conversation("new"))["native_thread_id"], "native-new")

    async def test_filtering_and_settings_are_isolated_json(self):
        await self.store.upsert_conversation("hidden", eligible=False, native_thread_id="x")
        await self.store.record_user_message("hidden", "h", datetime.now(timezone.utc))
        self.assertIsNone(await self.store.get_wake_eligible_conversation("hidden"))
        value = {"items": [1]}
        await self.store.set_setting("state", value)
        value["items"].append(2)
        self.assertEqual(await self.store.get_setting("state"), {"items": [1]})

    async def test_receipt_acceptance_is_idempotent_and_monotonic(self):
        await self.store.accept_wake("w1", "native")
        await self.store.accept_wake("w1", "native")
        self.assertEqual(await self.store.wake_count("native"), 1)
        with self.assertRaises(ValueError):
            await self.store.accept_wake("w1", "different")
        await self.store.mark_notified("w1")
        await self.store.finish_wake("w1", completed=True, substantive=True)
        receipt = await self.store.read_wake("w1")
        self.assertEqual((receipt["completed"], receipt["notified"], receipt["substantive"]), (1, 1, 1))

    async def test_duplicate_message_closes_transaction_and_releases_database(self):
        stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
        await self.store.upsert_conversation("c")
        await self.store.record_user_message("c", "m", stamp)
        self.assertFalse(await self.store.record_user_message("c", "m", stamp))
        self.assertFalse(self.store._conn.in_transaction)
        other = sqlite3.connect(self.store.path, timeout=0.1)
        try:
            other.execute("INSERT INTO settings VALUES('other', '{}')")
            other.commit()
        finally:
            other.close()


class ReportTests(unittest.TestCase):
    def test_projection_returns_only_displayed_scene_note_refs(self):
        now = datetime(2026, 1, 2, tzinfo=timezone.utc)
        scene = {"id": "s", "type": "custom", "status": "active", "title": "Example",
                 "started_at": now.isoformat(), "nudge": "scene note", "nudge_wakes_left": 2}
        text, refs = render_report(now=now, full=True, trigger="companion_scene", scenes=[scene])
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0]["owner"], "scene")
        self.assertIn("scene note", text)
        self.assertNotIn("thought", text.lower())

    def test_short_projection_and_expired_scene(self):
        now = datetime(2026, 1, 2, tzinfo=timezone.utc)
        scenes = [{"id": "expired", "status": "active", "expires_at": "2026-01-01T00:00:00Z"}]
        text, refs = render_report(now=now, full=False, trigger="alarm", scenes=scenes)
        self.assertEqual(refs, [])
        self.assertNotIn("<scenes>", text)


if __name__ == "__main__":
    unittest.main()
