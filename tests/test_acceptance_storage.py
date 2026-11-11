"""Black-box acceptance tests for persistent scheduling and wake receipts."""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from deamo_wake.storage import SQLiteStore


STAMP = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)


class SQLiteStorageAcceptance(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.path = Path(self.tempdir.name) / "wake.sqlite3"
        self.store = SQLiteStore(self.path, now=lambda: STAMP)
        await self.store.initialize()

    async def asyncTearDown(self):
        await self.store.close()
        self.tempdir.cleanup()

    async def test_settings_and_receipts_survive_reopen(self):
        await self.store.set_setting("state", {"items": [1, "two"]})
        await self.store.upsert_conversation("c1", native_thread_id="t1")
        await self.store.record_user_message("c1", "m1", STAMP)
        await self.store.accept_wake("w1", "t1")
        await self.store.mark_notified("w1")
        await self.store.finish_wake("w1", completed=True, substantive=True)
        await self.store.close()

        self.store = SQLiteStore(self.path, now=lambda: STAMP)
        await self.store.initialize()
        self.assertEqual(await self.store.get_setting("state"), {"items": [1, "two"]})
        self.assertEqual(await self.store.get_latest_wake_eligible_conversation(), {
            "id": "c1", "native_thread_id": "t1", "last_user_at": STAMP.isoformat(timespec="microseconds")
        })
        receipt = await self.store.read_wake("w1")
        self.assertEqual(receipt["thread_id"], "t1")
        self.assertEqual(receipt["completed"], 1)
        self.assertEqual(receipt["notified"], 1)
        self.assertEqual(receipt["substantive"], 1)

    async def test_latest_target_filters_ineligible_sessions_and_orders_stably(self):
        cases = [
            ("older", "to", {}),
            ("newer", "tn", {}),
            ("archived", "ta", {"archived": True}),
            ("disabled", "td", {"eligible": False}),
            ("hidden", "th", {"listable": False}),
            ("nonchat", "tx", {"session_type": "system"}),
            ("unbound", None, {}),
        ]
        for conversation_id, thread_id, flags in cases:
            await self.store.upsert_conversation(conversation_id, native_thread_id=thread_id, **flags)

        await self.store.record_user_message("older", "m-old", STAMP)
        await self.store.record_user_message("newer", "m-new", STAMP)
        # Duplicate IDs do not count as a fresh user turn or alter stable ordering.
        self.assertFalse(await self.store.record_user_message("older", "m-old", STAMP.replace(day=12)))
        await self.store.record_user_message("archived", "m-a", STAMP.replace(day=13))
        await self.store.record_user_message("disabled", "m-d", STAMP.replace(day=14))
        await self.store.record_user_message("hidden", "m-h", STAMP.replace(day=15))
        await self.store.record_user_message("nonchat", "m-x", STAMP.replace(day=16))
        await self.store.record_user_message("unbound", "m-u", STAMP.replace(day=17))

        expected = {"id": "unbound", "native_thread_id": None,
                    "last_user_at": STAMP.replace(day=17).isoformat(timespec="microseconds")}
        # A fresh conversation can be selected before the runner creates its native thread.
        self.assertEqual(await self.store.get_latest_wake_eligible_conversation(), expected)
        self.assertEqual(await self.store.get_latest_wake_eligible_conversation(), expected)
        self.assertIsNone(await self.store.get_wake_eligible_conversation("archived"))
        self.assertIsNone(await self.store.get_wake_eligible_conversation("disabled"))
        self.assertIsNone(await self.store.get_wake_eligible_conversation("hidden"))
        self.assertIsNone(await self.store.get_wake_eligible_conversation("nonchat"))
        self.assertEqual((await self.store.get_wake_eligible_conversation("unbound"))["id"], "unbound")

    async def test_background_thread_binding_does_not_change_user_target_order(self):
        await self.store.upsert_conversation("c1")
        await self.store.upsert_conversation("c2", native_thread_id="t2")
        await self.store.record_user_message("c1", "m1", STAMP)
        await self.store.record_user_message("c2", "m2", STAMP.replace(hour=11))

        before = await self.store.get_conversation("c1")
        await self.store.bind_native_thread("c1", "t1")
        after = await self.store.get_conversation("c1")

        self.assertEqual(before["last_user_at"], after["last_user_at"])
        self.assertEqual(before["last_user_order"], after["last_user_order"])
        self.assertEqual((await self.store.get_latest_wake_eligible_conversation())["id"], "c1")

    async def test_receipt_acceptance_thread_identity_completion_and_notification_are_separate(self):
        self.assertIsNone(await self.store.read_wake("w1"))
        with self.assertRaises(ValueError):
            await self.store.accept_wake("w1", "")

        await self.store.accept_wake("w1", "t1")
        first = await self.store.read_wake("w1")
        self.assertEqual(first["completed"], 0)
        self.assertEqual(first["notified"], 0)
        self.assertEqual(await self.store.wake_count("t1"), 1)
        with self.assertRaises(ValueError):
            await self.store.accept_wake("w1", "t2")

        await self.store.mark_notified("w1")
        notified = await self.store.read_wake("w1")
        self.assertEqual(notified["notified"], 1)
        self.assertEqual(notified["completed"], 0)

        await self.store.finish_wake("w1", completed=True, substantive=True)
        done = await self.store.read_wake("w1")
        self.assertEqual(done["completed"], 1)
        self.assertEqual(done["notified"], 1)
        self.assertEqual(done["thread_id"], "t1")

        await self.store.finish_wake("w1", completed=False, error="late transport error")
        after_late_error = await self.store.read_wake("w1")
        self.assertEqual(after_late_error["completed"], 1)
        self.assertEqual(after_late_error["notified"], 1)
        self.assertEqual(after_late_error["thread_id"], "t1")

    async def test_duplicate_acceptance_is_idempotent(self):
        await self.store.accept_wake("w1", "t1")
        accepted_at = (await self.store.read_wake("w1"))["accepted_at"]
        await self.store.accept_wake("w1", "t1")
        self.assertEqual(await self.store.wake_count("t1"), 1)
        self.assertEqual((await self.store.read_wake("w1"))["accepted_at"], accepted_at)
