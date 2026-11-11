"""Acceptance tests for compiled wake conditions and event matching."""

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from deamo_wake import companion
from deamo_wake.storage import SQLiteStore


class FakeSLM:
    def __init__(self, outputs):
        self.outputs = iter(outputs)

    async def __call__(self, *, system, user_content, max_tokens, temperature):
        return next(self.outputs)


class ConditionAcceptance(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.now = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
        self.store = SQLiteStore(Path(self.tempdir.name) / "wake.sqlite3", now=lambda: self.now)
        await self.store.initialize()

    async def asyncTearDown(self):
        await self.store.close()
        self.tempdir.cleanup()

    async def _create(self, scene_type, text, rule, slm):
        result = await companion.execute_collar_companion(
            self.store,
            {"action": "start_scene", "scene_type": scene_type, "title": scene_type,
             "wake_when": text},
            conversation_id="c1", slm=slm,
            known_places={"home": {"label": "Home", "role": "home"},
                          "office": {"label": "Office", "role": "work"}},
            known_apps=["chat-app"],
        )
        payload = json.loads(result)
        self.assertTrue(payload.get("ok"), payload)
        state = await self.store.get_setting("collar_companion")
        scene = next(s for s in state["scenes"] if s["type"] == scene_type and s["status"] == "active")
        self.assertEqual(scene["wake_condition"]["rule"], rule)
        return scene

    async def test_six_supported_condition_kinds_compile_through_public_tool(self):
        cases = [
            ("outing", "arrive home", {"kind": "arrive", "place": "home", "app": None, "device": "any", "minutes": None, "between": None}),
            ("work", "leave office", {"kind": "leave", "place": "office", "app": None, "device": "any", "minutes": None, "between": None}),
            ("bedtime", "open chat app", {"kind": "app_open", "place": None, "app": "chat-app", "device": "iphone", "minutes": None, "between": None}),
            ("morning", "use chat app for 20 minutes", {"kind": "app_use", "place": None, "app": "chat-app", "device": "iphone", "minutes": 20, "between": None}),
            ("active_without_chat", "device active after quiet", {"kind": "device_active", "place": None, "app": None, "device": "any", "minutes": 30, "between": None}),
            ("silence_escalation", "all devices quiet", {"kind": "all_quiet", "place": None, "app": None, "device": "any", "minutes": 20, "between": None}),
        ]
        slm = FakeSLM([{"ok": True, **rule} for _, _, rule in cases])
        defaults = {
            "outing": (30, 50), "work": (60, 40), "bedtime": (45, 55),
            "morning": (20, 60), "active_without_chat": (30, 45),
            "silence_escalation": (15, 65),
        }
        for scene_type, text, rule in cases:
            with self.subTest(kind=rule["kind"]):
                scene = await self._create(scene_type, text, rule, slm)
                cadence, priority = defaults[scene_type]
                self.assertEqual((scene["cadence_min"], scene["priority"]), (cadence, priority))

    async def test_unknown_place_app_device_and_multiple_rules_are_rejected(self):
        invalid = [
            {"ok": True, "kind": "arrive", "place": "unknown", "device": "any"},
            {"ok": True, "kind": "app_open", "app": "unknown", "device": "iphone"},
            {"ok": True, "kind": "app_open", "app": "chat-app", "device": "watch"},
            {"ok": False, "reason": "choose one event"},
        ]
        for response in invalid:
            slm = FakeSLM([response])
            result = await companion.execute_collar_companion(
                self.store,
                {"action": "start_scene", "scene_type": "custom", "wake_when": "condition"},
                conversation_id="c1", slm=slm,
                known_places={"home": {"label": "Home"}}, known_apps=["chat-app"],
            )
            self.assertIn("error", json.loads(result))

    async def test_condition_event_before_rule_creation_does_not_match(self):
        rule = {"kind": "arrive", "place": "home", "app": None, "device": "any", "minutes": None, "between": None}
        await self._create("outing", "arrive home", rule, FakeSLM([{"ok": True, **rule}]))
        old_event = {"event": "arrive", "detected_at": (self.now - timedelta(minutes=1)).isoformat(),
                     "place": {"name": "home", "label": "Home"}}
        matched = await companion.check_wake_conditions(self.store, event=old_event)
        self.assertFalse(matched)
        state = await self.store.get_setting("collar_companion")
        scene = next(s for s in state["scenes"] if s["type"] == "outing")
        self.assertIsNone(scene["wake_condition"]["met_at"])

    async def test_timer_claim_preserves_unmet_condition_and_failed_match_retries(self):
        rule = {"kind": "arrive", "place": "home", "app": None, "device": "any", "minutes": None, "between": None}
        scene = await self._create("outing", "arrive home", rule, FakeSLM([{"ok": True, **rule}]))
        timer_claim = await companion.claim_due_scene_wake(self.store, now=self.now + timedelta(minutes=31))
        self.assertIsNotNone(timer_claim)
        still_waiting = await self.store.get_setting("collar_companion")
        scene_now = next(s for s in still_waiting["scenes"] if s["id"] == scene["id"])
        self.assertIsNotNone(scene_now.get("wake_condition"))

        event = {"event": "arrive", "at": self.now.isoformat(),
                 "place": {"name": "home", "label": "Home"}}
        self.assertIn(scene["id"], await companion.check_wake_conditions(self.store, event=event))
        claim = await companion.claim_due_scene_wake(self.store, now=self.now)
        self.assertIsNotNone(claim)
        self.assertTrue(await companion.revalidate_scene_wake(self.store, claim))
        await companion.complete_scene_wake(self.store, claim, {"ok": False, "accepted": True, "error": "temporary"})
        after_failure = await self.store.get_setting("collar_companion")
        retained = next(s for s in after_failure["scenes"] if s["id"] == scene["id"])
        self.assertIsNotNone(retained.get("wake_condition"))
        self.assertEqual(retained["next_wake_at"], (self.now + timedelta(minutes=5)).isoformat(timespec="seconds"))

    async def test_snooze_that_arrives_after_claim_blocks_device_scene_before_dispatch(self):
        await companion.execute_collar_companion(
            self.store, {"action": "start_scene", "scene_type": "active_without_chat", "cadence_min": 5},
            conversation_id="c1")
        claim = await companion.claim_due_scene_wake(self.store, now=self.now + timedelta(minutes=6))
        self.assertIsNotNone(claim)
        await self.store.set_setting("collar_wake", {"snooze_until": (self.now + timedelta(hours=1)).isoformat()})
        self.assertFalse(await companion.revalidate_scene_wake(self.store, claim))
        self.assertEqual(claim["due"], [])

    async def test_each_condition_matches_its_corresponding_fresh_observation(self):
        definitions = [
            ("outing", {"kind": "arrive", "place": "home", "app": None, "device": "any", "minutes": None, "between": None}),
            ("work", {"kind": "leave", "place": "office", "app": None, "device": "any", "minutes": None, "between": None}),
            ("bedtime", {"kind": "app_open", "place": None, "app": "chat-app", "device": "iphone", "minutes": None, "between": None}),
            ("morning", {"kind": "app_use", "place": None, "app": "chat-app", "device": "iphone", "minutes": 20, "between": None}),
            ("active_without_chat", {"kind": "device_active", "place": None, "app": None, "device": "any", "minutes": 30, "between": None}),
            ("silence_escalation", {"kind": "all_quiet", "place": None, "app": None, "device": "any", "minutes": 20, "between": None}),
        ]
        base = self.now
        for index, (scene_type, rule) in enumerate(definitions):
            if index:
                await self.store.close()
                self.now = base
                self.store = SQLiteStore(Path(self.tempdir.name) / f"condition-{index}.sqlite3", now=lambda: self.now)
                await self.store.initialize()
            scene = await self._create(scene_type, "condition", rule,
                                       FakeSLM([{"ok": True, **rule}]))
            if rule["kind"] == "arrive":
                matched = await companion.check_wake_conditions(self.store, event={
                    "event": "arrive", "at": self.now.isoformat(),
                    "place": {"name": "home", "label": "Home"}})
            elif rule["kind"] == "leave":
                matched = await companion.check_wake_conditions(self.store, event={
                    "event": "leave", "at": self.now.isoformat(),
                    "place": {"name": "office", "label": "Office"}})
            elif rule["kind"] == "app_open":
                matched = await companion.check_wake_conditions(self.store, event={
                    "event": "open", "at": self.now.isoformat(), "device": "iphone", "app": "chat-app"})
            elif rule["kind"] == "app_use":
                self.now += timedelta(minutes=25)
                phone = {"current_app": {"iphone": {
                    "app": "chat-app", "since": base.isoformat(), "stale": False}}}
                matched = await companion.check_wake_conditions(
                    self.store, device_state={"phone": phone, "presence": {"devices": {}}})
            elif rule["kind"] == "device_active":
                previous = {"iphone": {"last_used": (self.now - timedelta(minutes=60)).isoformat(),
                                        "active": False}}
                matched = await companion.check_wake_conditions(
                    self.store,
                    presence_event={"device": "iphone", "semantic": {"active": True}},
                    device_state={"previous_usage": previous, "phone": {}, "presence": {"devices": {}}})
            else:
                self.now += timedelta(minutes=25)
                device_state = {
                    "phone": {"last_event": {"at": (base - timedelta(minutes=30)).isoformat(), "age_min": 30},
                              "current_app": {}},
                    "presence": {"devices": {"macbook": {"online": False}}},
                }
                matched = await companion.check_wake_conditions(self.store, device_state=device_state)
            with self.subTest(kind=rule["kind"]):
                self.assertIn(scene["id"], matched)

    async def test_phone_scene_recreation_pause_expires(self):
        original = await companion.start_scene(
            self.store, scene_type="morning", source="model", conversation_id="c1")
        await companion.end_scene(self.store, scene_id=original["id"],
                                  reason="done", recreate_after_minutes=30)
        paused = await companion.start_scene(
            self.store, scene_type="morning", source="phone_event", conversation_id="c1")
        self.assertIsNone(paused)
        self.now += timedelta(minutes=31)
        recreated = await companion.start_scene(
            self.store, scene_type="morning", source="phone_event", conversation_id="c1")
        self.assertIsNotNone(recreated)
        self.assertNotEqual(recreated["id"], original["id"])
        self.assertGreater(datetime.fromisoformat(recreated["next_wake_at"]), self.now)

    async def test_bedtime_sleep_wake_is_claimed_once_and_retried_after_backoff(self):
        await companion.start_scene(self.store, scene_type="bedtime", conversation_id="c1")
        first = await companion.claim_due_scene_wake(self.store, now=self.now, asleep=True)
        self.assertEqual(first["wake_reason"], "bedtime_asleep_confirmed")
        self.assertEqual(len(first["due"]), 1)
        first_claim_id = first["asleep_claim"]["claim_id"]
        await companion.complete_scene_wake(self.store, first, {"ok": False, "error": "temporary"})
        self.assertIsNone(await companion.claim_due_scene_wake(
            self.store, now=self.now + timedelta(seconds=30), asleep=True))
        self.now += timedelta(minutes=1)
        second = await companion.claim_due_scene_wake(
            self.store, now=self.now, asleep=True)
        self.assertNotEqual(second["asleep_claim"]["claim_id"], first_claim_id)
        await companion.complete_scene_wake(self.store, second, {"ok": False, "error": "temporary again"})
        self.assertIsNone(await companion.claim_due_scene_wake(
            self.store, now=self.now + timedelta(minutes=1), asleep=True))
        self.now += timedelta(minutes=2)
        third = await companion.claim_due_scene_wake(
            self.store, now=self.now, asleep=True)
        self.assertIsNotNone(third)

    async def test_sleep_claim_lease_expiry_allows_reclaim(self):
        await companion.start_scene(self.store, scene_type="bedtime", conversation_id="c1")
        first = await companion.claim_due_scene_wake(self.store, now=self.now, asleep=True)
        first_id = first["asleep_claim"]["claim_id"]
        self.assertIsNone(await companion.claim_due_scene_wake(
            self.store, now=self.now + timedelta(seconds=299), asleep=True))
        lease_retry = await companion.claim_due_scene_wake(
            self.store, now=self.now + timedelta(seconds=300), asleep=True)
        self.assertNotEqual(lease_retry["asleep_claim"]["claim_id"], first_id)

    async def test_sustained_presence_after_sleep_confirmation_ends_bedtime_scene(self):
        scene = await companion.start_scene(self.store, scene_type="bedtime", conversation_id="c1")
        await companion.claim_due_scene_wake(self.store, now=self.now, asleep=True)
        base = self.now
        for minute in range(0, 32, 2):
            at = base + timedelta(minutes=minute)
            self.now = at
            result = await companion.handle_presence_event(
                self.store, {"device": "iphone", "source": "watch"},
                {"stored": True, "received_at": at.isoformat(),
                 "semantic": {"active": True, "effective_state": "active", "app": "chat-app"},
                 "changed": minute == 0},
                device_state={"phone": {}, "presence": {"devices": {}}})
        self.assertEqual(result["ended"][0]["id"], scene["id"])
        state = await self.store.get_setting("collar_companion")
        current = next(s for s in state["scenes"] if s["id"] == scene["id"])
        self.assertEqual(current["status"], "ended")

    async def test_replacing_and_clearing_rule_changes_future_matches(self):
        arrive = {"kind": "arrive", "place": "home", "app": None, "device": "any", "minutes": None, "between": None}
        leave = {"kind": "leave", "place": "office", "app": None, "device": "any", "minutes": None, "between": None}
        await self._create("outing", "arrive home", arrive,
                           FakeSLM([{"ok": True, **arrive}]))
        updated = await companion.execute_collar_companion(
            self.store, {"action": "update_scene", "scene_type": "outing", "wake_when": "leave office"},
            conversation_id="c1", slm=FakeSLM([{"ok": True, **leave}]),
            known_places={"home": {"label": "Home"}, "office": {"label": "Office"}},
            known_apps=[])
        self.assertTrue(json.loads(updated).get("ok"))
        old_event = {"event": "arrive", "at": self.now.isoformat(), "place": {"name": "home"}}
        self.assertFalse(await companion.check_wake_conditions(self.store, event=old_event))
        new_event = {"event": "leave", "at": self.now.isoformat(), "place": {"name": "office"}}
        self.assertTrue(await companion.check_wake_conditions(self.store, event=new_event))

        cleared = await companion.execute_collar_companion(
            self.store, {"action": "update_scene", "scene_type": "outing", "wake_when": ""},
            conversation_id="c1", slm=FakeSLM([]), known_places={}, known_apps=[])
        self.assertTrue(json.loads(cleared).get("ok"))
        state = await self.store.get_setting("collar_companion")
        scene = next(s for s in state["scenes"] if s["type"] == "outing")
        self.assertIsNone(scene.get("wake_condition"))
