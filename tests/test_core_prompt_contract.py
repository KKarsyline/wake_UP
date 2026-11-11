"""The classifier prompt example is a live contract with its scene consumer."""

import json
import re
import unittest
from datetime import datetime, timezone

from deamo_wake import companion
from deamo_wake.engine import WakeEngine
from deamo_wake.storage import SQLiteStore


class PromptContractTests(unittest.IsolatedAsyncioTestCase):
    def test_rendered_scene_guidance_uses_registered_tools_only(self):
        rendered = companion._render_scenes(
            [{"type": "custom", "started_at": "2026-11-11T12:00:00+00:00"}],
            datetime(2026, 11, 11, 12, tzinfo=timezone.utc), compact=False)
        registered = {schema["name"] for schema in WakeEngine(None, runner=None).tool_schemas}
        mentioned = {name for name in registered if name in rendered}
        self.assertEqual(mentioned, {"collar_companion", "notify_user"})

    async def test_prompt_example_drives_real_scene_creation(self):
        match = re.search(r"Example:\n(\{.*\})$", companion._SCENE_CLASSIFIER_SYSTEM, re.S)
        self.assertIsNotNone(match)
        response = json.loads(match.group(1))
        delta = response["deltas"][0]
        self.assertIn("scene_type", delta)
        self.assertNotIn("calendar_candidates", response)
        self.assertIn("complaint about feeling tired", companion._SCENE_CLASSIFIER_SYSTEM)
        self.assertIn("no more than four facts", companion._SCENE_CLASSIFIER_SYSTEM)
        self.assertIn("未使用的place、app、minutes或between填null", companion.WAKE_WHEN_TRANSLATOR_SYSTEM)
        self.assertIn("不能从未说明的时长猜测一个数字", companion.WAKE_WHEN_TRANSLATOR_SYSTEM)
        from deamo_wake.scheduler import _AUTO_WAKE_SCANNER_SYSTEM
        self.assertIn("more than one hour", _AUTO_WAKE_SCANNER_SYSTEM)

        class PromptExampleSLM:
            async def __call__(self, *, system, user_content, max_tokens, temperature):
                assert system == companion._SCENE_CLASSIFIER_SYSTEM
                return response

        store = SQLiteStore(":memory:")
        await store.initialize()
        try:
            result = await companion.process_user_message_scene(
                store, PromptExampleSLM(), conversation_id="chat-1",
                message_id="message-1", user_text="I am going to sleep now.")
            self.assertEqual(len(result["deltas"]), 1)
            state = await companion._load_state(store)
            scenes = [s for s in state["scenes"] if s.get("status") == "active"]
            self.assertEqual(scenes[0]["type"], delta["scene_type"])
        finally:
            await store.close()
