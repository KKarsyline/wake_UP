"""Exercise message -> scene -> phone condition -> local runner -> receipt."""

import asyncio
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from deamo_wake import WakeEngine
from deamo_wake import companion
from deamo_wake.storage import SQLiteStore


class LocalSLM:
    """Deterministic stand-in for both message classification and condition parsing."""
    def __init__(self):
        self.responses = [
            {"deltas": [{"action": "start", "scene_type": "custom",
                         "facts": ["Wait for chat-app to open on the phone"]}]},
            {"ok": True, "kind": "app_open", "place": None, "app": "chat-app",
             "device": "iphone", "minutes": None, "between": None},
        ]

    async def __call__(self, *, system, user_content, max_tokens, temperature):
        return self.responses.pop(0)


async def main():
    now = [datetime.now(timezone.utc)]
    notifications = []

    async def runner(request, on_accepted, tools, interrupted):
        await on_accepted(request["native_thread_id"] or "local-scene-thread")
        if interrupted.is_set():
            return {"accepted": True, "completed": False, "error": "interrupted"}
        state = await companion.get_companion_status(engine.store)
        scene = state["active_scenes"][0]
        await tools("collar_companion", {"action": "update_scene", "scene_id": scene["id"],
                                          "cadence_min": 20})
        await tools("notify_user", {"message": "The configured app event reached this local wake."})
        return {"accepted": True, "completed": True, "substantive": True,
                "text": "The scene condition was handled."}

    async def notifier(conversation_id, message):
        notifications.append({"conversation_id": conversation_id, "message": message})
        return {"delivered": True}

    with tempfile.TemporaryDirectory(prefix="deamo-scene-wake-") as tmp:
        store = SQLiteStore(Path(tmp) / "state.sqlite3", now=lambda: now[0])
        engine = WakeEngine(store, slm=LocalSLM(), runner=runner, notifier=notifier,
                            known_apps=["chat-app"])
        await engine.initialize()
        await engine.user_message("scene-chat", "user-1", "Please wait for chat-app to open.", now[0])
        created = json.loads(await engine.tool("collar_companion", {
            "action": "start_scene", "scene_type": "custom", "title": "App event",
            "cadence_min": 30, "wake_when": "when chat-app opens on the phone",
        }, "scene-chat"))
        assert created["ok"], created
        await engine.configure(enabled=True, wake_live=True)
        now[0] += timedelta(minutes=1)
        observed = await engine.observe({"kind": "phone", "body": {
            "event": "open", "device": "iphone", "app": "chat-app",
            "event_id": "phone-open-1", "at": now[0].isoformat(),
        }})
        wake = observed.get("wake")
        assert wake and wake.get("completed") and wake.get("notified"), observed
        state = await companion.get_companion_status(store)
        scene = next(s for s in state["recent_scenes"] if s["id"] == created["scene"]["id"])
        receipt = await store.read_wake(wake["wake_id"])
        assert not scene.get("wake_condition"), "completed condition should be consumed"
        print(json.dumps({"observation": observed, "scene": scene, "receipt": receipt,
                          "notifications": notifications}, indent=2))
        await engine.close()


if __name__ == "__main__":
    asyncio.run(main())
