"""Run a full local message -> alarm -> native receipt -> explicit notify cycle."""

import asyncio
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from deamo_wake.engine import WakeEngine
from deamo_wake.storage import SQLiteStore


async def main():
    clock = [datetime.now(timezone.utc)]
    sent = []

    async def local_runner(request, on_accepted, tools, interrupted):
        # Replace this callable with an external native runtime in an application.
        await on_accepted(request["native_thread_id"] or "local-native-thread")
        if interrupted.is_set():
            return {"accepted": True, "completed": False, "error": "interrupted"}
        await tools("notify_user", {"message": "A local wake was delivered."})
        return {"accepted": True, "completed": True, "substantive": True,
                "text": "Finished the local wake turn."}

    async def local_notifier(conversation_id, message):
        sent.append((conversation_id, message))
        return {"delivered": True}

    with tempfile.TemporaryDirectory(prefix="deamo-wake-") as tmp:
        store = SQLiteStore(Path(tmp) / "wake.sqlite3", now=lambda: clock[0])
        engine = WakeEngine(store, runner=local_runner, notifier=local_notifier)
        await engine.initialize()
        await engine.user_message("example-chat", "user-1", "Please remind me shortly.", clock[0])
        await store.upsert_conversation("example-chat", native_thread_id="local-native-thread")
        alarm_at = clock[0] + timedelta(minutes=1)
        print(await engine.tool("collar_companion", {
            "action": "set_alarm", "at": alarm_at.isoformat(), "note": "local demo"
        }, "example-chat"))
        await engine.configure(enabled=True, wake_live=True)
        clock[0] = alarm_at + timedelta(seconds=1)
        result = await engine.tick()
        receipt = await store.read_wake(result["wake_id"])
        print(json.dumps({"wake": result, "receipt": receipt, "notifications": sent}, indent=2))
        await engine.close()


if __name__ == "__main__":
    asyncio.run(main())
