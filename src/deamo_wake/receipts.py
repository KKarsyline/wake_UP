"""Settle note versions displayed in accepted wake reports."""

from . import companion


async def complete_wake(storage, wake_id, note_refs, *, ok, substantive):
    if not ok:
        return
    async with storage.state_lock:
        receipt = await storage.read_wake(wake_id)
        if not receipt or receipt.get("completed"):
            return
        state = await companion._load_state(storage)
        scenes = companion._active_from_state(state, storage.now())
        if substantive:
            for ref in note_refs:
                if ref.get("owner") != "scene":
                    continue
                target = next((scene for scene in scenes
                               if scene.get("id") == ref.get("id")), None)
                if target:
                    companion.consume_nudge(target, expected=ref, receipt=wake_id)
        await companion._save_state(storage, state)
        await storage.finish_wake(wake_id, completed=True, substantive=substantive)
