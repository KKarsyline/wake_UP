"""Small SQLite persistence for wake state and session routing."""

from __future__ import annotations

import json
import sqlite3
import asyncio
from datetime import datetime, timezone
from pathlib import Path


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


class SQLiteStore:
    def __init__(self, path: str | Path, now=None):
        self.path = str(path)
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.state_lock = asyncio.Lock()
        self._conn: sqlite3.Connection | None = None

    async def initialize(self) -> None:
        if self._conn is not None:
            return
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript("""
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY, value_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS conversations (
                conversation_id TEXT PRIMARY KEY,
                native_thread_id TEXT,
                eligible INTEGER NOT NULL DEFAULT 1,
                archived INTEGER NOT NULL DEFAULT 0,
                listable INTEGER NOT NULL DEFAULT 1,
                session_type TEXT NOT NULL DEFAULT 'chat',
                last_user_at TEXT,
                last_user_order INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS user_messages (
                conversation_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                at TEXT NOT NULL,
                PRIMARY KEY (conversation_id, message_id),
                FOREIGN KEY (conversation_id) REFERENCES conversations(conversation_id)
            );
            CREATE TABLE IF NOT EXISTS personal_wake_receipts (
                wake_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                accepted_at TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0,
                notified INTEGER NOT NULL DEFAULT 0,
                substantive INTEGER NOT NULL DEFAULT 0,
                error TEXT
            );
            CREATE INDEX IF NOT EXISTS conversations_latest_user
                ON conversations(last_user_at DESC, last_user_order DESC);
            CREATE INDEX IF NOT EXISTS receipts_thread ON personal_wake_receipts(thread_id);
        """)
        self._conn.commit()

    def _db(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("SQLiteStore.initialize() must be called first")
        return self._conn

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    async def get_setting(self, key: str):
        row = self._db().execute("SELECT value_json FROM settings WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    async def set_setting(self, key: str, value) -> None:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        with self._db() as db:
            db.execute("INSERT INTO settings VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
                       (key, encoded))

    async def upsert_conversation(self, conversation_id: str, *, native_thread_id=None,
                                 eligible=True, archived=False, listable=True,
                                 session_type="chat") -> None:
        with self._db() as db:
            db.execute("""INSERT INTO conversations
            (conversation_id,native_thread_id,eligible,archived,listable,session_type)
            VALUES(?,?,?,?,?,?) ON CONFLICT(conversation_id) DO UPDATE SET
            native_thread_id=COALESCE(excluded.native_thread_id, conversations.native_thread_id),
            eligible=excluded.eligible, archived=excluded.archived,
            listable=excluded.listable, session_type=excluded.session_type""",
                (conversation_id, native_thread_id, int(eligible), int(archived), int(listable), session_type))

    async def record_user_message(self, conversation_id: str, message_id: str, at: datetime) -> bool:
        db = self._db()
        stamp = _iso(at)
        with db:
            cursor = db.execute("INSERT INTO user_messages VALUES(?,?,?) ON CONFLICT(conversation_id,message_id) DO NOTHING",
                                (conversation_id, message_id, stamp))
            if cursor.rowcount == 0:
                return False
            db.execute("""UPDATE conversations SET last_user_at=?, last_user_order=?
                WHERE conversation_id=? AND (last_user_at IS NULL OR last_user_at <= ?)""",
                (stamp, cursor.lastrowid, conversation_id, stamp))
            return True

    async def latest_user_message_at(self):
        """Latest real user turn across every conversation, even hidden ones."""
        row = self._db().execute("SELECT MAX(at) FROM user_messages").fetchone()
        return row[0] if row and row[0] else None

    @staticmethod
    def _conversation(row):
        if row is None:
            return None
        return {"id": row["conversation_id"], "native_thread_id": row["native_thread_id"],
                "last_user_at": row["last_user_at"]}

    async def get_wake_eligible_conversation(self, conversation_id: str):
        row = self._db().execute("""SELECT * FROM conversations WHERE conversation_id=?
            AND eligible=1 AND archived=0 AND listable=1 AND session_type='chat'
            AND last_user_at IS NOT NULL""", (conversation_id,)).fetchone()
        return self._conversation(row)

    async def get_latest_wake_eligible_conversation(self, latest_user=True):
        row = self._db().execute("""SELECT * FROM conversations WHERE eligible=1 AND archived=0
            AND listable=1 AND session_type='chat' AND last_user_at IS NOT NULL
            ORDER BY last_user_at DESC, last_user_order DESC LIMIT 1""").fetchone()
        return self._conversation(row)

    async def get_conversation(self, conversation_id: str):
        row = self._db().execute("SELECT * FROM conversations WHERE conversation_id=?", (conversation_id,)).fetchone()
        return dict(row) if row else None

    async def bind_native_thread(self, conversation_id: str, thread_id: str) -> None:
        with self._db() as db:
            db.execute("UPDATE conversations SET native_thread_id=? WHERE conversation_id=?",
                       (thread_id, conversation_id))

    async def wake_count(self, thread_id: str) -> int:
        return self._db().execute("SELECT COUNT(*) FROM personal_wake_receipts WHERE thread_id=?",
                                  (thread_id,)).fetchone()[0]

    async def accept_wake(self, wake_id: str, thread_id: str) -> None:
        if not thread_id:
            raise ValueError("accepted wake requires native thread id")
        with self._db() as db:
            row = db.execute("SELECT thread_id FROM personal_wake_receipts WHERE wake_id=?", (wake_id,)).fetchone()
            if row and row[0] != thread_id:
                raise ValueError("wake already belongs to another native thread")
            db.execute("INSERT OR IGNORE INTO personal_wake_receipts(wake_id,thread_id,accepted_at) VALUES(?,?,?)",
                       (wake_id, thread_id, _iso(self.now())))

    async def read_wake(self, wake_id: str):
        row = self._db().execute("SELECT * FROM personal_wake_receipts WHERE wake_id=?", (wake_id,)).fetchone()
        return dict(row) if row else None

    async def finish_wake(self, wake_id: str, *, completed=True, substantive=False, error=None) -> None:
        # A completion cannot manufacture or erase the earlier acceptance.
        with self._db() as db:
            db.execute("""UPDATE personal_wake_receipts SET
                completed=MAX(completed, ?), substantive=MAX(substantive, ?),
                error=CASE WHEN completed=1 THEN error ELSE COALESCE(?, error) END WHERE wake_id=?""",
                (int(completed), int(substantive), error, wake_id))

    async def mark_notified(self, wake_id: str) -> None:
        with self._db() as db:
            db.execute("UPDATE personal_wake_receipts SET notified=1 WHERE wake_id=?", (wake_id,))
