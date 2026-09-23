"""taskq：SQLite 后端的最小任务队列（基线版）。

能做：入队、FIFO 领取一条、完成、失败、统计。
还不能做：租约、重试、退避、死信、幂等入队、并发安全领取。
"""
from __future__ import annotations

import json
import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    queue      TEXT NOT NULL,
    payload    TEXT NOT NULL,
    state      TEXT NOT NULL DEFAULT 'pending',
    attempts   INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_queue_state ON tasks(queue, state, id);
"""

STATES = ("pending", "running", "done", "failed")


class TaskQueue:
    def __init__(self, path: str = ":memory:"):
        self.path = path
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)

    def enqueue(self, queue: str, payload) -> int:
        cur = self.db.execute(
            "INSERT INTO tasks(queue, payload, state, created_at) VALUES (?,?,?,?)",
            (queue, json.dumps(payload, ensure_ascii=False), "pending", time.time()),
        )
        self.db.commit()
        return int(cur.lastrowid)

    def dequeue(self, queue: str):
        row = self.db.execute(
            "SELECT * FROM tasks WHERE queue=? AND state='pending' ORDER BY id LIMIT 1",
            (queue,),
        ).fetchone()
        if row is None:
            return None
        self.db.execute(
            "UPDATE tasks SET state='running', attempts=attempts+1 WHERE id=?", (row["id"],)
        )
        self.db.commit()
        return {"id": row["id"], "payload": json.loads(row["payload"]), "attempts": row["attempts"] + 1}

    def complete(self, task_id: int) -> bool:
        cur = self.db.execute("UPDATE tasks SET state='done' WHERE id=?", (task_id,))
        self.db.commit()
        return cur.rowcount == 1

    def fail(self, task_id: int, error: str = "") -> bool:
        cur = self.db.execute(
            "UPDATE tasks SET state='failed', last_error=? WHERE id=?", (error, task_id)
        )
        self.db.commit()
        return cur.rowcount == 1

    def get(self, task_id: int):
        row = self.db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["payload"] = json.loads(d["payload"])
        return d

    def stats(self, queue: str | None = None) -> dict:
        sql = "SELECT state, COUNT(*) AS n FROM tasks"
        args: tuple = ()
        if queue is not None:
            sql += " WHERE queue=?"
            args = (queue,)
        sql += " GROUP BY state"
        counts = {s: 0 for s in STATES}
        for row in self.db.execute(sql, args):
            counts[row["state"]] = row["n"]
        counts["total"] = sum(counts.values())
        return counts

    def close(self) -> None:
        self.db.close()
