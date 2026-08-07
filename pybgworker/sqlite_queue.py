import sqlite3
import json
from datetime import timedelta
from .queue import BaseQueue
from .config import DB_PATH, WORKER_TIMEOUT
from .utils import now, get_conn
from .state import validate_transition


class SQLiteQueue(BaseQueue):

    def __init__(self, db_path=None):
        self.db_path = db_path
        self._init_db()

    # ---------------- DB init ----------------

    def _init_db(self):
        with get_conn(self.db_path) as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                name TEXT,
                args TEXT,
                kwargs TEXT,
                status TEXT,
                attempt INTEGER,
                max_retries INTEGER,
                run_at TEXT,
                priority INTEGER DEFAULT 5,
                -- locked_by references workers.name (no formal FK so SQLite
                -- busy_timeout / WAL semantics are unaffected).  fetch_next
                -- uses a LEFT JOIN on workers.name to detect stale locks.
                -- If you add cascading behaviour (e.g. auto-release on worker
                -- deletion) add a proper FOREIGN KEY + PRAGMA foreign_keys=ON.
                locked_by TEXT,
                locked_at TEXT,
                last_error TEXT,
                result TEXT,
                created_at TEXT,
                updated_at TEXT,
                finished_at TEXT,
                -- progress: optional in-task progress tracking (0-100).
                -- Written by the worker subprocess via set_progress().
                progress TEXT DEFAULT NULL,
                -- idempotency_key: unique client-supplied key for deduplication.
                -- A duplicate .delay() call with the same key returns the
                -- existing AsyncResult instead of inserting a second row.
                idempotency_key TEXT DEFAULT NULL
            )
            """)

            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_priority_runat
            ON tasks(status, priority, run_at)
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS workers (
                name TEXT PRIMARY KEY,
                last_seen TEXT
            )
            """)

            conn.commit()

        # Add new columns to databases created before these columns existed.
        self._migrate_schema()

    def _migrate_schema(self):
        """Add columns introduced in later versions to pre-existing databases.

        Uses ``ALTER TABLE … ADD COLUMN`` which is a no-op-equivalent when the
        column is already present (guarded by catching the OperationalError that
        SQLite raises for duplicate column names).
        """
        migrations = [
            "ALTER TABLE tasks ADD COLUMN progress TEXT DEFAULT NULL",
            "ALTER TABLE tasks ADD COLUMN idempotency_key TEXT DEFAULT NULL",
        ]
        with get_conn(self.db_path) as conn:
            for sql in migrations:
                try:
                    conn.execute(sql)
                except sqlite3.OperationalError:
                    # Column already exists — safe to ignore.
                    pass
            # Partial unique index for idempotency (idempotent CREATE).
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idempotency_key
                ON tasks(idempotency_key)
                WHERE idempotency_key IS NOT NULL
            """)
            conn.commit()

    # ---------------- enqueue ----------------

    def enqueue(self, task):
        """Insert a single task row.

        If ``task`` contains an ``idempotency_key`` that already exists in the
        database the INSERT is silently skipped and this method returns the
        *existing* task id so the caller can build an ``AsyncResult`` from it.

        Args:
            task (dict): Task dict with exactly the columns defined in the
                schema (id, name, args, kwargs, status, attempt, max_retries,
                run_at, priority, locked_by, locked_at, last_error, result,
                created_at, updated_at, finished_at, progress, idempotency_key).

        Returns:
            str: The task id that was inserted (or the pre-existing id if a
            duplicate idempotency key was detected).
        """
        idempotency_key = task.get("idempotency_key")
        with get_conn(self.db_path) as conn:
            if idempotency_key:
                # Check for an existing task with this key first.
                row = conn.execute(
                    "SELECT id FROM tasks WHERE idempotency_key=?",
                    (idempotency_key,)
                ).fetchone()
                if row:
                    return row[0]  # Duplicate — return existing id.

            placeholders = ",".join(["?"] * len(task))
            conn.execute(
                f"INSERT INTO tasks VALUES ({placeholders})",
                tuple(task.values())
            )
            conn.commit()
            return task["id"]

    # ---------------- batch enqueue ----------------

    def enqueue_many(self, tasks):
        """Insert multiple task rows in a single transaction.

        This is significantly faster than calling ``enqueue()`` in a loop for
        large batches because it avoids per-row connection overhead and wraps
        all INSERTs in one ``BEGIN IMMEDIATE`` commit.

        Idempotency keys are respected: rows whose key already exists are
        silently skipped (``INSERT OR IGNORE``).

        Args:
            tasks: An iterable of task dicts in the same format as ``enqueue()``.

        Returns:
            int: Number of rows actually inserted (skipped duplicates not counted).
        """
        tasks = list(tasks)
        if not tasks:
            return 0

        placeholders = ",".join(["?"] * len(tasks[0]))
        inserted = 0

        with get_conn(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            for task in tasks:
                cur = conn.execute(
                    f"INSERT OR IGNORE INTO tasks VALUES ({placeholders})",
                    tuple(task.values())
                )
                inserted += cur.rowcount
            conn.commit()

        return inserted

    # ---------------- atomic fetch ----------------

    def fetch_next(self, worker):
        stale_time = (now() - timedelta(seconds=WORKER_TIMEOUT)).isoformat()

        with get_conn(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            row = conn.execute("""
                UPDATE tasks
                SET status='running',
                    locked_by=?,
                    locked_at=?,
                    updated_at=?
                WHERE id = (
                    SELECT t.id FROM tasks t
                    LEFT JOIN workers w ON t.locked_by = w.name
                    WHERE
                        (
                            t.status IN ('queued','retrying')
                            OR
                            -- Bug 6 fix: treat NULL last_seen (no heartbeat row
                            -- ever written by that worker) as stale, so ghost
                            -- locks from workers that crashed before their first
                            -- heartbeat are reclaimed like any other stale lock.
                            (t.status='running' AND (w.last_seen IS NULL OR w.last_seen < ?))
                        )
                    AND t.run_at <= ?
                    ORDER BY t.priority ASC, t.run_at ASC
                    LIMIT 1
                )
                RETURNING *
            """, (
                worker,
                now().isoformat(),
                now().isoformat(),
                stale_time,
                now().isoformat()
            )).fetchone()

            conn.commit()
            return dict(row) if row else None

    # ---------------- ack ----------------

    def ack(self, task_id):
        with get_conn(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()
            if row:
                validate_transition(row["status"], "success")
            conn.execute("""
                UPDATE tasks
                SET status='success',
                    finished_at=?,
                    updated_at=?,
                    locked_by=NULL,
                    locked_at=NULL
                WHERE id=?
            """, (now().isoformat(), now().isoformat(), task_id))
            conn.commit()

    # ---------------- fail ----------------

    def fail(self, task_id, error):
        with get_conn(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()
            if row:
                validate_transition(row["status"], "failed")
            conn.execute("""
                UPDATE tasks
                SET status='failed',
                    last_error=?,
                    finished_at=?,
                    updated_at=?,
                    locked_by=NULL,
                    locked_at=NULL
                WHERE id=?
            """, (error, now().isoformat(), now().isoformat(), task_id))
            conn.commit()

    # ---------------- dead ----------------

    def dead(self, task_id, error):
        with get_conn(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()
            if row:
                validate_transition(row["status"], "dead")
            conn.execute("""
                UPDATE tasks
                SET status='dead',
                    last_error=?,
                    finished_at=?,
                    updated_at=?,
                    locked_by=NULL,
                    locked_at=NULL
                WHERE id=?
            """, (error, now().isoformat(), now().isoformat(), task_id))
            conn.commit()

    # ---------------- retry ----------------

    def reschedule(self, task_id, delay):
        run_at = now() + timedelta(seconds=delay)
        with get_conn(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()
            if row:
                validate_transition(row["status"], "retrying")
            conn.execute("""
                UPDATE tasks
                SET status='retrying',
                    attempt=attempt+1,
                    run_at=?,
                    updated_at=?,
                    locked_by=NULL,
                    locked_at=NULL
                WHERE id=?
            """, (run_at.isoformat(), now().isoformat(), task_id))
            conn.commit()

    # ---------------- cancel ----------------

    def cancel(self, task_id):
        with get_conn(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()
            if row:
                validate_transition(row["status"], "cancelled")
            conn.execute("""
                UPDATE tasks
                SET status='cancelled',
                    finished_at=?,
                    updated_at=?,
                    locked_by=NULL,
                    locked_at=NULL
                WHERE id=?
            """, (now().isoformat(), now().isoformat(), task_id))
            conn.commit()

    # ---------------- progress ----------------

    def set_progress(self, task_id, percent, message=None):
        """Update the progress column for a running task.

        Args:
            task_id: The task to update.
            percent: Integer 0–100 (clamped automatically).
            message: Optional human-readable status string.
        """
        percent = max(0, min(100, int(percent)))
        payload = json.dumps({"percent": percent, "message": message})
        with get_conn(self.db_path) as conn:
            conn.execute(
                "UPDATE tasks SET progress=? WHERE id=?",
                (payload, task_id)
            )
            conn.commit()

    # ---------------- maintenance ----------------

    def cleanup(
        self,
        retention_days,
        vacuum=True,
    ):
        if retention_days <= 0:
            return {
                "deleted": 0,
                "deleted_finished": 0,
                "vacuumed": False,
                "locked": False,
            }

        deleted_finished = 0
        locked = False

        retention_cutoff = None

        if retention_days > 0:
            retention_cutoff = (now() - timedelta(days=retention_days)).isoformat()

        with get_conn(self.db_path) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError:
                locked = True
            else:
                if retention_cutoff:
                    cur = conn.execute("""
                        DELETE FROM tasks
                        WHERE finished_at IS NOT NULL
                        AND finished_at < ?
                    """, (retention_cutoff,))
                    deleted_finished = cur.rowcount

                conn.commit()

        deleted = deleted_finished

        vacuumed = False
        if vacuum and deleted > 0 and not locked:
            with get_conn(self.db_path) as conn:
                try:
                    conn.execute("VACUUM")
                    conn.commit()
                    vacuumed = True
                except sqlite3.OperationalError:
                    vacuumed = False

        return {
            "deleted": deleted,
            "deleted_finished": deleted_finished,
            "vacuumed": vacuumed,
            "locked": locked,
        }
