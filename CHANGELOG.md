# Changelog

All notable changes to this project will be documented in this file.

---

## [0.4.0] - 2026-08-07

### Added
- **Task progress reporting**: `set_progress(percent, message)` writes into a new `progress` DB column; `AsyncResult.progress` reads it back as `{"percent": int, "message": str|None}`.
- **Bulk / batch enqueue**: `task.delay_many([(args, kwargs), ...])` and `SQLiteQueue.enqueue_many(tasks)` wrap all INSERTs in a single transaction for fast bulk workloads.
- **Success/failure callbacks**: `@task(on_success=fn, on_failure=fn)` — worker invokes these callables after a terminal state is reached; exceptions are caught and logged.
- **Idempotency keys**: `.delay(idempotency_key="...")` prevents duplicate task rows; a second call with the same key returns the existing `AsyncResult`.
- **Machine-readable CLI output**: `pybgworker inspect --json` and `pybgworker stats --json` emit JSON objects instead of formatted text.
- **`PYBGWORKER_CURRENT_TASK_ID` env var**: set in child subprocesses by the worker so `set_progress()` can find the current task without context injection.
- **`AsyncResult.progress` property**: reads the latest `set_progress()` snapshot.
- **`AsyncResult.dead()` and `AsyncResult.cancelled()` helpers**: companion methods to the existing `successful()` and `failed()`.
- **`AsyncResult.forget()`**: deletes a task row from the database immediately.
- **`_fire_callback` worker helper**: fires `on_success`/`on_failure` safely, logging any exceptions without crashing the worker.
- New `progress.py` module exported as `pybgworker.set_progress`.

### Fixed
- **Scheduler crash** (`sqlite3.ProgrammingError: Incorrect number of bindings`): the cron scheduler was building a task dict with 22 fields — including non-schema TASK_REGISTRY metadata (`retry_delay`, `retry_backoff`, `timeout`, etc.) — and passing it to `enqueue()` which expected exactly 16 DB columns.  The scheduler task dict now contains only the 18 schema columns, matching what `delay()` inserts.
- **Dynamic `enqueue()` placeholders**: replaced the hardcoded 16-`?` INSERT with `len(task)`-based dynamic placeholders, so the method works correctly with both current and any future schema additions.
- `SQLiteQueue._migrate_schema()` added to `ALTER TABLE ADD COLUMN` for `progress` and `idempotency_key` on pre-existing databases without requiring a full schema rebuild.

### Documentation
- Fixed `res.status()` / `res.result()` → `res.status` / `res.result` (properties, not methods) throughout UserGuide.
- Full `AsyncResult` API documented in one place: `status`, `result`, `error`, `progress`, `ready()`, `successful()`, `failed()`, `dead()`, `cancelled()`, `get(timeout)`, `forget()`.
- Added **Environment Variables** reference table to both README and UserGuide.
- Corrected cancel docs: `queued`, `retrying`, and `running` tasks are all cancellable (was incorrectly described as running-only).
- Added `retry_for` documentation with examples and explanation of MRO-based inheritance matching.
- Clarified `rate_limit` as a *start-rate* limit, not a concurrency cap.
- Added **Extending PyBgWorker** section documenting `BaseQueue` / `BaseBackend` as the intended extension points.
- Added **Direct SQLite Access** section for custom monitoring without the CLI.
- Added new feature sections: progress reporting, `delay_many`, callbacks, idempotency keys, `--json` flag.

---

## [0.3.0] - 2026-02-10

### Removed
- Cleanup interval minutes, task TTL, and result TTL options (CLI/env) and related cleanup behavior

### Updated
- Cleanup logic now uses retention-only policy for finished tasks
- README and UserGuide aligned with current capabilities and roadmap
- Tests updated to match current APIs and behavior; Windows-friendly DB cleanup in tests

---

## [0.2.2] - 2026-02-03

### Added
- Task timeouts (per-task timeout support)
- Per-task rate limiting in addition to global rate limit

### Updated
- README: expanded feature list to match current capabilities
- README: clarified roadmap with items not yet included
- Docs consistency with UserGuide

### Internal
- Version bump to 0.2.2

---

## [0.2.1] - 2026-01-31

### Added
- Retry system
- CLI inspect / retry / purge
- Task cancellation
- Worker heartbeat monitoring
- Cron scheduler for recurring tasks
- JSON structured logging
- Task duration tracking
- Rate limiting for overload protection
- Heartbeat event logging
- Crash and timeout tracking

### Improved
- Worker observability
- Logging consistency
- Retry visibility
- Production safety of worker loop

---

## [0.1.0]

### Initial release
- SQLite task queue
- Basic worker execution
- Async task API
