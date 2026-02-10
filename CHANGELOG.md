# Changelog

All notable changes to this project will be documented in this file.

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
