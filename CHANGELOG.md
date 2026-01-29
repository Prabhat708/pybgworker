# Changelog

All notable changes to **PyBgWorker** will be documented in this file.

This project follows **Semantic Versioning (SemVer)**  
and the format is inspired by **Keep a Changelog**.

---

## [0.1.0] - 2026-01-29

### 🎉 Initial Stable Release

This is the first stable open-source release of **PyBgWorker**.

### ✨ Added
- SQLite-based background task queue
- Task decorator with `.delay()` API
- Background worker with safe task locking
- Automatic retry mechanism with configurable retry count
- Retry delay support
- Delayed task execution using `countdown` and `eta`
- Task state persistence (`queued`, `running`, `retrying`, `success`, `failed`)
- Crash-safe recovery of stale tasks
- Task result and error persistence
- `AsyncResult` API for checking task status and results
- Command-line interface (`pybgworker run`)
- Support for multi-language job producers via SQLite
- Example workflows:
  - Basic background task
  - Retry handling
  - Real-world e-commerce pipeline

### 🛠 Fixed
- Race conditions in task locking
- Retry state loss on worker restart
- Inconsistent task scheduling timestamps

### 📚 Documentation
- Comprehensive README with real-world examples
- Usage guidelines and limitations
- Installation and setup instructions

---

## [Unreleased]

### 🚧 Planned
- `AsyncResult.get()` with timeout support
- Exponential backoff for retries
- Periodic (cron-like) tasks
- Additional queue backends (PostgreSQL, Redis)
- Task chaining and pipelines
- Concurrency options (threads / processes)
- Monitoring and inspection commands

---

## 🧭 Versioning Policy

- **MAJOR** version when incompatible API changes are made
- **MINOR** version when functionality is added in a backward-compatible manner
- **PATCH** version when backward-compatible bug fixes are made
