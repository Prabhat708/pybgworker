# PyBgWorker 🚀

**PyBgWorker** is a lightweight, SQLite-based background task worker for Python — inspired by Celery, but designed to work **without Redis or RabbitMQ**.

> Run background jobs, retries, and delayed tasks using **only Python + SQLite**.

---

## 🔥 Why PyBgWorker?

Celery is powerful, but often **too heavy** for:
- Small to medium projects
- Windows environments
- Side projects & MVPs
- Systems where Redis/RabbitMQ is not available

**PyBgWorker** focuses on:
- Simplicity
- Reliability
- Minimal infrastructure

---

## ✨ Features

- 🧵 Background task execution
- 🔁 Automatic retries
- ⏱ Delayed execution (`countdown`, `eta`)
- 💥 Failure handling with persistence
- 🗄 SQLite-based queue
- 🔐 Crash-safe task locking
- 📊 Task result & status tracking
- 🔎 `AsyncResult` API (Celery-like)
- 🌍 Multi-language job producers (via SQLite)

---

## ❌ When NOT to Use PyBgWorker

PyBgWorker is **not** a replacement for Celery in all cases.

Do NOT use it if you need:
- ❌ High throughput (10k+ jobs/sec)
- ❌ Multi-node distributed workers
- ❌ Real-time guarantees
- ❌ Advanced routing / fan-out

For those → Celery, Kafka, or RabbitMQ.

---

## 📦 Installation

```bash
pip install pybgworker
