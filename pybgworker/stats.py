from .utils import get_conn
from datetime import datetime, timezone


def stats():
    with get_conn() as conn:
        workers = conn.execute("""
            SELECT name, last_seen FROM workers
        """).fetchall()

        queued = conn.execute("""
            SELECT COUNT(*) FROM tasks
            WHERE status IN ('queued', 'retrying')
        """).fetchone()[0]

    print("\n👷 Worker Stats\n")

    now = datetime.now(timezone.utc)

    for w in workers:
        last_seen = datetime.fromisoformat(w[1])
        delta = (now - last_seen).total_seconds()
        status = "alive" if delta < 15 else "dead"

        print(f"{w[0]:10} {status:5} ({int(delta)}s ago)")

    print(f"\n📦 Queue depth: {queued}\n")
