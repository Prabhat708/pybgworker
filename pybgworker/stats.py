import json as _json
from .utils import get_conn
from datetime import datetime, timezone


def stats(as_json=False):
    """Print worker statistics and queue depth.

    Args:
        as_json: When ``True``, emit a JSON object instead of formatted text.
            Useful for piping into monitoring tools or scripts.

    JSON shape::

        {
            "workers": [
                {"name": "worker-1", "last_seen": "...", "status": "alive", "seconds_ago": 2}
            ],
            "queue_depth": 5
        }
    """
    with get_conn() as conn:
        workers_raw = conn.execute("""
            SELECT name, last_seen FROM workers
        """).fetchall()

        queued = conn.execute("""
            SELECT COUNT(*) FROM tasks
            WHERE status IN ('queued', 'retrying')
        """).fetchone()[0]

    now = datetime.now(timezone.utc)

    worker_list = []
    for w in workers_raw:
        last_seen = datetime.fromisoformat(w[1])
        delta = (now - last_seen).total_seconds()
        worker_list.append({
            "name": w[0],
            "last_seen": w[1],
            "status": "alive" if delta < 15 else "dead",
            "seconds_ago": int(delta),
        })

    if as_json:
        print(_json.dumps({
            "workers": worker_list,
            "queue_depth": queued,
        }, indent=2))
        return

    print("\n👷 Worker Stats\n")
    for w in worker_list:
        print(f"{w['name']:10} {w['status']:5} ({w['seconds_ago']}s ago)")
    print(f"\n📦 Queue depth: {queued}\n")
