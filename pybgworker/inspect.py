import json as _json
from .utils import get_conn
from datetime import datetime, timezone


def inspect(as_json=False):
    """Print queue status and worker health.

    Args:
        as_json: When ``True``, emit a JSON object instead of formatted text.
            Useful for piping into monitoring tools or scripts.

    JSON shape::

        {
            "tasks": {"queued": 3, "running": 1, "success": 42, ...},
            "total": 46,
            "workers": [
                {"name": "worker-1", "last_seen": "...", "status": "alive", "seconds_ago": 2}
            ]
        }
    """
    with get_conn() as conn:
        conn.row_factory = dict_factory

        stats = conn.execute("""
            SELECT status, COUNT(*) as count
            FROM tasks
            GROUP BY status
        """).fetchall()

        workers = conn.execute("""
            SELECT name, last_seen
            FROM workers
        """).fetchall()

    now = datetime.now(timezone.utc)

    task_counts = {row["status"]: row["count"] for row in stats}
    total = sum(task_counts.values())

    worker_list = []
    for w in workers:
        last_seen = datetime.fromisoformat(w["last_seen"])
        delta = (now - last_seen).total_seconds()
        worker_list.append({
            "name": w["name"],
            "last_seen": w["last_seen"],
            "status": "alive" if delta < 15 else "dead",
            "seconds_ago": int(delta),
        })

    if as_json:
        print(_json.dumps({
            "tasks": task_counts,
            "total": total,
            "workers": worker_list,
        }, indent=2))
        return

    print("\n📦 Task Stats")
    for status, count in task_counts.items():
        print(f"{status:10} {count}")
    print(f"{'total':10} {total}")

    print("\n👷 Workers")
    for w in worker_list:
        print(f"{w['name']:10} {w['status']:5} ({w['seconds_ago']}s ago)")
    print()


def dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
