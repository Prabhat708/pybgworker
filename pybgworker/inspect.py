from .utils import get_conn
from datetime import datetime, timezone


def inspect():
    with get_conn() as conn:
        conn.row_factory = dict_factory

        print("\n📦 Task Stats")

        stats = conn.execute("""
            SELECT status, COUNT(*) as count
            FROM tasks
            GROUP BY status
        """).fetchall()

        total = 0
        for row in stats:
            print(f"{row['status']:10} {row['count']}")
            total += row["count"]

        print(f"{'total':10} {total}")

        print("\n👷 Workers")

        workers = conn.execute("""
            SELECT name, last_seen
            FROM workers
        """).fetchall()

        now = datetime.now(timezone.utc)

        for w in workers:
            last_seen = datetime.fromisoformat(w["last_seen"])
            delta = (now - last_seen).total_seconds()

            status = "alive" if delta < 15 else "dead"
            print(f"{w['name']:10} {status:5} ({int(delta)}s ago)")

        print()
def dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
