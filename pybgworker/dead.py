from .utils import get_conn


def list_dead():
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, name, attempt, last_error
            FROM tasks
            WHERE status='dead'
            ORDER BY updated_at DESC
        """).fetchall()

    if not rows:
        print("No dead tasks")
        return

    print("\nDead Tasks\n")

    for r in rows:
        print(f"ID: {r[0]}")
        print(f"Task: {r[1]}")
        print(f"Attempts: {r[2]}")
        print(f"Error: {r[3][:120] if r[3] else 'None'}")
        print("-" * 40)
