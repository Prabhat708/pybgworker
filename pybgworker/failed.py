from .utils import get_conn


def list_failed():
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, name, attempt, last_error
            FROM tasks
            WHERE status='failed'
            ORDER BY updated_at DESC
        """).fetchall()

    if not rows:
        print("✅ No failed tasks")
        return

    print("\n❌ Failed Tasks\n")

    for r in rows:
        print(f"ID: {r[0]}")
        print(f"Task: {r[1]}")
        print(f"Attempts: {r[2]}")
        print(f"Error: {r[3][:120] if r[3] else 'None'}")
        print("-" * 40)
