from pybgworker import task
import time

attempt = {"count": 0}

@task(retries=3, retry_delay=2)
def unstable_task():
    attempt["count"] += 1
    print(f"Attempt {attempt['count']}")

    time.sleep(1)

    if attempt["count"] <= 3:
        raise Exception("Temporary failure")

    return "Success after retries"

if __name__ == "__main__":
    print("🚀 Submitting unstable task")
    result = unstable_task.delay()

    while not result.ready():
        print("⏳ Waiting for task...")
        time.sleep(1)

    if result.successful():
        print("✅ Task succeeded:", result.result)
    else:
        print("❌ Task failed:", result.error)
