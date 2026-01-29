from pybgworker import task
import time

@task()
def say_hello(name):
    time.sleep(2)
    print(f"👋 Hello, {name}")
    return f"Greeted {name}"

if __name__ == "__main__":
    print("🚀 App started")

    result = say_hello.delay("Prabhat")

    print("⚙️ Doing other work...")
    for i in range(3):
        print(f"Main work {i+1}")
        time.sleep(1)

    print("📊 Task status:", result.status)
    print("📦 Task result:", result.result)
