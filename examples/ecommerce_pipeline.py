from pybgworker import task
import time
import random

# ---- TASKS ----

@task(retries=3, retry_delay=4)
def process_payment(order_id, amount):
    print(f"💳 Processing payment for order {order_id} (₹{amount})")
    time.sleep(3)

    if random.random() < 0.6:
        raise Exception("Payment gateway timeout")

    return "PAYMENT_SUCCESS"


@task()
def generate_invoice(order_id):
    print(f"🧾 Generating invoice for order {order_id}")
    time.sleep(2)
    return f"invoice_{order_id}.pdf"


@task()
def send_confirmation_email(order_id, invoice_file):
    print(f"📧 Sending confirmation email for {order_id}")
    time.sleep(2)
    return "EMAIL_SENT"


# ---- MAIN APP ----

def place_order(order_id, amount):
    print(f"🛒 Order placed: {order_id}")

    payment = process_payment.delay(order_id, amount)

    print("✅ Order accepted, processing in background")

    while not payment.ready():
        print("⏳ Waiting for payment...")
        time.sleep(2)

    if not payment.successful():
        print("❌ Payment failed, order cancelled")
        return

    invoice = generate_invoice.delay(order_id)

    while not invoice.ready():
        time.sleep(1)

    email = send_confirmation_email.delay(order_id, invoice.result)

    while not email.ready():
        time.sleep(1)

    print("🎉 Order completed successfully!")


if __name__ == "__main__":
    place_order(order_id="ORD-1024", amount=2499)
