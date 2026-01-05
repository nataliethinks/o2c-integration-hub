import json
import os
import time
import pika
import psycopg2

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
QUEUE_NAME = os.getenv("QUEUE_NAME", "o2c.events")

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGUSER = os.getenv("PGUSER", "o2c")
PGPASSWORD = os.getenv("PGPASSWORD", "o2c")
PGDATABASE = os.getenv("PGDATABASE", "o2c_reporting")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sales_order_events (
  id SERIAL PRIMARY KEY,
  order_id INT NOT NULL,
  customer_id TEXT NOT NULL,
  currency TEXT NOT NULL,
  amount NUMERIC NOT NULL,
  amount_cad NUMERIC NOT NULL,
  created_at BIGINT NOT NULL,
  received_at BIGINT NOT NULL
);
"""

def connect_db():
    for _ in range(30):
        try:
            conn = psycopg2.connect(
                host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE
            )
            conn.autocommit = True
            return conn
        except Exception:
            time.sleep(1)
    raise RuntimeError("Unable to connect to Postgres")

def transform_to_cad(currency: str, amount: float) -> float:
    # MVP fixed rate just to demonstrate transform step
    rate = 1.35 if currency.upper() == "USD" else 1.00
    return round(amount * rate, 2)

def main():
    db = connect_db()
    cur = db.cursor()
    cur.execute(CREATE_TABLE_SQL)

    connection = None
    for _ in range(30):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            break
        except Exception:
            time.sleep(1)
    if not connection:
        raise RuntimeError("Unable to connect to RabbitMQ")

    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        msg = json.loads(body.decode("utf-8"))
        if msg.get("event_type") != "SalesOrderCreated":
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        data = msg["data"]
        amount = float(data["amount"])
        amount_cad = transform_to_cad(data["currency"], amount)

        cur.execute(
            """
            INSERT INTO sales_order_events
            (order_id, customer_id, currency, amount, amount_cad, created_at, received_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                int(data["order_id"]),
                str(data["customer_id"]),
                str(data["currency"]),
                amount,
                amount_cad,
                int(data["created_at"]),
                int(time.time())
            ),
        )

        print(f"[OK] Loaded order {data['order_id']} amount_cad={amount_cad}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    print("[*] Worker waiting for messages...")
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    channel.start_consuming()

if __name__ == "__main__":
    main()
