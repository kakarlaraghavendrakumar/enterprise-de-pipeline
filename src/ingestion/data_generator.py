import argparse
import json
import os
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker("en_IN")
VALID_STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]


def generate_orders(run_date: str, num_records: int = 1000) -> list[dict]:
    base_dt = datetime.strptime(run_date, "%Y-%m-%d")
    records = []

    for i in range(num_records):
        event_ts = base_dt + timedelta(seconds=random.randint(0, 86399))
        record = {
            "order_id": f"ORD-{run_date.replace('-', '')}-{i:05d}",
            "customer_id": f"CUST-{random.randint(1000, 1200)}",
            "product_id": f"PROD-{random.randint(1, 50):03d}",
            "order_status": random.choice(VALID_STATUSES),
            "amount": round(random.uniform(199.0, 15000.0), 2),
            "event_timestamp": event_ts.strftime("%Y-%m-%d %H:%M:%S"),
        }
        records.append(record)

    # Inject ~5% duplicates (same order_id, slightly newer timestamp & status update)
    for sample in random.sample(records, k=int(num_records * 0.05)):
        dup = sample.copy()
        orig_ts = datetime.strptime(dup["event_timestamp"], "%Y-%m-%d %H:%M:%S")
        dup["event_timestamp"] = (orig_ts + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")
        dup["order_status"] = "SHIPPED"
        records.append(dup)

    # Inject ~3% dirty records to test Dead Letter Queue (DLQ) routing
    dirty_samples = [
        {
            "order_id": None,  # Missing primary key
            "customer_id": "CUST-9999",
            "product_id": "PROD-001",
            "order_status": "PLACED",
            "amount": 500.00,
            "event_timestamp": f"{run_date} 10:00:00",
        },
        {
            "order_id": f"ORD-{run_date.replace('-', '')}-BAD1",
            "customer_id": "CUST-1001",
            "product_id": "PROD-002",
            "order_status": "PLACED",
            "amount": -250.00,  # Negative amount
            "event_timestamp": f"{run_date} 11:00:00",
        },
        {
            "order_id": f"ORD-{run_date.replace('-', '')}-BAD2",
            "customer_id": "CUST-1002",
            "product_id": "PROD-003",
            "order_status": "UNKNOWN_STATE",  # Invalid enum
            "amount": 1200.00,
            "event_timestamp": f"{run_date} 12:00:00",
        },
    ]
    records.extend(dirty_samples * 10)
    random.shuffle(records)
    return records


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
    parser.add_argument("--rows", type=int, default=1000, help="Base number of rows")
    args = parser.parse_args()

    output_dir = f"/opt/airflow/data/raw/orders/dt={args.date}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "orders.jsonl")

    orders = generate_orders(args.date, args.rows)
    with open(output_path, "w", encoding="utf-8") as f:
        for row in orders:
            f.write(json.dumps(row) + "\n")

    print(f"Generated {len(orders)} raw records at {output_path}")


if __name__ == "__main__":
    main()