# producer/paysim_producer.py
"""
Đọc PaySim CSV (hoặc sinh synthetic) → publish vào Kafka topic 'transactions'
với exactly-once semantics qua transactional producer.
"""

import json, time, uuid, random, argparse, os
from kafka import KafkaProducer
from kafka.errors import KafkaError
import pandas as pd

TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

def make_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode(),
        # Exactly-once: idempotent + acks=all + retries
        enable_idempotence=True,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )


def stream_paysim(csv_path: str, bootstrap: str, rate: int):
    """
    rate: số transaction/giây muốn phát
    """
    df = pd.read_csv(csv_path)
    producer = make_producer(bootstrap)

    print(f"[Producer] Streaming {len(df)} records at ~{rate} msg/s")
    interval = 1.0 / rate

    for _, row in df.iterrows():
        event = {
            "event_id":       str(uuid.uuid4()),
            "step":           int(row["step"]),
            "type":           row["type"],
            "amount":         float(row["amount"]),
            "nameOrig":       row["nameOrig"],
            "oldbalanceOrg":  float(row["oldbalanceOrg"]),
            "newbalanceOrig": float(row["newbalanceOrig"]),
            "nameDest":       row["nameDest"],
            "oldbalanceDest": float(row["oldbalanceDest"]),
            "newbalanceDest": float(row["newbalanceDest"]),
            "isFraud":        int(row["isFraud"]),     
            "timestamp_ms":   int(time.time() * 1000),
        }
        producer.send(TOPIC, value=event)
        time.sleep(interval)

    producer.flush()
    print("[Producer] Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv",    default=os.getenv("CSV_PATH",    "PS_20174392719_1491204439457_log.csv"))
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument("--rate",   type=int, default=int(os.getenv("STREAM_RATE", "500")))
    args = parser.parse_args()
    stream_paysim(args.csv, args.broker, args.rate)