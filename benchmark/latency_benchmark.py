# benchmark/latency_benchmark.py
"""
Đo end-to-end latency (producer timestamp → alert received)
dưới các mức throughput khác nhau: 100 → 1k → 10k → 50k msg/s.
"""

import json, time, uuid, threading, statistics
from kafka import KafkaProducer, KafkaConsumer

BROKER       = "localhost:9092"
TRANS_TOPIC  = "transactions"
ALERT_TOPIC  = "alerts"
LOADS        = [100, 500, 1_000, 5_000, 10_000, 50_000]   # msg/s

sent_times: dict[str, float] = {}
latencies: list[float] = []


def consumer_thread():
    """Thread riêng lắng nghe alert → tính latency."""
    consumer = KafkaConsumer(
        ALERT_TOPIC,
        bootstrap_servers=BROKER,
        value_deserializer=lambda m: json.loads(m.decode()),
        auto_offset_reset="latest",
        group_id="bench-consumer",
    )
    for msg in consumer:
        eid = msg.value.get("event_id")
        if eid and eid in sent_times:
            lat = (time.time() - sent_times.pop(eid)) * 1000  # ms
            latencies.append(lat)


threading.Thread(target=consumer_thread, daemon=True).start()

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode(),
    enable_idempotence=True,
    acks="all",
)

results = []

for rate in LOADS:
    latencies.clear()
    interval = 1.0 / rate
    n_msgs   = min(rate * 10, 5000)    # Gửi 10s worth, tối đa 5k
    print(f"\n[Bench] Rate={rate} msg/s  N={n_msgs}")

    start = time.time()
    for _ in range(n_msgs):
        eid = str(uuid.uuid4())
        event = {
            "event_id": eid, "step": 1, "type": "TRANSFER",
            "amount": 9999.99,
            "nameOrig": "C_bench", "oldbalanceOrg": 10000, "newbalanceOrig": 0.01,
            "nameDest": "C_dest",  "oldbalanceDest": 0,    "newbalanceDest": 9999.99,
            "timestamp_ms": int(time.time() * 1000),
        }
        sent_times[eid] = time.time()
        producer.send(TRANS_TOPIC, value=event)
        time.sleep(interval)

    producer.flush()
    time.sleep(5)   # đợi alerts về

    if latencies:
        s = sorted(latencies)
        r = {
            "rate":   rate,
            "n":      len(latencies),
            "p50_ms": round(statistics.median(s), 1),
            "p95_ms": round(s[int(len(s) * 0.95)], 1),
            "p99_ms": round(s[int(len(s) * 0.99)], 1),
            "max_ms": round(max(s), 1),
        }
        results.append(r)
        print(f"  p50={r['p50_ms']}ms  p95={r['p95_ms']}ms  p99={r['p99_ms']}ms  max={r['max_ms']}ms")
    else:
        print("  No alerts received (fraud rate too low at this load?)")

print("\n=== Benchmark Summary ===")
print(f"{'Rate':>8} | {'p50':>8} | {'p95':>8} | {'p99':>8} | {'max':>8}")
for r in results:
    print(f"{r['rate']:>8} | {r['p50_ms']:>7}ms | {r['p95_ms']:>7}ms | {r['p99_ms']:>7}ms | {r['max_ms']:>7}ms")