# alert/alert_consumer.py
"""
Subscribe Kafka 'alerts' topic → gửi webhook / log alert.
Chạy song song với Spark job.
"""

import json, requests
from kafka import KafkaConsumer

TOPIC   = "alerts"
BROKER  = "localhost:9092"
WEBHOOK = "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"  # thay bằng real

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    value_deserializer=lambda m: json.loads(m.decode()),
    auto_offset_reset="latest",
    group_id="alert-notifier",
)

print("[Alert Consumer] Listening...")
for msg in consumer:
    alert = msg.value
    print(f"[FRAUD ALERT] {alert['nameOrig']} → {alert['nameDest']} "
          f"| amount={alert['amount']:.2f} | score={alert['fraud_score']:.3f}")

    # Gửi webhook
    try:
        requests.post(WEBHOOK, json={
            "text": (
                f":rotating_light: *FRAUD DETECTED*\n"
                f"Account `{alert['nameOrig']}` → `{alert['nameDest']}`\n"
                f"Amount: *${alert['amount']:,.2f}*  |  Score: *{alert['fraud_score']:.3f}*"
            )
        }, timeout=3)
    except Exception as e:
        print(f"Webhook error: {e}")