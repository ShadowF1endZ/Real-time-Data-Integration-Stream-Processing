# Real-time Fraud Detection Pipeline

A streaming data pipeline that detects fraudulent financial transactions in under 1 second — built with Apache Kafka, Spark Streaming, and a Random Forest classifier trained on the PaySim dataset.

How it works:
- Transactions flow in continuously via Kafka (up to 3,000/sec)
- Spark groups them into rolling time windows (30s, 60s) to spot unusual account behavior
- A machine learning model scores each transaction live and triggers instant alerts for suspicious ones
- Results are stored in PostgreSQL and visualized in Grafana

Highlights:
- End-to-end latency under 1 second at 3,000 transactions/sec
- No data loss or duplicate processing, even if the system crashes and restarts
- Benchmarked across traffic levels from 100 to 50,000 transactions/sec

Stack: Python · Apache Kafka · Apache Spark · PostgreSQL · Grafana · Docker