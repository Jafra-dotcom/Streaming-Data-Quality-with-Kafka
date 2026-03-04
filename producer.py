"""
producer.py — DO NOT MODIFY
Reads flights_summary.json line by line and publishes each line
as a raw message to the Kafka topic: raw-flights
"""

import time
import json
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
INPUT_FILE   = "flights_summary.json"
TOPIC        = "raw-flights"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: v.encode("utf-8"),   # send raw string, not pre-parsed
)

print(f"[Producer] Starting — publishing to topic '{TOPIC}'")

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    for line_number, line in enumerate(f, start=1):
        line = line.strip()
        if not line:
            continue

        producer.send(TOPIC, value=line)
        print(f"[Producer] Sent line {line_number}: {line[:80]}{'...' if len(line) > 80 else ''}")
        time.sleep(0.3)   # simulate realistic stream pace

producer.flush()
producer.close()
print("[Producer] Done — all records sent.")