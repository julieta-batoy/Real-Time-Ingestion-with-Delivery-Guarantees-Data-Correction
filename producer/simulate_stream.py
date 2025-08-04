import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer
import os

# Kafka config
producer = Producer({'bootstrap.servers': 'localhost:9092'})
TOPIC = "user_events"

event_history = []
event_log = []

# Output JSON file
LOG_FILE = "producer/produced_events.json"

def current_utc():
    return datetime.now(timezone.utc).isoformat()

def emit_event(event, is_tombstone=False):
    event_id = event["event_id"] if not is_tombstone else event
    value = None if is_tombstone else json.dumps(event).encode("utf-8")

    # Send to Kafka
    producer.produce(TOPIC, key=event_id.encode("utf-8"), value=value)
    producer.flush()

    # Append to in-memory event log
    if is_tombstone:
        event_log.append({"event_id": event, "tombstone": True})
    else:
        event_log.append(event)

    # Write entire event log to JSON file
    with open(LOG_FILE, "w") as f:
        json.dump(event_log, f, indent=2)

def create_event():
    now = current_utc()
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user{random.randint(1, 10)}",
        "event_type": random.choice(["click", "view", "purchase"]),
        "event_time": now,
        "received_time": now,
        "is_valid": True
    }

def simulate_stream():
    counter = 0
    while True:
        counter += 1
        base_event = create_event()

        # 10% delayed
        if random.random() < 0.1:
            delay_seconds = random.randint(30, 60)
            delayed_time = datetime.now(timezone.utc) - timedelta(seconds=delay_seconds)
            base_event["event_time"] = delayed_time.isoformat()

        emit_event(base_event)
        event_history.append(base_event)

        # Emit corrections (5–10%) after some events
        if counter > 5 and random.random() < 0.1 and len(event_history) > 3:
            original = random.choice(event_history)

            correction_type = random.choice(["invalidate", "tombstone"])
            if correction_type == "invalidate":
                corrected_event = original.copy()
                corrected_event["is_valid"] = False
                corrected_event["received_time"] = current_utc()
                emit_event(corrected_event)
            else:  # tombstone
                emit_event(original["event_id"], is_tombstone=True)

        time.sleep(1)

if __name__ == "__main__":
    print("Starting simulated event producer...")
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    simulate_stream()
