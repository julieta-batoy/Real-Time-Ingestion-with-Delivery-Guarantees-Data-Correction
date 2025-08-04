import json
import time
import uuid
import logging
import psycopg2
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

producer = Producer({'bootstrap.servers': 'localhost:9092'})

# PostgreSQL connection
DB_CONN = psycopg2.connect(
    dbname="user_events",
    user="postgres", 
    password="admin123", 
    host="localhost",
    port=5432
)

TOPIC = "user_events"

def send_event(event, key=None):
    producer.produce(TOPIC, key=key, value=json.dumps(event).encode('utf-8') if event else None)
    producer.flush()

def test_ingestion():
    logging.info("Running test_ingestion...")
    eid = str(uuid.uuid4())
    event = {
        "event_id": eid,
        "user_id": "user_1",
        "event_type": "click",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "received_time": datetime.now(timezone.utc).isoformat(),
        "is_valid": True
    }
    send_event(event, key=eid)
    time.sleep(3)
    with DB_CONN.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM events WHERE event_id = %s", (eid,))
        assert cur.fetchone()[0] == 1
    logging.info("test_ingestion passed")

def test_late_event():
    logging.info("Running test_late_event...")
    eid = str(uuid.uuid4())
    late_event = {
        "event_id": eid,
        "user_id": "user_late",
        "event_type": "click",
        "event_time": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
        "received_time": datetime.now(timezone.utc).isoformat(),
        "is_valid": True
    }
    send_event(late_event, key=eid)
    time.sleep(3)
    with DB_CONN.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM events WHERE event_id = %s", (eid,))
        assert cur.fetchone()[0] == 1
    logging.info("test_late_event passed")

def test_invalidation_flag():
    logging.info("Running test_invalidation_flag...")
    eid = str(uuid.uuid4())
    event = {
        "event_id": eid,
        "user_id": "user_invalidate",
        "event_type": "click",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "received_time": datetime.now(timezone.utc).isoformat(),
        "is_valid": True
    }
    send_event(event, key=eid)
    time.sleep(1)

    # Now send correction
    event["is_valid"] = False
    send_event(event, key=eid)
    time.sleep(3)

    with DB_CONN.cursor() as cur:
        cur.execute("SELECT is_valid FROM events WHERE event_id = %s", (eid,))
        assert cur.fetchone()[0] == False
    logging.info("test_invalidation_flag passed")

def test_tombstone_deletion():
    logging.info("Running test_tombstone_deletion...")
    eid = str(uuid.uuid4())
    event = {
        "event_id": eid,
        "user_id": "user_tombstone",
        "event_type": "click",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "received_time": datetime.now(timezone.utc).isoformat(),
        "is_valid": True
    }
    send_event(event, key=eid)
    time.sleep(1)

    # Send tombstone (null payload)
    send_event(None, key=eid)
    time.sleep(3)

    with DB_CONN.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM events WHERE event_id = %s", (eid,))
        assert cur.fetchone()[0] == 0
    logging.info("test_tombstone_deletion passed")

def test_duplicate_handling():
    logging.info("Running test_duplicate_handling...")
    eid = str(uuid.uuid4())
    event = {
        "event_id": eid,
        "user_id": "user_dupe",
        "event_type": "view",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "received_time": datetime.now(timezone.utc).isoformat(),
        "is_valid": True
    }

    send_event(event, key=eid)
    send_event(event, key=eid)  # duplicate
    time.sleep(3)

    with DB_CONN.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM events WHERE event_id = %s", (eid,))
        count = cur.fetchone()[0]
        assert count == 1  # deduplication should prevent multiple inserts
    logging.info("test_duplicate_handling passed")

def log_observability_metrics():
    logging.info("\n--- Observability Metrics ---")
    with DB_CONN.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM events;")
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM events WHERE is_valid = false;")
        invalidated = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM events
            WHERE received_time - event_time > INTERVAL '30 seconds';
        """)
        late = cur.fetchone()[0]

    logging.info(f"Total Events: {total}")
    logging.info(f"Invalidated: {invalidated}")
    logging.info(f"Late Events (>30s): {late}")

if __name__ == "__main__":
    test_ingestion()
    test_late_event()
    test_invalidation_flag()
    test_tombstone_deletion()
    test_duplicate_handling()
    log_observability_metrics()
