import json
import psycopg2
from uuid import UUID
from pathlib import Path

# PostgreSQL connection
DB_CONN = psycopg2.connect(
    dbname="user_events",
    user="postgres",
    password="admin123",
    host="localhost",
    port=5432
)

# Load events from producer file 
def load_produced_events(file_path):
    events = {}
    with open(file_path, 'r') as f:
        data = json.load(f)
        for event in data:
            if 'event_id' in event:
                events[event['event_id']] = event
    return events

# Load events from Postgres 
def load_stored_events():
    stored = {}
    with DB_CONN.cursor() as cur:
        cur.execute("""
            SELECT event_id, user_id, event_type, event_time, received_time, is_valid
            FROM events;
        """)
        for row in cur.fetchall():
            stored[str(row[0])] = {
                "event_id": str(row[0]),
                "user_id": row[1],
                "event_type": row[2],
                "event_time": str(row[3]),
                "received_time": str(row[4]),
                "is_valid": row[5]
            }
    return stored

# Validation
def validate(produced, stored):
    print("Total produced:", len(produced))
    print("Total stored:", len(stored))

    # 1. Missing valid events
    missing = [
        eid for eid in produced
        if produced[eid].get('is_valid') == True and eid not in stored
    ]
    print(f"\nMissing valid events: {len(missing)}")
    if missing:
        print("Examples:", missing[:5])

    # 2. Corrections not applied
    unapplied = [
        eid for eid in produced
        if produced[eid].get('is_valid') == False and eid in stored and stored[eid]['is_valid'] != False
    ]
    print(f"\nUnapplied corrections (is_valid=false not reflected): {len(unapplied)}")
    if unapplied:
        print("Examples:", unapplied[:5])

    # 3. Duplicates are handled by PK (event_id) in Postgres — no need to check explicitly
    print("\nValidation complete.")

if __name__ == "__main__":
    file_path = "producer/produced_events.json"  # adjust if needed
    produced_events = load_produced_events(file_path)
    stored_events = load_stored_events()
    validate(produced_events, stored_events)
