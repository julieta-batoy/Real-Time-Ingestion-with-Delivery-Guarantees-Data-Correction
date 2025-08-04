import json
import psycopg2
from confluent_kafka import Consumer, KafkaException

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="user_events",
    user="postgres",
    password="admin123",
    host="localhost",
    port=5432
)

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'event-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(conf)
consumer.subscribe(['user_events'])

def upsert_event(event):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO events (event_id, user_id, event_type, event_time, received_time, is_valid)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE
            SET user_id = EXCLUDED.user_id,
                event_type = EXCLUDED.event_type,
                event_time = EXCLUDED.event_time,
                received_time = EXCLUDED.received_time,
                is_valid = EXCLUDED.is_valid;
        """, (
            event['event_id'],
            event['user_id'],
            event['event_type'],
            event['event_time'],
            event['received_time'],
            event['is_valid']
        ))
    conn.commit()

def delete_event(event_id):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM events WHERE event_id = %s;", (event_id,))
    conn.commit()

try:
    print("Consumer started. Listening for messages...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        key = msg.key().decode('utf-8') if msg.key() else None
        value = msg.value()

        if value is None:
            # Tombstone delete
            print(f"Deleting event {key}")
            delete_event(key)
        else:
            try:
                event = json.loads(value.decode('utf-8'))
                upsert_event(event)
                print(f"Processed event {event['event_id']}")
            except Exception as e:
                print(f"Error processing message: {e}")

except KeyboardInterrupt:
    print("\nConsumer stopped manually.")
finally:
    consumer.close()
    conn.close()
