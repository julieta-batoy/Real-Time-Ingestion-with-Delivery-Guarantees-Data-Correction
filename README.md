# Real-Time Data Ingestion Pipeline for User Events using Kafka, Python, and PostgreSQL

This pipeline simulates and processes user activity events in real time using Apache Kafka and PostgreSQL. It supports late-arriving events, event corrections, and deletions. It includes validation and integration testing to ensure data correctness and observability metrics for ingestion monitoring.

## A. Setup & Run Instructions

### 1. Prerequisites
- Python 3.10+ [https://www.python.org/downloads/windows/](https://www.python.org/downloads/windows/)
- Kafka & Zookeeper [https://kafka.apache.org/downloads](https://kafka.apache.org/downloads)
  - Choose the latest binary release (e.g., "Scala 2.13 – kafka_2.x").
  - Extract the downloaded Kafka ZIP file (e.g., kafka_2.13-3.9.1) to a folder like:
    <pre lang="makefile">
    C:\kafka
    </pre>
- PostgreSQL (with `user_events` DB created) [https://www.postgresql.org/download/windows/](https://www.postgresql.org/download/windows/)
- Python dependencies:
  <pre lang="bash">pip install -r requirements.txt</pre>

### 2. PostgreSQL Setup
Create schema:
<pre lang="sql">
-- Create the database (optional if already created)
CREATE DATABASE user_events;
</pre>
<pre lang="sql">
-- Create the events table
CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    received_time TIMESTAMPTZ NOT NULL,
    is_valid BOOLEAN NOT NULL
);
</pre>

### 3. Start Zookeeper
Kafka uses Zookeeper as a dependency. Run in a Command Prompt:
<pre lang="cmd">
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
</pre>
Keep this terminal open.

### 4. Start Kafka Broker
Go to `C:\kafka\config` and open the `server.properties`. Change the `broker.id` from `0` to `1`.
<pre lang="markdown">
broker.id=1
</pre>
Open a new Command Prompt, and run:
<pre lang="cmd">
set KAFKA_HEAP_OPTS=-Xmx512M -Xms512M
</pre>
Then launch Kafka:
<pre lang="cmd">
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
</pre>

### 5. Run Producer
<pre lang="bash">
python producer/simulate_stream.py
</pre>

### 6. Run Consumer
<pre lang="bash">
python consumer/consume_events.py
</pre>

### 7. Validate Data
<pre lang="bash">
python validation/validate_data.py
</pre>

### 8. Run Integration Tests
<pre lang="bash">
python tests/integration_test.py
</pre>


## B. Design Summary (tools, approach)
**Tools Used**
- Kafka: Stream ingestion
- PostgreSQL: Event storage with deduplication
- Python: Producer, Consumer, Validation
- Confluent Kafka client: Kafka producer/consumer
- JSON log: Backup and comparison for validation

**Approach**
- Events are produced with metadata (event_time, received_time, etc.).
- Kafka Consumer inserts to PostgreSQL using event_id as primary key.
- Validation script compares original .jsonl log with database contents.
- Integration tests simulate real-world scenarios and log observability metrics.


## C. Handling Strategy (for late data, duplicates, deletions)
**At-Least-Once Delivery**
- Kafka ensures retries until ack.
- PostgreSQL deduplicates using `event_id` as `PRIMARY KEY`.

**Late Data**
- `event_time` and `received_time` are tracked separately.
- Late data is accepted and inserted without loss.
- Metrics report count of late events >30s.

**Invalidation / Deletions**
- Invalidation: Uses `is_valid = False` field.
- Deletion: Tombstone message (`value=None`) deletes the row in DB.
- Validation ensures invalidations and deletions are respected.

**Observability & Metrics**
- Metrics printed after integration tests:
  - Total events
  - Invalidated events
  - Late arrivals
  - Ingestion success
- Uses Python logging for structured outputs.


## D. Scaling Considerations for Production
**Database**
- Use partitioned tables by date for event_time.
- Add indexing on `event_time`, `user_id`.

**Kafka**
- Use Kafka partitions per `user_id` hash for parallelism.
- Enable log compaction for deduplication on the topic.

**Consumers**
- Add multiple Kafka consumers with group ID for horizontal scaling.
- Implement retries with exponential backoff on transient DB errors.

**Monitoring**
- Add Prometheus + Grafana dashboards.
- Alert on ingestion lag or invalidation failures.
