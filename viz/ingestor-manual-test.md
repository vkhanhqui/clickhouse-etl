# Ingestor — Manual Run & Test Guide

End-to-end manual test for the ingestor role without running the full orchestration layer. You verify the flow: **Kafka → Ingestor → NATS JetStream**.

---

## 1. Start Infrastructure

```bash
# NATS with JetStream
docker run -d --name nats -p 4222:4222 nats:latest -js

# Kafka (KRaft, no ZK, single broker)
# Single-broker setup needs KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
# because __consumer_offsets defaults to RF=3 and fails to auto-create
# with only one broker, which prevents the consumer group coordinator
# from initializing (COORDINATOR_NOT_AVAILABLE).
docker run -d --name kafka -p 9092:9092 \
  -e CLUSTER_ID="$(docker run --rm confluentinc/cp-kafka:latest kafka-storage random-uuid)" \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka:latest

# PostgreSQL (for schema store — ingestor requires DB)
docker run -d --name pg -p 5432:5432 \
  -e POSTGRES_USER=glassflow \
  -e POSTGRES_PASSWORD=glassflow \
  -e POSTGRES_DB=glassflow \
  postgres:16

brew install libpq golang-migrate

POSTGRES_USER=glassflow POSTGRES_PASSWORD=glassflow \
  ./glassflow-api/migrations/run-migrations.sh
```

---

## 2. Create a Test Pipeline JSON

This is the config the ingestor loads at startup. Save as `/tmp/pipeline.json`.

```json
{
  "pipeline_id": "manual-test-pipeline",
  "name": "manual-test",
  "source_type": "kafka",
  "ingestor": {
    "type": "kafka",
    "provider": "kafka",
    "kafka_connection_params": {
      "brokers": ["localhost:9092"],
      "protocol": "PLAINTEXT",
      "mechanism": "NO_AUTH"
    },
    "kafka_topics": [
      {
        "name": "test-events",
        "id": "topic-1",
        "replicas": 1,
        "consumer_group_initial_offset": "earliest",
        "consumer_group_name": "glassflow-ingestor-manual",
        "deduplication": {
          "enabled": false
        }
      }
    ]
  },
  "schema_versions": {
    "topic-1": {
      "source_id": "topic-1",
      "version_id": "1",
      "data_type": "json",
      "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "value", "type": "int"}
      ]
    }
  },
  "status": {
    "pipeline_id": "manual-test-pipeline"
  }
}
```

---

## 3. Create NATS Stream + Kafka Topic

```bash
# NATS stream for ingestor output
docker run --rm --network host natsio/nats-box \
  nats stream add gfm-manual --subjects "manual.0" --storage file --replicas 1 --defaults

# Component signals stream (so ingestor can report backpressure signals)
# glassflow-api/internal/ingestor/processor.go:258
docker run --rm --network host natsio/nats-box \
  nats stream add component-signals --subjects "component-signals.failures" --storage file --replicas 1 --defaults

# DLQ stream (pipeline ID hash is first 8 chars of SHA-256("manual-test-pipeline"))
# The DLQ stream name is `gfm-<pipeline-hash>-DLQ`. For `manual-test-pipeline` the hash is `c15f4e93`:
docker run --rm --network host natsio/nats-box \
  nats stream add gfm-c15f4e93-DLQ --subjects "gfm-c15f4e93-DLQ.failed" --storage file --replicas 1 --defaults

# Kafka topic
docker exec kafka kafka-topics --create \
  --topic test-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

---

## 4. Seed Schema Version in PostgreSQL

The ingestor validates messages against a schema stored in the `schema_versions` table. Seed it with the pipeline's source fields.

```bash
# Direct psql
psql postgres://glassflow:glassflow@localhost:5432/glassflow -f .vscode/tmp/seed-manual-test.sql

# Or via docker exec
docker exec -i pg psql -U glassflow -d glassflow < .vscode/tmp/seed-manual-test.sql
```

---

## 5. Run the Ingestor

```bash
cd glassflow-api 
go run ./cmd/glassflow/ -role ingestor
```

---

## 6. Send Test Events

```bash
# Terminal 2 — produce 5 events
for i in $(seq 1 5); do
  echo '{"id":'$i',"name":"event-'$i'","value":'$((i*10))'}' | \
    docker exec -i kafka kafka-console-producer \
      --bootstrap-server localhost:9092 \
      --topic test-events
done
```

---

## 7. Verify End-to-End

```bash
# Read ingestor output from the `manual.0` stream:
docker run --rm --network host natsio/nats-box \
  nats sub "manual.0"

# Check Kafka consumer group offset
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group glassflow-ingestor-manual --describe
```

---

## 8. Test Backpressure

Simulate a full NATS stream to trigger backpressure behavior.

```bash
# Limit the stream to 1 message max
docker run --rm --network host natsio/nats-box \
  nats stream update gfm-manual --max-msgs=1 --max-bytes=-1 --discard=new --force

# Send 10 events rapidly
for i in $(seq 1 10); do
  echo '{"id":'$i',"name":"event-'$i'","value":'$((i*10))'}' | \
    docker exec -i kafka kafka-console-producer \
      --bootstrap-server localhost:9092 \
      --topic test-events
done
```

Ingestor logs will show:

```
{"level":"INFO","msg":"Processing batch of messages","batchSize":1}
{"level":"INFO","msg":"ingestor backpressure: start","pipeline_id":"manual-test-pipeline"}
```

Clear the stream
```bash
docker run --rm --network host natsio/nats-box \
  nats stream purge gfm-manual --force
```

To read the backpressure signals from the `component-signals` stream:
```bash
docker run --rm --network host natsio/nats-box \
  nats sub "component-signals.failures"
```

---

## 9. Test DLQ Path

Send a message that fails schema validation and watch it go to DLQ.

```bash
# Produce a non-JSON message
echo "not-json-at-all" | \
  docker exec -i kafka kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic test-events
```

Check DLQ subject:
```bash
docker run --rm --network host natsio/nats-box \
  nats sub "gfm-c15f4e93-DLQ.failed"
```

---

## 10. Test Restart Recovery (At-Least-Once)

```bash
# 1. Kill the ingestor (Ctrl+C)
# 2. While it's down, send 10 messages
for i in $(seq 1 10); do
  echo '{"id":'$i',"name":"event-'$i'","value":'$((i*10))'}' | \
    docker exec -i kafka kafka-console-producer \
      --bootstrap-server localhost:9092 \
      --topic test-events
done
```

The ingestor resumes from the committed Kafka offset. Uncommitted records (if any were in-flight during the kill) replay — downstream dedup handles the duplicates.

---

## Stop & Cleanup

```bash
docker stop nats kafka pg
docker rm nats kafka pg
```
