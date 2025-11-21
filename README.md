# Kafka Topic Bridge

A robust, schema-aware Kafka bridge designed to replicate topics from a **Confluent** source cluster to an **Apicurio** target cluster (running in Confluent-compatibility mode).

This service handles schema migration, schema evolution, and implements a strict "Safe or Stop" error handling policy.

## Architecture

* **Source:** Kafka (Confluent Platform) + Confluent Schema Registry
* **Target:** Kafka (Apache/KRaft) + Apicurio Registry
* **Bridge:** Spring Boot (Kotlin) service using `spring-kafka`.

## Error Handling & Reliability

The bridge distinguishes between **Infrastructure Failures** and **Data Poisoning** to ensure data integrity without stalling processing.

| Error Type | Scenario | Behavior | Outcome |
| :--- | :--- | :--- | :--- |
| **Infrastructure** | Network down, Broker offline, Registry unavailable | **Backoff & Retry** <br> `30s` -> `5m` -> `15m` | If retries exhaust: **Stop Service** (Requires manual intervention/restart). |
| **Data Poisoning** | Invalid Schema, Corrupt Bytes, "Poison Pill" | **Skip & DLQ** | **Write to DLT** (Raw Bytes) AND **Continue** processing next message. |

### Dead Letter Topic (DLT) Strategy

* **Infrastructure DLT:** Preserves the original Avro record (can be replayed later).
* **Poison DLT:** Writes raw bytes (using a `ByteArraySerializer`) to preserve the corrupt payload for forensic analysis.

## Configuration

Key configuration is managed in `application.yaml`. Note the bean name `bridgeProperties` is explicitly used for SpEL resolution.

```yaml
bridge:
  target-kafka-brokers: localhost:29093
  preserve-timestamp: true

  # Retry Backoff Intervals (ms)
  retry-intervals-ms:
    - 30000   # 30s
    - 300000  # 5m
    - 900000  # 15m

  # Topic Mappings (Source -> Target)
  topic-mappings:
    source-users: target-users
    source-orders: target-orders

  # DLT Mappings (Source -> DLT)
  dlt-mappings:
    source-users: target-users-dlt
    source-orders: target-orders-dlt
````

## Operations & Recovery Playbook

### Scenario 1: Poison Pill (Bad Data)

**Symptoms:** `ERROR` log regarding `DeserializationException`, message appears in DLT. Service **continues** running.
**Action:**

1.  **Do not restart the bridge.** It has already skipped the bad offset and is processing valid data behind it.
2.  **Investigate:** Consume the DLT to inspect the raw bytes.
3.  **Fix:** Contact the upstream producer to fix their serialization bug.
4.  **Recover:** Ask the producer to **resend** the affected records. They will arrive at a new (higher) offset and be bridged automatically.

### Scenario 2: Infrastructure Outage

**Symptoms:** `WARN` logs retrying connection. Eventually `ERROR` "Infrastructure retries exhausted" and service shuts down.
**Action:**

1.  Fix the underlying infrastructure (Broker/Registry).
2.  Restart the Bridge. It will resume from the last committed offset.

## Regression Testing

This project includes a full **Fault Injection Regression Suite** to verify resilience against infrastructure failures (e.g., Registry outages) and data corruption.

### Prerequisite

Ensure Docker is running and you have `make` installed.

### Running the Suite

This command spins up the full infrastructure, injects failures (pauses containers), and verifies the Bridge recovers without data loss.

```bash
make regression
```

**What it tests:**

1.  **Source Failure:** Pauses Source Registry -\> Bridge Retries -\> Registry Resumes -\> Bridge Recovers.
2.  **Target Failure:** Pauses Target Registry -\> Bridge Retries -\> Registry Resumes -\> Bridge Recovers.
3.  **Data Integrity:** Verifies all messages are present in the target topic after recovery.

## How to Run Manually

### 1\. Start Infrastructure

Starts Source Kafka, Target Kafka, and both Registries.

```bash
make start
```

### 2\. Run the Bridge

**Note:** Success logs are at `DEBUG` level. Enable debug logging to see message flow.

```bash
# Run locally (requires Java 21)
make run

# Run with DEBUG logs enabled (Recommended for local dev)
mvn spring-boot:run -Dspring-boot.run.arguments="--logging.level.com.yourcompany.kafkabridge=DEBUG"
```

### 3\. Test Scenarios

The project includes producers to simulate various traffic patterns:

```bash
# 1. Standard & Evolved Data (Happy Path)
make produce

# 2. Poison Pill (Bad Data Test)
# Sends valid data + one corrupt record + more valid data.
# Verifies the bridge skips the bad record and continues.
make produce-bad
```

## Key Features

* **Idempotency:** Enabled (`enable.idempotence=true`) to prevent duplicate writes during network retries.
* **Read Committed:** Consumers use `isolation.level=read_committed` to ignore transactional ghosts.
* **Smart Recovery:** Automatic distinction between fatal serialization errors (Data) and transient network errors (Infra).
* **Observability:** Exposes Micrometer/Prometheus metrics for `messages.processed` and `messages.failed`.
