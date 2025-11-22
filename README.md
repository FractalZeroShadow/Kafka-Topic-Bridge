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

## Error Handling Priority & Behavior

The bridge uses a tiered error handling strategy. It distinguishes between **Permanent Failures** (which must be skipped immediately) and **Transient/Ambiguous Failures** (which must be retried to prevent data loss).

### Error Classification Matrix

| Exception / Scenario | Classification | Behavior | Outcome |
| :--- | :--- | :--- | :--- |
| **Schema Not Found (404)**<br>`ArtifactNotFoundException` | **Permanent Data Error** | **Fail Fast** | Record is **immediately** sent to DLT. Processing continues. |
| **Bad Configuration**<br>`ClassCastException`<br>`IllegalArgumentException` | **Permanent Code Error** | **Fail Fast** | Record is **immediately** sent to DLT. Processing continues. |
| **Registry/Broker Down**<br>`ConnectException`<br>`SocketTimeoutException` | **Infrastructure** | **Retry** | Enter Backoff Loop (`30s` -> `5m` -> `15m`). Service halts until connectivity is restored. |
| **Deserialization Error**<br>`DeserializationException` | **Ambiguous** | **Retry** | **Safe Mode:** Because Registry timeouts often look like deserialization errors, these are **retried** to ensure the service survives outages.<br><br>If the error is a true "Poison Pill" (bad bytes), it will be retried until the backoff exhausts, and *then* sent to DLT. |

### Retry Strategy (Backoff)
For **Infrastructure** and **Ambiguous** errors, the bridge applies the following non-blocking backoff policy defined in `application.yaml`:
1.  **Wait 30s** (First attempt)
2.  **Wait 5m** (Second attempt)
3.  **Wait 15m** (Subsequent attempts)

If the error resolves (e.g., Registry comes back online), the bridge resumes normal processing automatically.

### Dead Letter Topic (DLT)
Records that fail permanently (or exhaust retries) are routed to a specific DLT based on the topic mapping:
* **Preserves Data:** Infrastructure failures are written as valid Avro (if possible).
* **Preserves Evidence:** "Poison Pills" are written as raw bytes (using `ByteArraySerializer`) to ensure the corrupt payload can be inspected without crashing the DLT consumer.
