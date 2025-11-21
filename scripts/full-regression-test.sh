#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

LOG_FILE="bridge-regression.log"

function log() {
    echo -e "${BLUE}[TEST]${NC} $1"
}

function cleanup() {
    log "Cleaning up background processes..."
    pkill -f "kafka-bridge" || true
    pkill -f "spring-boot:run" || true
    # Ensure registry is unpaused in case we crash mid-test
    docker unpause target-registry 2>/dev/null || true
}
trap cleanup EXIT

# ==============================================================================
# 1. PREPARATION
# ==============================================================================
log "Ensuring environment is clean..."
cleanup
make clean > /dev/null 2>&1

log "Starting Infrastructure (make start)..."
make start
sleep 5

log "Building Application (make build)..."
make build > /dev/null

# ==============================================================================
# 2. START BRIDGE (Background)
# ==============================================================================
log "Starting Kafka Bridge in background (logging to $LOG_FILE)..."

# Enable DEBUG logs to see "Bridged" messages
export LOGGING_LEVEL_COM_YOURCOMPANY_KAFKABRIDGE=DEBUG

nohup mvn spring-boot:run \
    -Dspring-boot.run.profiles=local \
    > "$LOG_FILE" 2>&1 &

BRIDGE_PID=$!

log "Waiting for Bridge to initialize..."
MAX_RETRIES=30
COUNT=0
while ! grep -q "Started KafkaBridgeApplication" "$LOG_FILE"; do
    sleep 2
    COUNT=$((COUNT+1))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo -e "${RED}Bridge failed to start. Check $LOG_FILE${NC}"
        exit 1
    fi
    echo -n "."
done
echo ""
log "Bridge is UP (PID: $BRIDGE_PID)"

# ==============================================================================
# 3. PHASE 1: NORMAL OPERATION
# ==============================================================================
log ">>> PHASE 1: Happy Path Production"
make produce

log "Waiting for processing..."
sleep 5

# Check logs
if grep -q "Bridged" "$LOG_FILE"; then
    echo -e "${GREEN}Log confirmation: Messages bridged successfully.${NC}"
else
    echo -e "${RED}Bridge logs do not show success. Check $LOG_FILE.${NC}"
    echo "Tail of log file:"
    tail -n 10 "$LOG_FILE"
    exit 1
fi

# ==============================================================================
# 4. PHASE 2: FAULT INJECTION (Pause Target Registry)
# ==============================================================================
log ">>> PHASE 2: Fault Injection"
log "Pausing Target Schema Registry (Simulating Network Freeze)..."
# CHANGED: Use pause instead of stop to preserve In-Memory Schema Data
docker pause target-registry

log "Producing data while Target Registry is FROZEN..."
make produce

log "Waiting 10s to ensure Bridge hits the error and enters Backoff (30s)..."
sleep 10

# The error might be "Connection refused", "IO exception", or "timed out" depending on the freeze
if grep -qE "Connection refused|IO exception|timed out|Timeout" "$LOG_FILE"; then
    echo -e "${GREEN}Bridge encountered connection/timeout error as expected.${NC}"
else
    echo -e "${BLUE}Warning: Specific error text not found yet. Continuing to verify recovery...${NC}"
fi

# ==============================================================================
# 5. PHASE 3: RECOVERY
# ==============================================================================
log ">>> PHASE 3: Infrastructure Recovery"
log "Unpausing Target Schema Registry..."
docker unpause target-registry

log "Waiting for Registry to come up and Bridge to retry (approx 40s)..."
# 30s Backoff + buffer
sleep 40

# ==============================================================================
# 6. PHASE 4: VERIFICATION
# ==============================================================================
log ">>> PHASE 4: Final Verification (Consuming all messages)"
log "Running ManualConsumer to verify data integrity..."

set +e
make consume
EXIT_CODE=$?
set -e

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}=============================================${NC}"
    echo -e "${GREEN} REGRESSION TEST PASSED: All data recovered.${NC}"
    echo -e "${GREEN}=============================================${NC}"
else
    echo -e "${RED}=============================================${NC}"
    echo -e "${RED} REGRESSION TEST FAILED: Data missing.${NC}"
    echo -e "${RED}=============================================${NC}"
    exit 1
fi