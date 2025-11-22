#!/bin/bash
set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

LOG_FILE="source-failure.log"

function log() { echo -e "${BLUE}[SOURCE-TEST]${NC} $1"; }

# 1. Verify Root
if [ ! -f "pom.xml" ]; then
    echo -e "${RED}Error: Must run from project root (e.g., ./scripts/test-source-failure.sh)${NC}"
    exit 1
fi

function cleanup() {
    log "Stopping Bridge process..."
    pkill -f "kafka-bridge" || true
    pkill -f "spring-boot:run" || true

    # Always ensure registry is recovered
    docker unpause source-registry 2>/dev/null || true

    if [ "$SKIP_CLEANUP" == "true" ]; then
        log "Skipping Docker cleanup (SKIP_CLEANUP=true)"
    else
        log "Wiping Docker environment..."
        make stop > /dev/null 2>&1
    fi
}
trap cleanup EXIT

# ==============================================================================
# 1. SETUP
# ==============================================================================
make start

log ">>> PHASE 1: Pre-Produce Data"
make produce

# ==============================================================================
# 2. INFRASTRUCTURE FAILURE
# ==============================================================================
log ">>> PHASE 2: Simulating Source Registry Outage"
docker pause source-registry

# ==============================================================================
# 3. START BRIDGE
# ==============================================================================
log "Starting Bridge (Expectations: Retry Loop, NO Crash)..."
pkill -f "kafka-bridge" || true

# CHANGED: Passed logging level inside arguments to guarantee propagation
# CHANGED: 10 retries of 1s to ensure quick recovery after the TCP timeout
nohup mvn spring-boot:run \
    -Dspring-boot.run.profiles=local \
    -Dspring-boot.run.arguments="--logging.level.com.yourcompany.kafkabridge=DEBUG --bridge.retry-intervals-ms=1000,1000,1000,1000,1000,1000,1000,1000,1000,1000" \
    > "$LOG_FILE" 2>&1 &

BRIDGE_PID=$!
log "Bridge PID: $BRIDGE_PID. Waiting 100s for TCP timeout (approx 94s)..."

sleep 100

# CHANGED: We now accept "Record in retry" as a sign that the error handler caught the timeout
if grep -qE "Connection refused|IO exception|timed out|Timeout|Record in retry" "$LOG_FILE"; then
    echo -e "${GREEN}✔ Bridge detected source registry failure (Backoff Active).${NC}"
else
    echo -e "${RED}✘ Warning: Specific connection error or retry log not found.${NC}"
fi

if grep -q "Bridged" "$LOG_FILE"; then
    echo -e "${RED}✘ FAILURE: Bridge processed messages despite Registry being down!${NC}"
    exit 1
fi

# ==============================================================================
# 4. RECOVERY
# ==============================================================================
log ">>> PHASE 3: Recovery"
docker unpause source-registry

log "Waiting for Bridge to recover (30s)..."
sleep 30

if grep -q "Bridged" "$LOG_FILE"; then
    echo -e "${GREEN}✔ SUCCESS: Messages recovered.${NC}"
else
    echo -e "${RED}✘ FAILURE: Messages stuck/lost.${NC}"
    echo "Tail of logs:"
    tail -n 20 "$LOG_FILE"
    exit 1
fi