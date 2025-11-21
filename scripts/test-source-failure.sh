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

    # Always ensure registry is recovered, even if skipping full cleanup
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
# Ensure Infra is UP (Idempotent)
make start

log ">>> PHASE 1: Pre-Produce Data"
# Produce data BEFORE bridge starts (so it sits in Source Kafka)
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
# Restart the app to get fresh logs
pkill -f "kafka-bridge" || true

# Start with DEBUG logs
nohup mvn spring-boot:run \
    -Dspring-boot.run.profiles=local \
    -Dlogging.level.com.yourcompany.kafkabridge=DEBUG \
    > "$LOG_FILE" 2>&1 &

BRIDGE_PID=$!
log "Bridge PID: $BRIDGE_PID. Waiting 15s for connection errors..."

sleep 15

if grep -qE "Connection refused|IO exception|timed out|Timeout" "$LOG_FILE"; then
    echo -e "${GREEN}✔ Bridge detected source registry failure.${NC}"
else
    echo -e "${RED}✘ Warning: Specific connection error not found in logs yet.${NC}"
fi

# Ensure NO messages were processed
if grep -q "Bridged" "$LOG_FILE"; then
    echo -e "${RED}✘ FAILURE: Bridge processed messages despite Registry being down!${NC}"
    exit 1
fi

# ==============================================================================
# 4. RECOVERY
# ==============================================================================
log ">>> PHASE 3: Recovery"
docker unpause source-registry

log "Waiting for Bridge to recover (40s)..."
sleep 40

if grep -q "Bridged" "$LOG_FILE"; then
    echo -e "${GREEN}✔ SUCCESS: Messages recovered.${NC}"
else
    echo -e "${RED}✘ FAILURE: Messages stuck/lost.${NC}"
    tail -n 20 "$LOG_FILE"
    exit 1
fi