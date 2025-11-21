#!/bin/bash
set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

LOG_FILE="target-failure.log"

function log() { echo -e "${BLUE}[TARGET-TEST]${NC} $1"; }

if [ ! -f "pom.xml" ]; then
    echo -e "${RED}Error: Must run from project root.${NC}"
    exit 1
fi

function cleanup() {
    log "Stopping Bridge process..."
    pkill -f "kafka-bridge" || true
    pkill -f "spring-boot:run" || true

    docker unpause target-registry 2>/dev/null || true

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

log "Starting Bridge..."
pkill -f "kafka-bridge" || true

nohup mvn spring-boot:run \
    -Dspring-boot.run.profiles=local \
    -Dlogging.level.com.yourcompany.kafkabridge=DEBUG \
    > "$LOG_FILE" 2>&1 &

BRIDGE_PID=$!

# Wait for startup
log "Waiting for Bridge to initialize..."
MAX_RETRIES=30
COUNT=0
while ! grep -q "Started KafkaBridgeApplication" "$LOG_FILE"; do
    sleep 2
    COUNT=$((COUNT+1))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo -e "${RED}Bridge failed to start.${NC}"
        exit 1
    fi
done
log "Bridge UP."

# ==============================================================================
# 2. HAPPY PATH
# ==============================================================================
log ">>> PHASE 1: Happy Path"
make produce
sleep 5
if ! grep -q "Bridged" "$LOG_FILE"; then
    echo -e "${RED}✘ Initial produce failed.${NC}"
    exit 1
fi

# ==============================================================================
# 3. FAULT INJECTION
# ==============================================================================
log ">>> PHASE 2: Simulating Target Registry Outage"
docker pause target-registry

log "Producing data while Target Registry is FROZEN..."
make produce

log "Waiting 10s for backoff..."
sleep 10

if grep -qE "Connection refused|IO exception|timed out|Timeout" "$LOG_FILE"; then
    echo -e "${GREEN}✔ Bridge encountered timeout/error as expected.${NC}"
else
    log "Warning: Specific error not found yet. Continuing..."
fi

# ==============================================================================
# 4. RECOVERY
# ==============================================================================
log ">>> PHASE 3: Recovery"
docker unpause target-registry

log "Waiting for retry (40s)..."
sleep 40

# ==============================================================================
# 5. VERIFICATION
# ==============================================================================
log ">>> PHASE 4: Verification (Manual Consumer)"

# The consume command might fail if network is flaky, so we allow retry logic or simple exit code check
set +e
make consume
EXIT_CODE=$?
set -e

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✔ SUCCESS: All data recovered.${NC}"
else
    echo -e "${RED}✘ FAILURE: Data missing.${NC}"
    exit 1
fi