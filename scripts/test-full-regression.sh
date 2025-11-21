#!/bin/bash
set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "========================================"
echo "    STARTING FULL REGRESSION SUITE      "
echo "========================================"

# 1. Verify Project Root
if [ ! -f "pom.xml" ] || [ ! -f "Makefile" ]; then
    echo "Error: You must run this script from the Project Root."
    exit 1
fi

# 2. Make scripts executable (just in case)
chmod +x scripts/test-source-failure.sh
chmod +x scripts/test-target-failure.sh

# 3. Global Setup (Wipe once at the start)
echo -e "${BLUE}[SUITE] Initializing Fresh Infrastructure...${NC}"
make fullwipe > /dev/null 2>&1 || true
make start
make build > /dev/null

# ==============================================================================
# TEST 1: Source Registry Failure
# ==============================================================================
echo ""
echo "----------------------------------------"
echo "TEST 1/2: Source Registry Failure"
echo "----------------------------------------"
# Run with SKIP_CLEANUP=true so it leaves Docker running for the next test
export SKIP_CLEANUP=true
./scripts/test-source-failure.sh

# ==============================================================================
# TEST 2: Target Registry Failure
# ==============================================================================
echo ""
echo "----------------------------------------"
echo "TEST 2/2: Target Registry Failure"
echo "----------------------------------------"
# Last test can perform cleanup, or keep it true if you want to inspect afterwards
export SKIP_CLEANUP=false
./scripts/test-target-failure.sh

echo ""
echo "========================================"
echo -e "${GREEN}       ALL TESTS PASSED SUCCESSFULLY    ${NC}"
echo "========================================"