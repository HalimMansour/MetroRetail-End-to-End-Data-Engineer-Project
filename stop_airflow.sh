#!/bin/bash

# =====================================================
# Stop Airflow Services
# =====================================================

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo ""
echo -e "${CYAN}Stopping Airflow Services...${NC}"
echo ""

# Stop scheduler
if pgrep -f "airflow scheduler" > /dev/null; then
    echo -e "${YELLOW}Stopping scheduler...${NC}"
    pkill -f "airflow scheduler"
    sleep 1
    echo -e "${GREEN}Scheduler stopped${NC}"
else
    echo -e "${YELLOW}Scheduler is not running${NC}"
fi

# Stop webserver
if pgrep -f "airflow webserver" > /dev/null; then
    echo -e "${YELLOW}Stopping webserver...${NC}"
    pkill -f "airflow webserver"
    sleep 1
    echo -e "${GREEN}Webserver stopped${NC}"
else
    echo -e "${YELLOW}Webserver is not running${NC}"
fi

echo ""
echo -e "${GREEN}All services stopped${NC}"
echo ""
