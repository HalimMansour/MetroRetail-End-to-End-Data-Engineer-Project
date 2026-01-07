#!/bin/bash

# =====================================================
# Start Airflow Services in WSL2
# =====================================================

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo ""
echo -e "${CYAN}╔════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║  Starting Airflow Services (WSL2)     ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════╝${NC}"
echo ""

# Activate virtual environment
if [ ! -d ".venv" ]; then
    echo -e "${RED}ERROR: Virtual environment not found at .venv${NC}"
    echo -e "${YELLOW}Please run: ./init_airflow.sh${NC}"
    exit 1
fi

source .venv/bin/activate

# Set Airflow home with proper path format
export AIRFLOW_HOME="$PWD/airflow_home"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:////${AIRFLOW_HOME}/airflow.db"
mkdir -p $AIRFLOW_HOME

# Create logs directory
mkdir -p $PWD/logs

echo -e "${GREEN}Virtual environment activated${NC}"
echo -e "${GREEN}Airflow home: $AIRFLOW_HOME${NC}"
echo ""

# Check if Airflow is already running
if pgrep -f "airflow scheduler" > /dev/null; then
    echo -e "${YELLOW}WARNING: Airflow scheduler is already running${NC}"
fi

if pgrep -f "airflow webserver" > /dev/null; then
    echo -e "${YELLOW}WARNING: Airflow webserver is already running${NC}"
fi

echo ""
echo -e "${YELLOW}Starting Airflow Scheduler...${NC}"
nohup airflow scheduler > logs/scheduler.log 2>&1 &
SCHEDULER_PID=$!
echo -e "${GREEN}Scheduler started (PID: $SCHEDULER_PID)${NC}"

sleep 2

echo -e "${YELLOW}Starting Airflow Webserver...${NC}"
nohup airflow webserver --port 8080 > logs/webserver.log 2>&1 &
WEBSERVER_PID=$!
echo -e "${GREEN}Webserver started (PID: $WEBSERVER_PID)${NC}"

sleep 3

echo ""
echo -e "${GREEN}✓ Airflow services started!${NC}"
echo ""
echo -e "${CYAN}Access Airflow:${NC}"
echo -e "  URL: ${YELLOW}http://localhost:8080${NC}"
echo -e "  Username: ${YELLOW}admin${NC}"
echo -e "  Password: ${YELLOW}admin123${NC}"
echo ""
echo -e "${CYAN}View Logs:${NC}"
echo -e "  Scheduler: ${YELLOW}tail -f logs/scheduler.log${NC}"
echo -e "  Webserver: ${YELLOW}tail -f logs/webserver.log${NC}"
echo ""
echo -e "${CYAN}Stop Airflow:${NC}"
echo -e "  ${YELLOW}./stop_airflow.sh${NC}"
echo ""
echo -e "${YELLOW}Note: Services are running in the background.${NC}"
echo -e "${YELLOW}Keep WSL2 terminal open or use nohup to background.${NC}"
echo ""
