#!/bin/bash

# =====================================================
# Initialize MetroRetail Project in WSL2
# Smart setup - only installs what's needed
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
echo -e "${CYAN}║  MetroRetail WSL2 Initialization      ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════╝${NC}"
echo ""

# Check if already initialized
if [ -f ".venv/bin/activate" ] && [ -d "airflow_home" ] && [ -f "airflow_home/airflow.db" ]; then
    echo -e "${GREEN}✓ Already initialized!${NC}"
    echo ""
    echo -e "${CYAN}To start Airflow:${NC}"
    echo -e "  ${YELLOW}./start_airflow.sh${NC}"
    echo ""
    exit 0
fi

echo -e "${YELLOW}Running initialization...${NC}"
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 1. Update system packages
echo -e "${YELLOW}Updating Ubuntu packages...${NC}"
sudo apt-get update -qq
sudo apt-get upgrade -y -qq

# 2. Install system dependencies
echo -e "${YELLOW}Installing system dependencies...${NC}"
sudo apt-get install -y -qq \
    python3 \
    python3-venv \
    python3-pip \
    git \
    curl \
    wget \
    unixodbc \
    unixodbc-dev \
    odbc-postgresql \
    libpq-dev \
    libssl-dev \
    libffi-dev \
    build-essential \
    supervisor

# 3. Set Python 3 as default (if not already)
echo -e "${YELLOW}Setting up Python...${NC}"
python3 --version

# 4. Create virtual environment
if [ -d ".venv" ]; then
    echo -e "${YELLOW}Removing corrupted virtual environment...${NC}"
    rm -rf .venv
fi

echo -e "${YELLOW}Creating Python virtual environment...${NC}"
python3 -m venv .venv

if [ ! -f ".venv/bin/activate" ]; then
    echo -e "${RED}ERROR: Failed to create virtual environment${NC}"
    exit 1
fi

echo -e "${GREEN}Virtual environment created successfully${NC}"

# 5. Activate virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source .venv/bin/activate

if [ -z "$VIRTUAL_ENV" ]; then
    echo -e "${RED}ERROR: Failed to activate virtual environment${NC}"
    exit 1
fi

echo -e "${GREEN}Virtual environment activated${NC}"

# 6. Upgrade pip
echo -e "${YELLOW}Upgrading pip...${NC}"
pip install --upgrade pip setuptools wheel

# 7. Install Python dependencies
echo -e "${YELLOW}Installing Python dependencies...${NC}"
if [ ! -f "requirements.txt" ]; then
    echo -e "${RED}ERROR: requirements.txt not found${NC}"
    exit 1
fi

# Show pip install output to catch errors
pip install -r requirements.txt

# Verify Airflow is installed
if ! command -v airflow &> /dev/null; then
    echo -e "${RED}ERROR: Airflow installation failed${NC}"
    pip list | grep -i airflow
    exit 1
fi

echo -e "${GREEN}Python dependencies installed successfully${NC}"

# 8. Set Airflow home with proper WSL2 path
export AIRFLOW_HOME="$PWD/airflow_home"
mkdir -p $AIRFLOW_HOME

# Ensure AIRFLOW_HOME uses proper Linux path (no Windows paths)
# If PWD contains /mnt/c, it's already correct
if [[ ! "$AIRFLOW_HOME" =~ ^/ ]]; then
    echo -e "${RED}ERROR: AIRFLOW_HOME path is not absolute: $AIRFLOW_HOME${NC}"
    exit 1
fi

echo -e "${GREEN}Airflow home configured: $AIRFLOW_HOME${NC}"

# 9. Initialize Airflow database with SQLite (absolute path)
echo -e "${YELLOW}Initializing Airflow database (this may take a minute)...${NC}"

# Set database to use absolute path for SQLite
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:////${AIRFLOW_HOME}/airflow.db"

# Initialize database
airflow db init

if [ $? -ne 0 ]; then
    echo -e "${RED}ERROR: Failed to initialize Airflow database${NC}"
    exit 1
fi

echo -e "${GREEN}Airflow database initialized successfully${NC}"

# 10. Create admin user
echo -e "${YELLOW}Creating admin user...${NC}"
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@metroretail.com \
    --password admin123 \
    2>/dev/null || echo -e "${CYAN}Admin user already exists${NC}"

# 11. Create supervisor config for auto-start (optional)
echo -e "${YELLOW}Setting up supervisor for auto-start...${NC}"
sudo tee /etc/supervisor/conf.d/airflow.conf > /dev/null << EOF
[program:airflow-scheduler]
directory=$PWD
command=$PWD/.venv/bin/airflow scheduler
autostart=false
autorestart=true
stderr_logfile=$PWD/logs/scheduler.err.log
stdout_logfile=$PWD/logs/scheduler.out.log
user=$USER
environment=AIRFLOW_HOME=$AIRFLOW_HOME

[program:airflow-webserver]
directory=$PWD
command=$PWD/.venv/bin/airflow webserver --port 8080
autostart=false
autorestart=true
stderr_logfile=$PWD/logs/webserver.err.log
stdout_logfile=$PWD/logs/webserver.out.log
user=$USER
environment=AIRFLOW_HOME=$AIRFLOW_HOME
EOF

echo ""
echo "============================================" 
echo -e "${GREEN}[SUCCESS] Setup Complete!${NC}"
echo "============================================"
echo ""
echo -e "${CYAN}Next Steps:${NC}"
echo "1. Start Airflow: ${YELLOW}./start_airflow.sh${NC}"
echo "2. Open browser: ${YELLOW}http://localhost:8080${NC}"
echo "3. Login: ${YELLOW}admin / admin123${NC}"
echo ""
echo -e "${CYAN}Virtual environment activated!${NC}"
echo "Run ${YELLOW}./start_airflow.sh${NC} to start Airflow"
echo ""
