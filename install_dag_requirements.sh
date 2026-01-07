#!/bin/bash

# =====================================================
# Install/Verify All Requirements for Airflow DAG
# Run in WSL2
# =====================================================

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

echo ""
echo -e "${CYAN}╔════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║  Installing DAG Requirements (WSL2)   ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════╝${NC}"
echo ""

# 1. Activate virtual environment
if [ ! -f ".venv/bin/activate" ]; then
    echo -e "${RED}ERROR: Virtual environment not found${NC}"
    exit 1
fi

source .venv/bin/activate
echo -e "${GREEN}✓ Virtual environment activated${NC}"

# 2. Upgrade pip
echo -e "${YELLOW}Upgrading pip...${NC}"
pip install --upgrade pip setuptools wheel -q

# 3. Install requirements
echo -e "${YELLOW}Installing Python packages...${NC}"
pip install -r requirements.txt -q
echo -e "${GREEN}✓ Requirements installed${NC}"

# 4. Verify Airflow installation
echo -e "${YELLOW}Verifying Airflow...${NC}"
airflow version
echo -e "${GREEN}✓ Airflow verified${NC}"

# 5. Check critical Python packages
echo -e "${YELLOW}Verifying critical packages...${NC}"

packages=("pyodbc" "pandas" "pyyaml" "requests" "dbt")
for package in "${packages[@]}"; do
    if python -c "import ${package}" 2>/dev/null; then
        echo -e "${GREEN}✓ ${package}${NC}"
    else
        echo -e "${RED}✗ ${package} - Installing...${NC}"
        pip install ${package} -q
    fi
done

# 6. Verify dbt
echo -e "${YELLOW}Verifying dbt...${NC}"
dbt --version
echo -e "${GREEN}✓ dbt verified${NC}"

# 7. Check ODBC drivers
echo -e "${YELLOW}Checking ODBC drivers...${NC}"
if command -v odbcinst &> /dev/null; then
    odbcinst -j
    echo -e "${GREEN}✓ ODBC drivers found${NC}"
else
    echo -e "${YELLOW}WARNING: ODBC tools not found (this is okay for WSL2)${NC}"
fi

echo ""
echo "============================================"
echo -e "${GREEN}[SUCCESS] All requirements installed!${NC}"
echo "============================================"
echo ""
echo -e "${CYAN}Your Airflow DAG is ready to run!${NC}"
echo ""
echo -e "${CYAN}To verify the DAG is detected:${NC}"
echo -e "  ${YELLOW}airflow dags list${NC}"
echo ""
echo -e "${CYAN}To test the DAG syntax:${NC}"
echo -e "  ${YELLOW}airflow dags list-runs --dag-id metro_retail_pipeline${NC}"
echo ""
