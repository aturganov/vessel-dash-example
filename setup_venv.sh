#!/bin/bash
# Setup script for Ship Data ETL Pipeline Virtual Environment
# This script creates a Python virtual environment and installs dependencies

set -e  # Exit on any error

# Configuration
PROJECT_NAME="ship_etl"
PYTHON_VERSION="3.12"
VENV_DIR=~/git


echo "Setting up Python virtual environment for Ship Data ETL Pipeline..."

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is not installed. Please install Python 3.12 or later."
    exit 1
fi

# Check Python version
PYTHON_VER=$(python3 --version | cut -d' ' -f2)
echo "Found Python version: $PYTHON_VER"

# Create virtual environment
echo "Creating virtual environment in $VENV_DIR..."
echo "python3 -m venv $VENV_DIR/venv$PYTHON_VERSION"
python3 -m venv $VENV_DIR/venv$PYTHON_VERSION

# Activate virtual environment
echo "Activating virtual environment..."
source "$VENV_DIR/venv$PYTHON_VERSION/bin/activate"

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "Installing dependencies from requirements.txt..."
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
else
    echo "requirements.txt not found. Installing basic dependencies..."
    pip install pandas psycopg2-binary SQLAlchemy openpyxl python-dateutil pytz python-dotenv
fi

echo "Virtual environment setup completed!"
echo ""
echo "To activate the environment, run:"
echo "source $VENV_DIR/venv$PYTHON_VERSION/bin/activate"
echo ""
echo "To deactivate, run:"
echo "deactivate"
echo ""
echo "Next steps:"
echo "1. Start PostgreSQL with: docker-compose up -d"
echo "2. Create database schema: docker-compose exec postgres psql -U vesseluser -d vesseldb -f /docker-entrypoint-initdb.d/01-create-schema.sql"
echo "3. Run ETL script: source $VENV_DIR/venv$PYTHON_VERSION/bin/activate && python etl_ship_data.py"