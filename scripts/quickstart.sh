#!/bin/bash

# Quick Start Script for Databricks Insight Agent
# This script sets up the environment and runs the application

echo "=================================="
echo "Databricks Insight Agent - Setup"
echo "=================================="
echo ""

# Check Python version
echo "Checking Python version..."
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Python version: $python_version"

if ! python3 -c 'import sys; exit(0 if sys.version_info >= (3, 9) else 1)'; then
    echo "❌ Error: Python 3.9+ is required"
    exit 1
fi

echo "✅ Python version OK"
echo ""

# Create virtual environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "✅ Virtual environment created"
else
    echo "✅ Virtual environment already exists"
fi

echo ""

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate
echo "✅ Virtual environment activated"
echo ""

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip -q
pip install -r requirements.txt -q
echo "✅ Dependencies installed"
echo ""

# Check for .env file
if [ ! -f ".env" ]; then
    echo "⚠️  No .env file found"
    echo "Creating .env from template..."
    cp .env.example .env
    echo "✅ .env file created"
    echo ""
    echo "⚠️  IMPORTANT: Please edit .env with your Databricks credentials:"
    echo "   - DATABRICKS_SERVER_HOSTNAME"
    echo "   - DATABRICKS_HTTP_PATH"
    echo "   - DATABRICKS_ACCESS_TOKEN"
    echo ""
    echo "Press Enter to edit .env now (or Ctrl+C to skip)..."
    read
    ${EDITOR:-nano} .env
else
    echo "✅ .env file found"
fi

echo ""

# Create data directories
echo "Creating data directories..."
mkdir -p data/csv data/documents data/cache data/logs
echo "✅ Data directories created"
echo ""

# Set PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
echo "✅ PYTHONPATH configured"
echo ""

# Run example to verify setup
echo "Running setup verification..."
python scripts/example_usage.py

echo ""
echo "=================================="
echo "Setup Complete!"
echo "=================================="
echo ""
echo "To start the application:"
echo ""
echo "  Streamlit UI:  ./run_app.sh"
echo "  CLI:           ./run_cli.sh"
echo "  Examples:      python scripts/example_usage.py"
echo "  Tests:         ./run_tests.sh"
echo ""
echo "For more information, see:"
echo "  - README.md                : Project overview"
echo "  - docs/SETUP_GUIDE.md      : Detailed setup instructions"
echo "  - docs/DEVELOPMENT.md      : Development guide"
echo ""
