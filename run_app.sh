#!/bin/bash

# Run Streamlit App from Project Root
# This script ensures the app can find all modules

cd "$(dirname "$0")/.."
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

streamlit run src/ui/app.py "$@"
