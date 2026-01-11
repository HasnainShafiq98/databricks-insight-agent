#!/bin/bash

# Run CLI from Project Root
# This script ensures the CLI can find all modules

cd "$(dirname "$0")/.."
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

python src/ui/main.py "$@"
