#!/bin/bash

# Run Tests from Project Root

cd "$(dirname "$0")/.."
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

pytest tests/ -v --cov=src --cov-report=term-missing "$@"
