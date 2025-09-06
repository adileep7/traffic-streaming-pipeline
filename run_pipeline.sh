#!/bin/bash
set -euo pipefail

echo "🚦 Traffic Streaming Pipeline Orchestrator"

# Step 1. Start Docker Desktop check
if ! docker info >/dev/null 2>&1; then
  echo "❌ Docker is not running. Please start Docker Desktop first."
  exit 1
fi

# Step 2. Start containers
echo "▶️ Starting containers..."
docker compose up -d

# Step 3. Activate venv (if exists)
if [ -d ".venv" ]; then
  echo "🐍 Activating virtual environment..."
  source .venv/bin/activate
else
  echo "⚠️ No .venv found. Make sure to create one and install requirements."
fi

# Step 4. Run Prefect flow
echo "🚀 Launching Prefect flow..."
python3 prefect/flow.py -- --hold
