#!/usr/bin/env bash
# Start broker + backend + actuator controller + gateway; Ctrl-C stops the three Python processes
# (the broker keeps running: `docker compose down` to stop it). Extra args go to the gateway,
# e.g. scripts/run_all.sh --interval 0.2
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PYTHON:-.venv/bin/python}
mkdir -p logs

docker compose up -d mosquitto

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT INT TERM

"$PY" -m iot_agri.backend > logs/backend.log 2>&1 & pids+=($!)
"$PY" -m iot_agri.edge.controller > logs/controller.log 2>&1 & pids+=($!)
sleep 2   # let the platform and controller subscribe before telemetry starts
"$PY" -m iot_agri.edge.gateway "$@" > logs/gateway.log 2>&1 & pids+=($!)

echo "dashboard: http://localhost:8000   logs: logs/*.log   Ctrl-C to stop"
wait "${pids[@]}"
