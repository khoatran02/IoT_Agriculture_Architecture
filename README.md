# IoT Agriculture Architecture

Standalone implementation of the smart-farm architecture. The bundled CSV in `datasets/` is replayed
as six simulated sensors. The default inference model is the trained **MLP 6–16–8–3 (275 parameters), TFLite INT8**
exported from `AIoT-AI-models`. Its inference runtime, model artifacts and demo dataset are included here;
installation and execution require only this repository.

```
Sensors → Gateway → MQTT → Ingestion → Inference → Decision engine → MQTT → Actuator controller → Actuators
                              ↓                          ↑                            │
                          Database ← REST/WebSocket API ←┘ (manual override)          └─ status/ACK → MQTT → Ingestion
                                          ↓
                                  Web dashboard
```

| Layer | Code | Process |
|---|---|---|
| 1. Device | `iot_agri/device/` – `SimulatedSensor` per CSV column (temperature, humidity, water_level, N, P, K); `RelayActuator` per actuator (fan, irrigation_pump, water_pump) | inside gateway / controller |
| 2. Edge | `iot_agri/edge/gateway.py` polls the sensors and publishes telemetry; `edge/controller.py` switches relays and reports status/ACK | `python -m iot_agri.edge.gateway`, `python -m iot_agri.edge.controller` |
| 3. Platform | `iot_agri/backend/` – `ingestion.py`, `inference.py`, `decision.py`, `db.py` (SQLite), `app.py` (REST + WebSocket) wired by `pipeline.py` | `python -m iot_agri.backend` |
| 4. Client | `iot_agri/client/static/index.html` served by the backend at `/` | browser |
| Broker | Mosquitto (`docker-compose.yml`) | `docker compose up -d` |

## Run

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -r requirements.txt
source .venv/bin/activate
scripts/run_all.sh --interval 0.5      # broker + backend + controller + gateway, then open http://localhost:8000
.venv/bin/python -m pytest             # unit tests (no broker needed)
.venv/bin/python -m iot_agri.tools.evaluate   # score the configured model against the CSV labels
```

Or one terminal per process: `docker compose up -d`, then `python -m iot_agri.backend`, `python -m iot_agri.edge.controller`,
`python -m iot_agri.edge.gateway`. Everything reads `config.yaml`.

Run these commands from this repository's directory. Requirements install the runtime dependencies from the package
index; no sibling checkout, local wheel or `aiot_ai` package is needed. Inference uses NumPy and LiteRT without
TensorFlow, pandas or scikit-learn. The inference code is in `iot_agri/backend/models/mlp_runtime.py`, the CSV is in
`datasets/IoTProcessed_Data.csv`, and the five deployment artifacts are in `models/mlp_v1/`.
Artifact structure, tensor shapes and decision thresholds are validated at startup. If upgrading an environment used before this
standalone integration, rerun the requirements install and restart the backend, controller and gateway.
Docker Desktop / a Docker daemon must be running for `scripts/run_all.sh`. Set `AGRI_CONFIG=/absolute/path/config.yaml`
to use one alternate configuration across all three processes. The launcher's other arguments go to the gateway.

## MQTT topics (`iot_agri/topics.py`)

| Topic | Publisher | Payload |
|---|---|---|
| `agri/<site>/sensor/<name>` | gateway | `{sensor, value, unit, seq, run_id, ts, gw_ts}` – one message per sensor per cycle |
| `agri/<site>/actuator/<name>/cmd` | decision engine | `{cmd_id, state: ON/OFF, source: auto/manual/retry, reason}` |
| `agri/<site>/actuator/<name>/status` (retained) | controller | `{state, cmd_id, ok}` – `cmd_id` set means it is the ACK for that command |
| `agri/<site>/node/<id>/availability` (retained, LWT) | every process | `{node, status: online/offline}` |

## How the platform decides

1. **Ingestion** groups the per-sensor messages of one gateway cycle into a `Sample`, drops QoS-1 duplicates, range-checks
   each value (`valid_range` in config) and fills missing/out-of-range readings with the last good one (flagged in `quality`).
2. **Inference** uses the latest sample (`window_size: 1`). The bundled runtime orders the six named features,
   standardizes them with the saved train statistics, quantizes INT8 inputs, dequantizes outputs, and applies the saved
   per-actuator thresholds (fan ≈0.50, irrigation 0.52, water pump 0.74). It loads the interpreter once.
3. **Decision engine** turns that into commands. Priority: *interlock > manual override > model*. Auto mode holds a state
   for `min_dwell_minutes` of dataset time to stop chatter; commands go out on change only and are re-sent until the
   controller confirms. Interlock shipped: the irrigation pump is forced OFF below 10 % water level.
   Interlocks run on every cleaned sample, including when inference fails, and bypass dwell even if the model also
   requests OFF. A failed prediction clears the current prediction; other actuator targets remain at their last state.
4. **API**: `GET /api/state|history|commands`, `POST /api/override {actuator, state, ttl_s}`, `DELETE /api/override/<actuator>`
   (back to auto), `WS /ws` (live `sample`, `prediction`, `command`, `actuator`, `node` events). Overrides expire after
   `decision.override_ttl_s`; an interlock refuses an unsafe override with HTTP 409.

`/api/state` and WebSocket `prediction` events include outputs, ON scores, model/preprocessing version, variant,
inference latency and out-of-training-range features. Predictions and diagnostics are stored in SQLite; older databases
receive the new diagnostics column automatically. The dashboard shows model proposals separately from confirmed
actuator states, because interlocks, dwell and manual overrides can change the final command. OOD values are flagged;
they are not automatically rejected. Sensor imputation flags remain in each sample's `quality`.

## Model configuration

The integrated default is:

```yaml
backend:
  window_size: 1
inference:
  model: iot_agri.backend.models.aiot_mlp:AIoTMLP
  params:
    model_dir: models/mlp_v1
    variant: int8
    threads: 1
```

Set `variant: fp32` to use the other export of the same checkpoint. Model paths resolve relative to the config file.
Keep both TFLite files, preprocessing, thresholds and metadata together. See [model bundle notes](models/README.md)
for updating artifacts. No training or holdout evaluation is triggered by starting the platform.

Everything the model touches is `InferenceModel` in `backend/inference.py`: `predict(window) → Prediction`, where `window`
is a list of `Sample` (use `self.features(window)` for a `window_size × 6` matrix) and the result is P(ON) or ON/OFF per
actuator. Fill in `backend/models/dl_template.py`, then set in `config.yaml`:

```yaml
inference:
  model: iot_agri.backend.models.dl_template:DLModel
  params: {weights_path: models/actuator_net.pt}
```

Nothing else changes – ingestion, decision logic, API and dashboard are model-agnostic. `python -m iot_agri.tools.evaluate`
scores whichever model is configured against the CSV's `*_ON` columns. The shipped `threshold` model gets
~97 % / 99.5 % / 99 % accuracy (fan / irrigation / water pump) on that data.

## Dataset notes

- `dataset.path` defaults to `datasets/IoTProcessed_Data.csv`, resolved relative to `config.yaml`.
  This is a regular file included in the repository. See [dataset notes](datasets/README.md) for its structure.
- The sensor "temperature" is the CSV column `tempreature` (typo in the source).
- Each `*_ON`/`*_OFF` column pair is one-hot, so each pair is one actuator state. These are the training labels, not sensors.
- The CSV is not in time order: it is sorted by `date` on load, and two rows with a blank date are dropped. About 2,970
  timestamps appear more than once with different values (it looks like several runs concatenated), so a sorted replay
  interleaves them; if your model needs contiguous runs, change the ordering in `device/dataset.py`.
- One row = 5 simulated minutes. The gateway replays one row per `gateway.interval_s` (default 1 s) and loops at the end.
- Many rows have `humidity = 0` and `water_level = 0`; they are passed through as real readings.

## Validation and review

```bash
.venv/bin/python -m pytest -q
# Optional: use an isolated broker, not an active farm broker (client IDs are fixed).
AGRI_TEST_MQTT_PORT=1883 .venv/bin/python -m pytest -q tests/test_mqtt_integration.py
```

The tests check both variants against historical validation predictions (including a score above 0.5 but below the
water-pump threshold), invalid thresholds, feature order, a standalone copy with external model imports blocked, DB migration,
API/WebSocket diagnostics, interlocks during inference failure, and an optional gateway → MQTT → MLP → controller → ACK
round trip. The offline evaluation command scores **all replay rows**, including train/validation/test; its aggregate
accuracy is a replay sanity check, not an independent estimate of generalization or of the final control policy.

Repository layout, independent installation and model updates: [docs/INDEPENDENT_DEPLOYMENT.md](docs/INDEPENDENT_DEPLOYMENT.md).

## Not implemented

Broker auth/TLS (dev Mosquitto is anonymous), real hardware drivers (sensors/relays are simulated), and a time-series
store (SQLite is enough for a replay; swap `backend/db.py` for InfluxDB/Timescale for production).
The API has no authentication, readings have no freshness timeout, and hardware-local interlocks/watchdogs are not
implemented. Current safety checks use the latest cleaned readings and depend on the backend/broker being available.
