# IoT Agriculture Architecture

Runnable implementation of the smart-farm architecture. The CSV in `../datasets/iot-agriculture-2024/` is replayed
as six simulated sensors; a rule-based model stands in for the deep-learning model until you plug yours in.

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
uv venv --python 3 .venv && uv pip install --python .venv/bin/python -r requirements.txt   # or python -m venv + pip
scripts/run_all.sh --interval 0.5      # broker + backend + controller + gateway, then open http://localhost:8000
.venv/bin/python -m pytest             # unit tests (no broker needed)
.venv/bin/python -m iot_agri.tools.evaluate   # score the configured model against the CSV labels
```

Or one terminal per process: `docker compose up -d`, then `python -m iot_agri.backend`, `python -m iot_agri.edge.controller`,
`python -m iot_agri.edge.gateway`. Everything reads `config.yaml`.

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
2. **Inference** gets the last `backend.window_size` samples and returns ON/OFF per actuator.
3. **Decision engine** turns that into commands. Priority: *interlock > manual override > model*. Auto mode holds a state
   for `min_dwell_minutes` of dataset time to stop chatter; commands go out on change only and are re-sent until the
   controller confirms. Interlock shipped: the irrigation pump is forced OFF below 10 % water level.
4. **API**: `GET /api/state|history|commands`, `POST /api/override {actuator, state, ttl_s}`, `DELETE /api/override/<actuator>`
   (back to auto), `WS /ws` (live `sample`, `prediction`, `command`, `actuator`, `node` events). Overrides expire after
   `decision.override_ttl_s`; an interlock refuses an unsafe override with HTTP 409.

## Plugging in the DL model

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

- The sensor "temperature" is the CSV column `tempreature` (typo in the source).
- Each `*_ON`/`*_OFF` column pair is one-hot, so each pair is one actuator state. These are the training labels, not sensors.
- The CSV is not in time order: it is sorted by `date` on load, and two rows with a blank date are dropped. About 2,970
  timestamps appear more than once with different values (it looks like several runs concatenated), so a sorted replay
  interleaves them; if your model needs contiguous runs, change the ordering in `device/dataset.py`.
- One row = 5 simulated minutes. The gateway replays one row per `gateway.interval_s` (default 1 s) and loops at the end.
- Many rows have `humidity = 0` and `water_level = 0`; they are passed through as real readings.

## Not implemented

Broker auth/TLS (dev Mosquitto is anonymous), real hardware drivers (sensors/relays are simulated), and a time-series
store (SQLite is enough for a replay; swap `backend/db.py` for InfluxDB/Timescale for production).
