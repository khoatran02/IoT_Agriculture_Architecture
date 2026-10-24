# IoT Agriculture Architecture

Standalone implementation of the smart-farm architecture. The bundled CSV in `datasets/` is replayed
as six simulated sensors. The default inference model is the trained **MLP 6–16–8–3 (275 parameters), TFLite INT8**
exported from `AIoT-AI-models`. Its inference runtime, model artifacts and demo dataset are included here;
installation and execution require only this repository.

```
Sensors → Gateway → MQTT → Ingestion → Inference → Decision engine → MQTT → Actuator controller → Actuators
                              ↓                          ↑                            │
                         InfluxDB ← REST/WebSocket API ←┘ (manual override)          └─ status/ACK → MQTT → Ingestion
                                          ↓
                                  Web dashboard
```

| Layer | Code | Process |
|---|---|---|
| 1. Device | `iot_agri/device/` – `SimulatedSensor` per CSV column (temperature, humidity, water_level, N, P, K); `RelayActuator` per actuator (fan, irrigation_pump, water_pump) | inside gateway / controller |
| 2. Edge | `iot_agri/edge/gateway.py` polls the sensors and publishes telemetry; `edge/controller.py` switches relays and reports status/ACK | `python -m iot_agri.edge.gateway`, `python -m iot_agri.edge.controller` |
| 3. Platform | `iot_agri/backend/` – `ingestion.py`, `inference.py`, `decision.py`, `db.py` (InfluxDB), `app.py` (REST + WebSocket) wired by `pipeline.py` | `python -m iot_agri.backend` |
| 4. Client | `iot_agri/client/static/index.html` served by the backend at `/` | browser |
| Broker, database | Mosquitto and InfluxDB 2.7 (`docker-compose.yml`) | `docker compose up -d --wait` |

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
| `agri/<site>/sensor/<name>` | gateway | `{sensor, value, unit, seq, run_id, ts, gw_ts, row, rows}` – one message per sensor per cycle; `row` is the dataset row |
| `agri/<site>/actuator/<name>/cmd` | decision engine | `{cmd_id, state: ON/OFF, source: auto/manual/retry, reason}` |
| `agri/<site>/actuator/<name>/status` (retained) | controller | `{state, gpio, level, cmd_id, ok}` – `cmd_id` set means it is the ACK for that command; an ACK also carries `cmd_ts`, `recv_ts`, `applied_ts` so each hop can be timed |
| `agri/<site>/node/<id>/availability` (retained, LWT) | every process | `{node, status: online/offline}` |
| `agri/<site>/node/<gw>/state` (retained) | gateway | `{paused, interval_s, row, rows, seq, faults, …}` – playback state |
| `agri/<site>/node/<gw>/control` | backend (from the dashboard) | `{action: pause/resume/step/interval/fault/clear_faults, …}` – see `edge/gateway.py` |

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
   (back to auto), `WS /ws` (live `sample`, `prediction`, `cycle`, `mqtt`, `command`, `ack`, `actuator`, `relay`, `gateway`,
   `node` events). Overrides expire after `decision.override_ttl_s`; an interlock refuses an unsafe override with HTTP 409.
   For the step-by-step dashboard: `GET /api/cycles` (per-sample trace), `GET /api/mqtt` (recent broker traffic) and
   `POST /api/gateway/control` (pause / resume / step / interval / inject a sensor fault).

`/api/state` and WebSocket `prediction` events include outputs, ON scores, model/preprocessing version, variant,
inference latency and out-of-training-range features. Predictions and diagnostics are stored in InfluxDB (see
[Storage](#storage-influxdb)). The dashboard shows model proposals separately from confirmed
actuator states, because interlocks, dwell and manual overrides can change the final command. OOD values are flagged;
they are not automatically rejected. Sensor imputation flags remain in each sample's `quality`.

## Dashboard: follow one sample through every step

Open <http://localhost:8000>. The **Data flow** strip shows the nine steps of the architecture; click one to inspect it.
Each step shows what it actually received and did for the selected sample cycle (click a cycle in the list to pin it;
*Follow live* un-pins):

| Step | What you can see |
|---|---|
| 1 Sensors | each CSV column as a simulated sensor: its bus, and the value it returned for the current dataset row; buttons to inject faults |
| 2 Gateway | run id, seq, next dataset row, playback state, active faults, the last telemetry JSON on the wire |
| 3 MQTT · telemetry / 7 MQTT · command | a live tap on the broker (its own subscriber to `agri/<site>/#`): who published, topic, QoS, retained, transit time, full JSON on click. Filter by telemetry / commands & ACKs / nodes / control; *Freeze* to read |
| 4 Ingestion | raw value received, arrival offset, range check, value used, and whether a gap was filled; duplicate/dropped counters |
| 5 Inference | the feature vector, P(ON) per actuator, proposed state, inference time, out-of-range inputs, model details |
| 6 Rule engine | per actuator: model proposal, every interlock checked (with the sensor value), min-dwell state, manual override, and the outcome (sent / unchanged / held / forced) |
| 8 Controller | each command's hops: MQTT → controller, relay switching, ACK → platform |
| 9 Relays | GPIO pin and level per relay, wanted vs confirmed state |
| InfluxDB, API | connection state, points stored per measurement, points that failed to write, and when the selected cycle's points were written; connected clients and event counts |

The **Sample cycle** panel is a waterfall of the selected cycle (gateway → broker → ingestion → inference → rule engine,
plus the command/ACK hops of any command it triggered).

To watch slowly, use the header controls (they drive the simulated gateway over MQTT): **Pause**, **Step** (publish exactly one
cycle, then stay paused) and the speed selector. To see ingestion do its job, open step 1 and press **Drop ×3** (the sensor
stops answering) or **Spike ×3** (it reports an impossible value), then look at step 4. These controls change only the
simulated gateway; the platform is unaware of them, as it would be with real hardware.

The **☀ Light / 🌙 Dark** button in the header switches the colour theme. The choice is remembered in the browser; until you
choose, the page follows your operating-system setting.

Notes: timings compare timestamps from different processes, so they assume one clock (true here; on separate hosts they
need synchronised time). An ACK that reaches the platform long after the controller sent it (the retained status replayed
after the platform restarted) is shown as "replayed" instead of a latency.

## Storage (InfluxDB)

Samples, predictions, commands, ACKs and actuator status are stored in **InfluxDB 2.x** (`influxdb:2.7.12`, the same
version as the course's `influxdb-explore` project). `docker compose up -d --wait` starts it next to Mosquitto;
`scripts/run_all.sh` does this for you. The UI is at <http://localhost:8086> (demo login `admin` / `admin-password-123`,
org `my-org`, bucket `iot_agri`, token `my-token`: local demo credentials, applied only when the volume is first created, so
change them before exposing the port). Connection settings are the `influxdb:` section of `config.yaml`; override them
with `INFLUXDB_URL`, `INFLUXDB_ORG`, `INFLUXDB_BUCKET`, `INFLUXDB_TOKEN`. The backend creates the bucket (no expiry) if it
is missing, so it also works against another InfluxDB 2.x instance, such as the one from `influxdb-explore`.

| Measurement | Tags | Fields |
|---|---|---|
| `sample` | `gateway` | each sensor (float), `seq` (int), `sim_ts`, `quality` (JSON), `run_id` |
| `prediction` | `model` | `on_<actuator>` (0/1), `score_<actuator>`, `inference_ms`, `data_status`, `ood_count`, `sample_ref`, `sim_ts`, `details_json` |
| `command` | `actuator`, `source` | `cmd_id`, `state`, `reason`, `sent_at`, `sim_ts` |
| `ack` | – | `cmd_id`, `state`, `acked_at` |
| `actuator_status` | `actuator` | `state`, `cmd_id` |

Design points worth knowing:

- **Points are stamped with wall-clock time; dataset time is the `sim_ts` field.** The replay loops and the CSV repeats about
  3,000 timestamps, and InfluxDB treats the same measurement + tags + time as one point, so dataset time would silently
  overwrite data. Point times are also kept strictly increasing, and never in the future (queries end at "now").
- **An ACK is a separate `ack` point** joined to its command when read (earliest ACK wins), because time-series points are
  not updated in place. This also survives a platform restart, when the controller's retained status is replayed.
- **Writes are batched in the background (200 ms).** The MQTT thread that runs the safety interlocks never waits for the
  database, and a database failure can never stop control: failed writes are counted and shown in the dashboard's InfluxDB
  step instead of raised. The newest fraction of a second may not be queryable yet. Counters are read from the stored data
  at startup, so they are correct after a restart.
- **If InfluxDB is down**, the platform keeps running and keeps retrying; `/api/history` and `/api/commands` answer `503`.
  Points queued during a long outage are not guaranteed to be kept.
- Sensor values are always written as floats: InfluxDB rejects a field that changes type.

Query it from the InfluxDB UI (Data Explorer → Script Editor), for example the last hour of humidity, and commands per actuator:

```flux
from(bucket: "iot_agri") |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sample" and r._field == "humidity")

from(bucket: "iot_agri") |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "command" and r._field == "state")
  |> group(columns: ["actuator"]) |> count()
```

The old SQLite file (`data/agri.db`) is no longer read or written; it is left in place and there is no migration tool.

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
# Optional: against a real InfluxDB (each test uses and deletes its own bucket).
AGRI_TEST_INFLUX_URL=http://localhost:8086 .venv/bin/python -m pytest -q tests/test_influx_integration.py
```

The tests check both variants against historical validation predictions (including a score above 0.5 but below the
water-pump threshold), invalid thresholds, feature order, a standalone copy with external model imports blocked, InfluxDB point layout and behaviour when the database is unreachable,
API/WebSocket diagnostics, interlocks during inference failure, and an optional gateway → MQTT → MLP → controller → ACK
round trip. The offline evaluation command scores **all replay rows**, including train/validation/test; its aggregate
accuracy is a replay sanity check, not an independent estimate of generalization or of the final control policy.

Repository layout, independent installation and model updates: [docs/INDEPENDENT_DEPLOYMENT.md](docs/INDEPENDENT_DEPLOYMENT.md).

## Not implemented

Broker auth/TLS (dev Mosquitto is anonymous), real hardware drivers (sensors/relays are simulated), InfluxDB users and
tokens beyond the single demo admin token, and retention/downsampling (the bucket keeps everything).
The API has no authentication, readings have no freshness timeout, and hardware-local interlocks/watchdogs are not
implemented. Current safety checks use the latest cleaned readings and depend on the backend/broker being available.
