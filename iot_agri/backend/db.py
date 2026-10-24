"""InfluxDB 2.x storage for samples, predictions, commands, ACKs and actuator status.

Schema (bucket from config; one point per event, timestamped with *wall-clock* time):

  sample           tag gateway | fields <sensor>... (float), seq (int), sim_ts, quality (JSON), run_id
  prediction       tag model   | fields on_<actuator> (0/1), score_<actuator>, inference_ms, data_status,
                                        ood_count, sample_ref, sim_ts, details_json
  command          tags actuator, source | fields cmd_id, state, reason, sent_at, sim_ts
  ack              (no tags)   | fields cmd_id, state, acked_at
  actuator_status  tag actuator | fields state, cmd_id

Why wall-clock time and not the dataset's: the replay loops, and the CSV itself repeats ~3,000 timestamps, so using
dataset time would make points overwrite each other (InfluxDB identifies a point by measurement + tags + time).
Dataset time is kept in `sim_ts`. Timestamps are made strictly increasing per process for the same reason.

Writes are batched in the background so a slow or unreachable database never delays the MQTT thread that runs the
safety interlocks; failures are counted and surfaced (see `info()`), never raised. An ACK is its own point and is joined
to its command at read time, because time-series points are not updated in place.
"""
import json
import logging
import math
import threading
import time

from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision

log = logging.getLogger(__name__)

SAMPLE, PREDICTION, COMMAND, ACK, STATUS = "sample", "prediction", "command", "ack", "actuator_status"
# A field that every point of the measurement has, used to count points.
_COUNT_FIELD = {SAMPLE: "seq", PREDICTION: "sample_ref", COMMAND: "sent_at", ACK: "acked_at", STATUS: "state"}
_COUNT_KEY = {SAMPLE: "samples", PREDICTION: "predictions", COMMAND: "commands", ACK: "acks", STATUS: "actuator_status"}
_SAMPLE_META = {"seq", "sim_ts", "quality", "run_id"}
_FLUX_COLUMNS = {"result", "table", "_time", "_start", "_stop", "_measurement", "gateway"}
# Reads look at a short window first and widen it only if there are too few points: cheap while data flows.
WINDOWS = ("15m", "6h", "3d", "90d")
RECONNECT_EVERY_S = 5


class DatabaseUnavailable(Exception):
    """InfluxDB cannot be reached (or the bucket cannot be prepared)."""


def _finite(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


# --- point builders (pure: unit-testable without a server) -------------------------------------------
def sample_point(s, ns: int) -> Point:
    p = Point(SAMPLE).tag("gateway", s.gateway or "unknown")
    for name, value in s.values.items():
        if _finite(value):
            p.field(name, float(value))      # always float: InfluxDB rejects a field that changes type
    p.field("seq", int(s.seq)).field("sim_ts", s.ts).field("quality", json.dumps(s.quality))
    if s.info.get("run_id"):
        p.field("run_id", str(s.info["run_id"]))
    return p.time(ns, WritePrecision.NS)


def prediction_point(sample_ref: int, sim_ts: str, pred, ns: int) -> Point:
    p = Point(PREDICTION).tag("model", pred.model).field("sample_ref", int(sample_ref)).field("sim_ts", sim_ts)
    for actuator, state in pred.outputs.items():
        p.field(f"on_{actuator}", 1 if state == "ON" else 0)
    for actuator, score in (pred.scores or {}).items():
        if _finite(score):
            p.field(f"score_{actuator}", float(score))
    details = pred.details or {}
    if _finite(details.get("inference_ms")):
        p.field("inference_ms", float(details["inference_ms"]))
    if details.get("data_status") is not None:
        p.field("data_status", str(details["data_status"]))
    p.field("ood_count", len(details.get("out_of_range_features") or []))
    if pred.details is not None:
        p.field("details_json", json.dumps(pred.details))
    return p.time(ns, WritePrecision.NS)


def command_point(c: dict, ns: int) -> Point:
    p = (Point(COMMAND).tag("actuator", c["actuator"]).tag("source", c["source"] or "unknown")
         .field("cmd_id", c["cmd_id"]).field("state", c["state"]).field("reason", c.get("reason") or "")
         .field("sent_at", float(c["sent_at"])))
    if c.get("sim_ts"):
        p.field("sim_ts", c["sim_ts"])
    return p.time(ns, WritePrecision.NS)


def ack_point(cmd_id: str, at: float, state: str, ns: int) -> Point:
    return (Point(ACK).field("cmd_id", cmd_id).field("state", state or "").field("acked_at", float(at))
            .time(ns, WritePrecision.NS))


def status_point(actuator: str, state: str, cmd_id, ns: int) -> Point:
    return (Point(STATUS).tag("actuator", actuator).field("state", state or "")
            .field("cmd_id", cmd_id or "").time(ns, WritePrecision.NS))


class Database:
    """cfg is the config's `influxdb` section: url, org, bucket, token."""

    def __init__(self, cfg: dict):
        self.url, self.org, self.bucket = cfg["url"], cfg["org"], cfg["bucket"]
        self._client = InfluxDBClient(url=self.url, token=cfg["token"], org=self.org, timeout=10_000)
        self._writer = self._client.write_api(
            write_options=WriteOptions(batch_size=500, flush_interval=200, max_retries=3),
            error_callback=self._on_write_error)
        self._query = self._client.query_api()
        self._lock = threading.Lock()
        self._last_ns = 0
        self._counts = {k: 0 for k in _COUNT_KEY.values()}
        self.write_errors = 0              # points that could not be stored
        self.last_error: str | None = None
        self.ready = False
        self._last_attempt = 0.0
        self._connect()

    # --- connection ----------------------------------------------------------------------------------
    def _connect(self) -> None:
        """Check the server, make sure the bucket exists, and seed the point counters from what is stored."""
        self._last_attempt = time.monotonic()
        try:
            if not self._client.ping():
                raise DatabaseUnavailable(f"no response from {self.url}")
            buckets = self._client.buckets_api()
            if buckets.find_bucket_by_name(self.bucket) is None:
                buckets.create_bucket(bucket_name=self.bucket, org=self.org)   # no expiry: keep the history
                log.info("created InfluxDB bucket %r", self.bucket)
            counts = {key: self._stored_count(m) for m, key in _COUNT_KEY.items()}
            with self._lock:
                self._counts = counts
            self.ready = True
            log.info("InfluxDB ready: %s org=%s bucket=%s (%s)", self.url, self.org, self.bucket, counts)
        except Exception as e:
            self.ready = False
            self.last_error = f"cannot prepare InfluxDB: {e}"[:300]
            log.warning("%s; the platform keeps running and will retry", self.last_error)

    def _need_ready(self) -> None:
        if not self.ready and time.monotonic() - self._last_attempt >= RECONNECT_EVERY_S:
            self._connect()
        if not self.ready:
            raise DatabaseUnavailable(self.last_error or "InfluxDB not ready")

    def _stored_count(self, measurement: str) -> int:
        flux = (f'from(bucket: {json.dumps(self.bucket)}) |> range(start: 0) '
                f'|> filter(fn: (r) => r._measurement == {json.dumps(measurement)} '
                f'and r._field == {json.dumps(_COUNT_FIELD[measurement])}) |> group() |> count()')
        return sum(int(r.get_value()) for t in self._query.query(flux) for r in t.records)

    def close(self) -> None:
        try:
            self._writer.close()           # flushes what is still queued
        finally:
            self._client.close()

    # --- writes: queued, never raise -----------------------------------------------------------------
    def _next_ns(self, at: float | None = None) -> int:
        """Unique, increasing point time. A caller-supplied time is clamped to now: queries end at 'now', so a point
        stamped in the future would be invisible, and the increasing-time rule would drag later points along with it."""
        with self._lock:
            now = time.time_ns()
            ns = max(min(int(at * 1e9), now) if at else now, self._last_ns + 1)
            self._last_ns = ns
            return ns

    def _put(self, point: Point, measurement: str) -> None:
        try:
            self._writer.write(bucket=self.bucket, org=self.org, record=point)
            with self._lock:
                self._counts[_COUNT_KEY[measurement]] += 1
        except Exception as e:             # a bad point must not stop the control loop
            self.write_errors += 1
            self.last_error = f"write rejected: {e}"[:300]
            log.exception("could not queue %s point", measurement)

    def _on_write_error(self, _conf, data, exception) -> None:
        """Called from the writer thread when a batch is finally rejected, after retries."""
        lines = (data.decode() if isinstance(data, bytes) else str(data)).splitlines()
        with self._lock:
            self.write_errors += len(lines)
            for line in lines:             # keep the counters honest: these points are not stored
                key = _COUNT_KEY.get(line.split(",", 1)[0].split(" ", 1)[0])
                if key and self._counts[key] > 0:
                    self._counts[key] -= 1
            self.last_error = f"write failed: {str(exception).splitlines()[0]}"[:300]
        log.error("dropped %d point(s): %s", len(lines), self.last_error)

    def insert_sample(self, s) -> int:
        ns = self._next_ns()
        self._put(sample_point(s, ns), SAMPLE)
        return ns

    def insert_prediction(self, sample_ref: int, sim_ts: str, p) -> int:
        ns = self._next_ns()
        self._put(prediction_point(sample_ref, sim_ts, p, ns), PREDICTION)
        return ns

    def insert_command(self, c: dict) -> int:
        ns = self._next_ns(c["sent_at"])
        self._put(command_point(c, ns), COMMAND)
        return ns

    def mark_ack(self, cmd_id: str, at: float, state: str) -> None:
        self._put(ack_point(cmd_id, at, state, self._next_ns(at)), ACK)

    def insert_status(self, at: float, actuator: str, state: str, cmd_id) -> int:
        ns = self._next_ns(at)
        self._put(status_point(actuator, state, cmd_id, ns), STATUS)
        return ns

    # --- reads ---------------------------------------------------------------------------------------
    def _rows(self, measurement: str, limit: int, start: str | int | None = None) -> list[dict]:
        """The newest `limit` points of a measurement, fields pivoted into columns, newest first."""
        self._need_ready()
        windows = (start,) if start is not None else WINDOWS
        rows: list[dict] = []
        for window in windows:
            rng = f"-{window}" if isinstance(window, str) else str(int(window))
            flux = (f'from(bucket: {json.dumps(self.bucket)}) |> range(start: {rng}) '
                    f'|> filter(fn: (r) => r._measurement == {json.dumps(measurement)}) '
                    f'|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value") '
                    f'|> group() |> sort(columns: ["_time"], desc: true) |> limit(n: {int(limit)})')
            try:
                rows = [r.values for t in self._query.query(flux) for r in t.records]
            except Exception as e:
                self.ready = False
                self.last_error = f"query failed: {e}"[:300]
                raise DatabaseUnavailable(self.last_error) from e
            if len(rows) >= limit:
                break
        return rows

    def recent_samples(self, limit: int = 200) -> list[dict]:
        """Oldest first, in the shape the API and dashboard use."""
        out = []
        for r in reversed(self._rows(SAMPLE, limit)):
            try:
                quality = json.loads(r.get("quality") or "{}")
            except ValueError:
                quality = {}
            out.append({"seq": int(r["seq"]), "ts": r.get("sim_ts"), "gateway": r.get("gateway"),
                        "values": {k: v for k, v in r.items()
                                   if k not in _SAMPLE_META and k not in _FLUX_COLUMNS and v is not None},
                        "quality": quality})
        return out

    def recent_commands(self, limit: int = 50) -> list[dict]:
        """Newest first, each joined with its earliest ACK (if one arrived)."""
        cmds = self._rows(COMMAND, limit)
        if not cmds:
            return []
        oldest = min(c["sent_at"] for c in cmds)
        acks: dict[str, dict] = {}
        for a in self._rows(ACK, 5000, start=int(oldest) - 1):
            first = acks.get(a["cmd_id"])
            if first is None or a["acked_at"] < first["acked_at"]:
                acks[a["cmd_id"]] = a
        return [{"cmd_id": c["cmd_id"], "sent_at": c["sent_at"], "sim_ts": c.get("sim_ts"),
                 "actuator": c["actuator"], "state": c["state"], "source": c["source"], "reason": c.get("reason"),
                 "acked_at": acks[c["cmd_id"]]["acked_at"] if c["cmd_id"] in acks else None,
                 "ack_state": acks[c["cmd_id"]]["state"] if c["cmd_id"] in acks else None} for c in cmds]

    # --- status ----------------------------------------------------------------------------------------
    def counts(self) -> dict[str, int]:
        """Points stored per measurement: counted once at start, then kept in step with writes."""
        with self._lock:
            return dict(self._counts)

    def info(self) -> dict:
        return {"engine": "InfluxDB 2.x", "url": self.url, "org": self.org, "bucket": self.bucket,
                "connected": self.ready, "write_errors": self.write_errors, "last_error": self.last_error}
