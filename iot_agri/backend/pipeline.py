"""Wires the platform layer: MQTT -> Ingestion -> Inference -> Decision -> MQTT, plus DB and live events.

Everything runs under one RLock because inputs arrive on three threads: paho's network thread,
the 1 Hz ticker, and API requests (manual overrides).

Besides the decisions themselves, the pipeline records a trace of every cycle (what each stage saw and did,
with timestamps) so the dashboard can show the flow step by step.
"""
import logging
import threading
import time
from collections import OrderedDict, deque

from .. import topics
from ..mqtt import MqttNode
from .db import Database
from .decision import DecisionEngine
from .inference import InferenceService, load_model
from .ingestion import Ingestion
from .tap import MqttTap

log = logging.getLogger(__name__)
REPLAYED_ACK_AFTER_S = 5   # a status older than this when it reaches us is a retained replay, not a live ACK


class Pipeline:
    def __init__(self, cfg: dict, db: Database, emit=lambda *_: None):
        self.cfg, self.db, self.emit = cfg, db, emit
        self.site = cfg["site"]
        self.gateway_id = cfg["gateway"]["id"]
        self.qos = cfg["mqtt"].get("qos", 1)
        self.lock = threading.RLock()
        self.window = deque(maxlen=cfg["backend"]["window_size"])
        self.latest_sample: dict | None = None
        self.latest_prediction: dict | None = None
        self.nodes: dict[str, str] = {}
        self.gateway_state: dict | None = None
        self.relays: dict[str, dict] = {}                  # last status each actuator reported
        self.cycles: deque = deque(maxlen=100)             # per-sample trace for the dashboard
        self.acks: OrderedDict[str, dict] = OrderedDict()  # cmd_id -> ACK timing from the controller

        sensor_names = [s["name"] for s in cfg["sensors"]]
        actuator_names = [a["name"] for a in cfg["actuators"]]
        self.ingestion = Ingestion(cfg["sensors"], self._on_sample)
        self.inference = InferenceService(load_model(
            cfg["inference"], sensor_names, actuator_names, base_dir=cfg.get("_base_dir"),
        ))
        if self.window.maxlen < self.inference.model.window_size:
            raise ValueError("backend.window_size must cover the model's window_size")
        self.node = MqttNode("backend", cfg["mqtt"]["host"], cfg["mqtt"]["port"],
                             availability_topic=topics.availability(self.site, "backend"), node_id="backend")
        self.decision = DecisionEngine(cfg, self._publish_command, emit, db)
        self.node.subscribe(topics.sensor_filter(self.site), self._on_sensor_msg, self.qos)
        self.node.subscribe(topics.actuator_status_filter(self.site), self._on_status_msg, self.qos)
        self.node.subscribe(topics.availability_filter(self.site), self._on_availability_msg, self.qos)
        self.node.subscribe(topics.state_filter(self.site), self._on_state_msg, self.qos)
        # Separate client on purpose: the tap must see what any other subscriber on the broker would see.
        self.tap_node = MqttNode("backend-tap", cfg["mqtt"]["host"], cfg["mqtt"]["port"])
        self.tap = MqttTap(self.tap_node, self.site, emit)
        self._stop = threading.Event()
        self._ticker = threading.Thread(target=self._tick_loop, name="ticker", daemon=True)

    def start(self) -> None:
        self.node.start()
        self.tap_node.start()
        self._ticker.start()

    def stop(self) -> None:
        self._stop.set()
        if self._ticker.is_alive():
            self._ticker.join()
        self.tap_node.stop()
        self.node.stop()

    # --- MQTT in -----------------------------------------------------------------------------------
    def _on_sensor_msg(self, _topic: str, msg: dict) -> None:
        with self.lock:
            self.ingestion.on_reading(msg)

    def _on_status_msg(self, topic: str, msg: dict) -> None:
        name, now = topics.actuator_name(topic), time.time()
        with self.lock:
            self.decision.on_status(name, msg.get("state"), msg.get("cmd_id"), msg.get("ok", True))
            relay = {"actuator": name, "state": msg.get("state"), "gpio": msg.get("gpio"),
                     "level": msg.get("level"), "t": now}
            self.relays[name] = relay
            ack = None
            if msg.get("cmd_id"):
                switched = msg.get("applied_ts") or msg.get("ts")
                ack = {"cmd_id": msg["cmd_id"], "actuator": name, "state": msg.get("state"),
                       "replayed": switched is not None and now - switched > REPLAYED_ACK_AFTER_S,
                       "ok": msg.get("ok", True), "error": msg.get("error"), "cmd_ts": msg.get("cmd_ts"),
                       "recv_ts": msg.get("recv_ts"), "applied_ts": msg.get("applied_ts"), "ack_at": now}
                self.acks[ack["cmd_id"]] = ack
                while len(self.acks) > 200:
                    self.acks.popitem(last=False)
        self.emit("relay", relay)
        if ack:
            self.emit("ack", ack)

    def _on_availability_msg(self, topic: str, msg: dict) -> None:
        node, status = topics.node_id(topic), msg.get("status")
        with self.lock:
            self.nodes[node] = status
        self.emit("node", {"node": node, "status": status})

    def _on_state_msg(self, topic: str, msg: dict) -> None:
        if topics.node_id(topic) != self.gateway_id:
            return
        with self.lock:
            self.gateway_state = msg
        self.emit("gateway", msg)

    # --- the flow: sample -> model -> decision ------------------------------------------------------
    def _on_sample(self, sample) -> None:
        sample_ns = self.db.insert_sample(sample)
        self.window.append(sample)
        self.latest_sample = sample.to_dict()
        self.emit("sample", self.latest_sample)
        # Safety uses every cleaned sample, even when model inference fails.
        forced = self.decision.on_sample(sample)
        t_infer0 = time.time()
        prediction = self.inference.run(list(self.window))
        t_infer1 = time.time()
        prediction_ns = None
        if prediction is None:
            self.latest_prediction = None
            self.emit("prediction", None)
            decisions = self.decision.on_no_prediction(sample, forced)
        else:
            prediction_ns = self.db.insert_prediction(sample_ns, sample.ts, prediction)
            self.latest_prediction = {"ts": sample.ts, "model": prediction.model, "outputs": prediction.outputs,
                                      "scores": prediction.scores, "details": prediction.details}
            self.emit("prediction", self.latest_prediction)
            decisions = self.decision.on_prediction(sample, prediction, forced)
        cycle = self._cycle(sample, sample_ns, prediction_ns, t_infer0, t_infer1, decisions)
        self.cycles.append(cycle)
        self.emit("cycle", cycle)

    def _cycle(self, sample, sample_ns, prediction_ns, t_infer0, t_infer1, decisions) -> dict:
        """Everything the stages did for one sample, for the dashboard's step-by-step view."""
        info = sample.info
        readings = []
        for s in self.cfg["sensors"]:
            name, recv = s["name"], info.get("recv", {}).get(s["name"])
            readings.append({
                "name": name, "unit": s.get("unit"), "interface": s.get("interface"),
                "raw": info.get("raw", {}).get(name), "value": sample.values[name], "quality": sample.quality[name],
                "recv_t": recv["t"] if recv else None,
                "net_ms": round((recv["t"] - recv["gw_ts"]) * 1000, 2) if recv and recv.get("gw_ts") else None,
            })
        return {
            "seq": sample.seq, "run_id": info.get("run_id"), "ts": sample.ts, "row": info.get("row"),
            "rows": info.get("rows"), "gateway": sample.gateway, "sensors": readings,
            "times": {"gateway": info.get("gw_ts"), "first_recv": info.get("first_recv"),
                      "last_recv": info.get("last_recv"), "ingested": info.get("finalized_at"),
                      "infer_start": t_infer0, "infer_end": t_infer1, "decided": time.time()},
            "ingestion_stats": info.get("stats"), "prediction": self.latest_prediction, "decisions": decisions,
            # When each point was written (seconds). InfluxDB has no row ids: a point is identified by its time.
            "db": {"sample_at": sample_ns / 1e9, "prediction_at": prediction_ns / 1e9 if prediction_ns else None},
            "commands": [d["cmd_id"] for d in decisions if d["cmd_id"]],
        }

    # --- MQTT out ----------------------------------------------------------------------------------
    def _publish_command(self, actuator: str, cmd: dict) -> None:
        self.node.publish(topics.actuator_cmd(self.site, actuator), cmd, qos=self.qos)

    def gateway_control(self, request: dict) -> dict:
        """Forward a playback/fault request to the gateway over MQTT. Raises ValueError if it is malformed."""
        action = request["action"]
        msg = {"action": action}
        if action == "interval":
            if request.get("interval_s") is None:
                raise ValueError("interval requires interval_s")
            msg["interval_s"] = request["interval_s"]
        elif action == "fault":
            if request.get("sensor") not in {s["name"] for s in self.cfg["sensors"]}:
                raise ValueError(f"unknown sensor {request.get('sensor')!r}")
            if request.get("kind") not in ("dropout", "spike"):
                raise ValueError("fault requires kind dropout or spike")
            msg.update(sensor=request["sensor"], kind=request["kind"], cycles=request.get("cycles", 3))
        self.node.publish(topics.gateway_control(self.site, self.gateway_id), msg, qos=self.qos)
        return msg

    # --- API entry points --------------------------------------------------------------------------
    def override(self, actuator: str, state: str, ttl_s: float | None) -> dict:
        with self.lock:
            return self.decision.override(actuator, state, ttl_s)

    def release(self, actuator: str) -> dict:
        with self.lock:
            return self.decision.release(actuator)

    def recent_cycles(self, limit: int = 60) -> list[dict]:
        with self.lock:
            return list(self.cycles)[-limit:]

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "site": self.site,
                "node_ids": {"gateway": self.gateway_id, "controller": self.cfg["controller"]["id"],
                             "backend": "backend"},
                "model": self.inference.model.name,
                "sensors": [{k: s.get(k) for k in ("name", "unit", "interface", "column", "valid_range")}
                            for s in self.cfg["sensors"]],
                "actuator_labels": {a["name"]: a.get("label", a["name"]) for a in self.cfg["actuators"]},
                "sample": self.latest_sample,
                "prediction": self.latest_prediction,
                "actuators": self.decision.snapshot(),
                "nodes": dict(self.nodes),
                "ingestion": dict(self.ingestion.stats),
                "gateway": self.gateway_state,
                "relays": dict(self.relays),
                "acks": list(self.acks.values())[-40:],
                "mqtt": self.tap.stats(),
                "db": self.db.counts(),
                "db_info": self.db.info(),
                "rules": self.decision.describe(),
            }

    def _tick_loop(self) -> None:
        while not self._stop.wait(1.0):
            try:
                with self.lock:
                    self.ingestion.flush_stale()
                    self.decision.tick()
            except Exception:
                log.exception("tick failed")
