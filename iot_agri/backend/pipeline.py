"""Wires the platform layer: MQTT -> Ingestion -> Inference -> Decision -> MQTT, plus DB and live events.

Everything runs under one RLock because inputs arrive on three threads: paho's network thread,
the 1 Hz ticker, and API requests (manual overrides).
"""
import logging
import threading
from collections import deque

from .. import topics
from ..mqtt import MqttNode
from .db import Database
from .decision import DecisionEngine
from .inference import InferenceService, load_model
from .ingestion import Ingestion

log = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, cfg: dict, db: Database, emit=lambda *_: None):
        self.cfg, self.db, self.emit = cfg, db, emit
        self.site = cfg["site"]
        self.qos = cfg["mqtt"].get("qos", 1)
        self.lock = threading.RLock()
        self.window = deque(maxlen=cfg["backend"]["window_size"])
        self.latest_sample: dict | None = None
        self.latest_prediction: dict | None = None
        self.nodes: dict[str, str] = {}

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
        self._stop = threading.Event()
        self._ticker = threading.Thread(target=self._tick_loop, name="ticker", daemon=True)

    def start(self) -> None:
        self.node.start()
        self._ticker.start()

    def stop(self) -> None:
        self._stop.set()
        if self._ticker.is_alive():
            self._ticker.join()
        self.node.stop()

    # --- MQTT in -----------------------------------------------------------------------------------
    def _on_sensor_msg(self, _topic: str, msg: dict) -> None:
        with self.lock:
            self.ingestion.on_reading(msg)

    def _on_status_msg(self, topic: str, msg: dict) -> None:
        with self.lock:
            self.decision.on_status(topics.actuator_name(topic), msg.get("state"), msg.get("cmd_id"),
                                    msg.get("ok", True))

    def _on_availability_msg(self, topic: str, msg: dict) -> None:
        node, status = topics.node_id(topic), msg.get("status")
        with self.lock:
            self.nodes[node] = status
        self.emit("node", {"node": node, "status": status})

    # --- the flow: sample -> model -> decision ------------------------------------------------------
    def _on_sample(self, sample) -> None:
        sample_id = self.db.insert_sample(sample)
        self.window.append(sample)
        self.latest_sample = sample.to_dict()
        self.emit("sample", self.latest_sample)
        # Safety uses every cleaned sample, even when model inference fails.
        self.decision.on_sample(sample)
        prediction = self.inference.run(list(self.window))
        if prediction is None:
            self.latest_prediction = None
            self.emit("prediction", None)
            return
        self.db.insert_prediction(sample_id, sample.ts, prediction)
        self.latest_prediction = {"ts": sample.ts, "model": prediction.model, "outputs": prediction.outputs,
                                  "scores": prediction.scores, "details": prediction.details}
        self.emit("prediction", self.latest_prediction)
        self.decision.on_prediction(sample, prediction)

    # --- MQTT out ----------------------------------------------------------------------------------
    def _publish_command(self, actuator: str, cmd: dict) -> None:
        self.node.publish(topics.actuator_cmd(self.site, actuator), cmd, qos=self.qos)

    # --- API entry points --------------------------------------------------------------------------
    def override(self, actuator: str, state: str, ttl_s: float | None) -> dict:
        with self.lock:
            return self.decision.override(actuator, state, ttl_s)

    def release(self, actuator: str) -> dict:
        with self.lock:
            return self.decision.release(actuator)

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "site": self.site,
                "model": self.inference.model.name,
                "sensors": [{k: s.get(k) for k in ("name", "unit", "interface")} for s in self.cfg["sensors"]],
                "actuator_labels": {a["name"]: a.get("label", a["name"]) for a in self.cfg["actuators"]},
                "sample": self.latest_sample,
                "prediction": self.latest_prediction,
                "actuators": self.decision.snapshot(),
                "nodes": dict(self.nodes),
                "ingestion": dict(self.ingestion.stats),
            }

    def _tick_loop(self) -> None:
        while not self._stop.wait(1.0):
            try:
                with self.lock:
                    self.ingestion.flush_stale()
                    self.decision.tick()
            except Exception:
                log.exception("tick failed")
