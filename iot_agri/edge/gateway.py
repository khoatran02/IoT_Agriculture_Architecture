"""IoT gateway (the ESP32 / Raspberry Pi): polls every sensor each cycle and publishes telemetry over MQTT.

    python -m iot_agri.edge.gateway [--interval 0.5] [--no-loop] [--start 0] [--max-samples N]

Playback can be driven over MQTT (topic agri/<site>/node/<id>/control, JSON), which is what the dashboard does:
    {"action": "pause" | "resume" | "step" | "clear_faults"}
    {"action": "interval", "interval_s": 0.5}
    {"action": "fault", "sensor": "humidity", "kind": "dropout" | "spike", "cycles": 3}
The gateway reports its state (retained) on agri/<site>/node/<id>/state.
"""
import argparse
import logging
import signal
import threading
import time
import uuid

from .. import topics
from ..config import load_config, resolve_path, setup_logging
from ..device.dataset import Dataset
from ..device.sensors import build_sensors
from ..mqtt import MqttNode

log = logging.getLogger("gateway")
FAULT_KINDS = ("dropout", "spike")


class Gateway:
    def __init__(self, cfg: dict, interval_s=None, loop=None, start=0, max_samples=None):
        gw = cfg["gateway"]
        self.site, self.id = cfg["site"], gw["id"]
        self.interval_s = gw["interval_s"] if interval_s is None else interval_s
        self.loop = gw.get("loop", True) if loop is None else loop
        self.start, self.max_samples = start, max_samples
        self.qos = cfg["mqtt"].get("qos", 1)
        self.sensors_cfg = cfg["sensors"]
        self.dataset = Dataset(resolve_path(cfg, cfg["dataset"]["path"]))
        self.sensors = build_sensors(cfg["sensors"], self.dataset, gw.get("dropout_prob", 0.0))
        # run_id lets the backend tell a restarted gateway (seq back to 0) from duplicate deliveries.
        self.run_id = uuid.uuid4().hex[:8]
        self.node = MqttNode(f"{self.id}-{self.run_id}", cfg["mqtt"]["host"], cfg["mqtt"]["port"],
                             availability_topic=topics.availability(self.site, self.id), node_id=self.id)
        self.node.subscribe(topics.gateway_control(self.site, self.id), self._on_control)
        self.node.on_connect(self._announce)
        self._stop = threading.Event()
        self._wake = threading.Event()        # cuts the inter-sample wait short when a control message arrives
        self._step = threading.Event()
        self._ctl = threading.Lock()          # guards the fields the MQTT thread changes: below
        self._paused = False
        self._faults: dict[str, dict] = {}    # sensor -> {"kind", "cycles"} injected sensor faults
        self._i = self.start % len(self.dataset)   # next dataset row
        self._seq = 0

    def _announce(self) -> None:
        self.node.publish(topics.meta(self.site, self.id), {
            "node": self.id, "role": "gateway", "run_id": self.run_id, "interval_s": self.interval_s,
            "sensors": [{k: s[k] for k in ("name", "unit", "interface") if k in s} for s in self.sensors_cfg],
        }, retain=True)
        self._publish_state()

    def _publish_state(self) -> None:
        with self._ctl:
            state = {"node": self.id, "run_id": self.run_id, "paused": self._paused, "interval_s": self.interval_s,
                     "row": self._i, "rows": len(self.dataset), "seq": self._seq, "loop": self.loop,
                     "faults": {k: dict(v) for k, v in self._faults.items()}, "ts": time.time()}
        self.node.publish(topics.gateway_state(self.site, self.id), state, retain=True)

    def _on_control(self, _topic: str, msg: dict) -> None:
        action = msg.get("action")
        with self._ctl:
            if action == "pause":
                self._paused = True
            elif action == "resume":
                self._paused = False
            elif action == "step":            # one cycle, then stay paused
                self._paused = True
                self._step.set()
            elif action == "interval":
                interval = msg.get("interval_s")
                if not isinstance(interval, (int, float)) or not 0.01 <= interval <= 60:
                    log.warning("ignoring bad interval %r", interval)
                    return
                self.interval_s = float(interval)
            elif action == "fault":
                sensor, kind, cycles = msg.get("sensor"), msg.get("kind"), msg.get("cycles", 3)
                if sensor not in {s.name for s in self.sensors} or kind not in FAULT_KINDS \
                        or not isinstance(cycles, int) or not 1 <= cycles <= 1000:
                    log.warning("ignoring bad fault request %r", msg)
                    return
                self._faults[sensor] = {"kind": kind, "cycles": cycles}
                log.info("injecting %s on %s for %d cycles", kind, sensor, cycles)
            elif action == "clear_faults":
                self._faults.clear()
            else:
                log.warning("ignoring unknown control action %r", action)
                return
        self._wake.set()
        self._publish_state()

    def _spike_value(self, sensor_name: str) -> float:
        """A reading far outside the sensor's valid range, like a glitching ADC."""
        rng = next((s.get("valid_range") for s in self.sensors_cfg if s["name"] == sensor_name), None)
        return rng[1] * 10 + 1000 if rng else 99999

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def _publish_cycle(self) -> None:
        i, seq = self._i, self._seq
        ts = self.dataset.timestamps[i]
        with self._ctl:
            faults = {k: dict(v) for k, v in self._faults.items()}
        for sensor in self.sensors:
            value = sensor.read(i)
            kind = faults.get(sensor.name, {}).get("kind")
            if kind == "dropout":
                value = None
            elif kind == "spike":
                value = self._spike_value(sensor.name)
            if value is None:  # sensor did not answer this cycle
                continue
            self.node.publish(topics.sensor(self.site, sensor.name), {
                "sensor": sensor.name, "value": value, "unit": sensor.unit, "interface": sensor.interface,
                "seq": seq, "run_id": self.run_id, "gw": self.id, "ts": ts, "gw_ts": time.time(),
                "row": i, "rows": len(self.dataset),
            }, qos=self.qos)
        if faults:
            with self._ctl:
                for name in faults:
                    if name in self._faults:
                        self._faults[name]["cycles"] -= 1
                        if self._faults[name]["cycles"] <= 0:
                            del self._faults[name]

    def run(self) -> int:
        self.node.start()
        log.info("gateway %s: %d sensors, %d dataset rows, one sample every %.2fs",
                 self.id, len(self.sensors), len(self.dataset), self.interval_s)
        while not self._stop.is_set():
            with self._ctl:
                paused = self._paused
            if paused and not self._step.is_set():
                self._wake.wait(0.25)
                self._wake.clear()
                continue
            stepping = self._step.is_set()
            self._step.clear()
            self._publish_cycle()
            self._seq += 1
            self._i += 1
            if self.max_samples is not None and self._seq >= self.max_samples:
                break
            if self._i >= len(self.dataset):
                if not self.loop:
                    break
                self._i = 0
            if stepping or self._faults:
                self._publish_state()    # keep the dashboard's row/fault display current while it is watching
            self._wake.wait(self.interval_s)
            self._wake.clear()
        self.node.stop()
        log.info("gateway stopped after %d samples", self._seq)
        return self._seq


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config")
    ap.add_argument("--interval", type=float, help="seconds between samples (default: config)")
    ap.add_argument("--no-loop", action="store_true", help="stop at the end of the dataset")
    ap.add_argument("--start", type=int, default=0, help="first dataset row (time-sorted)")
    ap.add_argument("--max-samples", type=int)
    args = ap.parse_args()
    setup_logging()
    gw = Gateway(load_config(args.config), args.interval, False if args.no_loop else None,
                 args.start, args.max_samples)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: gw.stop())
    gw.run()


if __name__ == "__main__":
    main()
