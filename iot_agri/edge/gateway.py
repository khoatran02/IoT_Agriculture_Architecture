"""IoT gateway (the ESP32 / Raspberry Pi): polls every sensor each cycle and publishes telemetry over MQTT.

    python -m iot_agri.edge.gateway [--interval 0.5] [--no-loop] [--start 0] [--max-samples N]
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
        self.node.on_connect(self._announce)
        self._stop = threading.Event()

    def _announce(self) -> None:
        self.node.publish(topics.meta(self.site, self.id), {
            "node": self.id, "role": "gateway", "run_id": self.run_id, "interval_s": self.interval_s,
            "sensors": [{k: s[k] for k in ("name", "unit", "interface") if k in s} for s in self.sensors_cfg],
        }, retain=True)

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> int:
        self.node.start()
        i, seq = self.start % len(self.dataset), 0
        log.info("gateway %s: %d sensors, %d dataset rows, one sample every %.2fs",
                 self.id, len(self.sensors), len(self.dataset), self.interval_s)
        while not self._stop.is_set():
            ts = self.dataset.timestamps[i]
            for sensor in self.sensors:
                value = sensor.read(i)
                if value is None:  # sensor did not answer this cycle
                    continue
                self.node.publish(topics.sensor(self.site, sensor.name), {
                    "sensor": sensor.name, "value": value, "unit": sensor.unit, "interface": sensor.interface,
                    "seq": seq, "run_id": self.run_id, "gw": self.id, "ts": ts, "gw_ts": time.time(),
                }, qos=self.qos)
            seq += 1
            i += 1
            if self.max_samples is not None and seq >= self.max_samples:
                break
            if i >= len(self.dataset):
                if not self.loop:
                    break
                i = 0
            self._stop.wait(self.interval_s)
        self.node.stop()
        log.info("gateway stopped after %d samples", seq)
        return seq


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
