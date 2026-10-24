"""Actuator controller: turns MQTT commands into relay/GPIO switching and reports status/ACKs back.

    python -m iot_agri.edge.controller
"""
import argparse
import logging
import signal
import threading
import time

from .. import topics
from ..config import load_config, setup_logging
from ..device.actuators import RelayActuator
from ..mqtt import MqttNode

log = logging.getLogger("controller")


class ActuatorController:
    def __init__(self, cfg: dict):
        ctrl = cfg["controller"]
        self.site, self.id = cfg["site"], ctrl["id"]
        self.actuation_delay_s = ctrl.get("actuation_delay_s", 0.0)
        self.relays = {a["name"]: RelayActuator(a["name"], a.get("gpio")) for a in cfg["actuators"]}
        self.node = MqttNode(self.id, cfg["mqtt"]["host"], cfg["mqtt"]["port"],
                             availability_topic=topics.availability(self.site, self.id), node_id=self.id)
        self.node.subscribe(topics.actuator_cmd_filter(self.site), self._on_command)
        self.node.on_connect(self._report_all)
        self._stop = threading.Event()

    def _status(self, name: str, cmd_id=None, ok=True, error=None, **timing) -> None:
        relay = self.relays[name]
        payload = {"actuator": name, "state": relay.state, "gpio": relay.gpio,
                   "level": "HIGH" if relay.state == "ON" else "LOW", "cmd_id": cmd_id, "ok": ok,
                   "ts": time.time(), **timing}
        if error:
            payload["error"] = error
        # Retained so a restarted backend learns the physical state immediately.
        self.node.publish(topics.actuator_status(self.site, name), payload, retain=True)

    def _report_all(self) -> None:
        for name in self.relays:
            self._status(name)

    def _on_command(self, topic: str, msg: dict) -> None:
        name, cmd_id = topics.actuator_name(topic), msg.get("cmd_id")
        recv_ts = time.time()
        # An ACK carries the command's own timestamp plus ours, so the backend can time every hop.
        timing = {"cmd_ts": msg.get("ts"), "recv_ts": recv_ts}
        if name not in self.relays:
            log.warning("command for unknown actuator %r", name)
            return
        time.sleep(self.actuation_delay_s)  # relay + mechanical latency
        try:
            changed = self.relays[name].set(msg.get("state"))
        except ValueError as e:
            self._status(name, cmd_id, ok=False, error=str(e), applied_ts=time.time(), **timing)
            return
        log.info("cmd %s: %s -> %s (%s%s)", cmd_id, name, msg["state"], msg.get("source", "?"),
                 "" if changed else ", already in state")
        self._status(name, cmd_id, applied_ts=time.time(), **timing)  # status doubles as the ACK for cmd_id

    def run(self) -> None:
        self.node.start()
        log.info("controller %s driving %s", self.id, ", ".join(self.relays))
        self._stop.wait()
        self.node.stop()

    def stop(self) -> None:
        self._stop.set()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config")
    args = ap.parse_args()
    setup_logging()
    ctrl = ActuatorController(load_config(args.config))
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: ctrl.stop())
    ctrl.run()


if __name__ == "__main__":
    main()
