"""MQTT wire tap: an independent subscriber on agri/<site>/# that shows the dashboard what is on the broker.

It uses its own client (not the pipeline's), so what it reports is what any other subscriber would receive:
telemetry, commands, status/ACKs, node availability, retained state.
"""
import json
import time
from collections import Counter, deque


def classify(topic: str) -> tuple[str, str]:
    """topic -> (kind, publisher) for agri/<site>/..."""
    parts = topic.split("/")
    if len(parts) >= 4 and parts[2] == "sensor":
        return "telemetry", "gateway"
    if len(parts) >= 5 and parts[2] == "actuator":
        return ("command", "rule engine") if parts[-1] == "cmd" else ("status", "controller")
    if len(parts) >= 5 and parts[2] == "node":
        kind = {"availability": "availability", "meta": "meta", "state": "gateway-state",
                "control": "control"}.get(parts[-1], "other")
        return kind, "dashboard" if kind == "control" else parts[3]
    return "other", "?"


class MqttTap:
    OTHER_KEEP = 100

    def __init__(self, node, site: str, emit=lambda *_: None, keep: int = 300, clock=time.time):
        self._emit, self._clock, self._keep = emit, clock, keep
        # One ring per kind: telemetry arrives ~10x faster than anything else and would evict commands, ACKs
        # and node state from a shared buffer within seconds.
        self.buffers: dict[str, deque] = {}
        self.counts: Counter = Counter()
        self.total = 0
        # QoS 1 so the delivered QoS shown to the user is what the publisher used (delivery is capped at the
        # subscription's QoS; a QoS 0 tap would report every message as QoS 0).
        node.monitor(f"agri/{site}/#", self._on_message, 1)

    def _on_message(self, topic: str, raw: bytes, qos: int, retain: bool) -> None:
        now = self._clock()
        try:
            payload = json.loads(raw.decode())
        except ValueError:
            payload = raw.decode(errors="replace")[:500]
        kind, publisher = classify(topic)
        self.total += 1
        self.counts[kind] += 1
        # Publishers stamp wall-clock time in gw_ts / ts; telemetry's "ts" is dataset time, so only numbers count.
        sent = next((payload[k] for k in ("gw_ts", "ts") if isinstance(payload, dict)
                     and isinstance(payload.get(k), (int, float)) and payload[k] > 1e9), None)
        record = {"id": self.total, "t": now, "topic": topic, "kind": kind, "from": publisher, "qos": qos,
                  "retain": retain, "bytes": len(raw), "payload": payload,
                  "latency_ms": None if sent is None or retain else round((now - sent) * 1000, 1)}
        self.buffers.setdefault(kind, deque(maxlen=self._keep if kind == "telemetry" else self.OTHER_KEEP)).append(record)
        self._emit("mqtt", record)

    def recent(self, limit: int = 200) -> list[dict]:
        """The last `limit` messages of each kind, oldest first."""
        return sorted((r for buf in self.buffers.values() for r in list(buf)[-limit:]), key=lambda r: r["id"])

    def stats(self) -> dict:
        return {"total": self.total, "by_kind": dict(self.counts)}
