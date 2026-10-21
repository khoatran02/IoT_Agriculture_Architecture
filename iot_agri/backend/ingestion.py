"""Data ingestion: per-sensor MQTT messages -> one validated, gap-filled Sample per gateway cycle."""
import logging
import math
import time
from collections import Counter, deque
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class Sample:
    seq: int
    ts: str                      # measurement time (dataset time, ISO 8601)
    gateway: str
    values: dict[str, float]     # cleaned: one value per configured sensor
    quality: dict[str, str]      # per sensor: "ok" | "imputed_missing" | "imputed_range"
    recv_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {"seq": self.seq, "ts": self.ts, "gateway": self.gateway, "values": self.values,
                "quality": self.quality}


class Ingestion:
    def __init__(self, sensors_cfg: list[dict], on_sample, stale_after_s: float = 5.0, clock=time.time):
        self.expected = [s["name"] for s in sensors_cfg]
        self.ranges = {s["name"]: s.get("valid_range") for s in sensors_cfg}
        self.on_sample = on_sample
        self.stale_after_s = stale_after_s
        self._clock = clock
        self._pending: dict[tuple[str, int], dict] = {}
        self._emitted: deque = deque(maxlen=2048)   # (run_id, seq) already processed, for QoS 1 duplicates
        self._emitted_set: set = set()
        self._last_good: dict[str, float] = {}
        self.stats: Counter = Counter()

    def on_reading(self, msg: dict) -> None:
        try:
            sensor, seq, run_id = msg["sensor"], int(msg["seq"]), str(msg.get("run_id", ""))
        except (KeyError, TypeError, ValueError):
            self.stats["malformed"] += 1
            return
        if sensor not in self.ranges:
            self.stats["unknown_sensor"] += 1
            return
        key = (run_id, seq)
        if key in self._emitted_set:
            self.stats["duplicate"] += 1
            return
        # MQTT preserves per-publisher order, so a newer seq means older partial cycles are final.
        for old in [k for k in self._pending if k[0] == run_id and k[1] < seq]:
            self._finalize(old)
        pend = self._pending.setdefault(key, {"ts": msg.get("ts"), "gateway": msg.get("gw", ""),
                                              "raw": {}, "first": self._clock()})
        pend["raw"][sensor] = msg.get("value")
        if len(pend["raw"]) == len(self.expected):
            self._finalize(key)

    def flush_stale(self) -> None:
        """Emit cycles that stopped receiving readings (e.g. the gateway went quiet mid-cycle)."""
        now = self._clock()
        for key in [k for k, p in self._pending.items() if now - p["first"] > self.stale_after_s]:
            self._finalize(key)

    def _clean(self, name: str, raw) -> tuple[float | None, str]:
        ok = isinstance(raw, (int, float)) and not isinstance(raw, bool) and math.isfinite(raw)
        if not ok:
            return self._last_good.get(name), "imputed_missing"
        lo, hi = self.ranges[name] or (-math.inf, math.inf)
        if not lo <= raw <= hi:
            return self._last_good.get(name), "imputed_range"
        self._last_good[name] = raw
        return raw, "ok"

    def _finalize(self, key: tuple[str, int]) -> None:
        pend = self._pending.pop(key)
        if len(self._emitted) == self._emitted.maxlen:
            self._emitted_set.discard(self._emitted[0])
        self._emitted.append(key)
        self._emitted_set.add(key)

        values, quality = {}, {}
        for name in self.expected:
            value, q = self._clean(name, pend["raw"].get(name))
            if value is None:  # never seen a good value for this sensor: the cycle is unusable
                self.stats["dropped"] += 1
                log.warning("dropping cycle %s: no usable value for %s", key, name)
                return
            values[name], quality[name] = value, q
        self.stats["samples"] += 1
        ts = pend["ts"] or time.strftime("%Y-%m-%dT%H:%M:%S")
        self.on_sample(Sample(seq=key[1], ts=ts, gateway=pend["gateway"], values=values, quality=quality))
