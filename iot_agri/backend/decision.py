"""Decision / rule engine: model predictions + safety rules + manual overrides -> actuator commands.

Priority per actuator: interlock > manual override > model (auto). Commands are sent on change only;
unconfirmed commands are re-sent by tick() until the controller reports the desired state.
"""
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime

log = logging.getLogger(__name__)


class OverrideBlocked(Exception):
    """A manual override was refused by a safety interlock."""


@dataclass
class ActuatorState:
    name: str
    mode: str = "auto"                  # "auto" | "manual"
    desired: str | None = None
    confirmed: str | None = None        # last state reported by the controller
    last_sent: str | None = None
    last_sent_at: float = 0.0           # wall clock
    last_change_sim: str | None = None  # dataset time of the last state change (for min dwell)
    override_until: float | None = None
    reason: str = ""


def _minutes_between(earlier: str, later: str) -> float:
    return (datetime.fromisoformat(later) - datetime.fromisoformat(earlier)).total_seconds() / 60


class DecisionEngine:
    def __init__(self, cfg: dict, publish_command, emit=lambda *_: None, db=None, clock=time.time):
        dec = cfg.get("decision", {})
        self.min_dwell = dec.get("min_dwell_minutes", 0)
        self.ack_timeout = dec.get("ack_timeout_s", 5)
        self.override_ttl = dec.get("override_ttl_s", 300)
        self.interlocks = dec.get("interlocks", [])
        self.actuators = {a["name"]: ActuatorState(a["name"]) for a in cfg["actuators"]}
        self._publish, self._emit, self._db, self._clock = publish_command, emit, db, clock
        self.latest_values: dict[str, float] = {}
        self.sim_ts: str | None = None

    # --- inputs ------------------------------------------------------------------------------------
    def on_prediction(self, sample, prediction) -> None:
        self.latest_values, self.sim_ts = sample.values, sample.ts
        for name, st in self.actuators.items():
            if st.mode == "manual":
                # An interlock still wins over a standing override.
                target, reason = self._interlock(name, st.desired)
                if reason:
                    st.mode, st.override_until = "auto", None
                    self._emit("actuator", self._view(st))
                    self._set_desired(st, target, "auto", reason)
                continue
            proposed = prediction.outputs.get(name)
            if proposed is None:
                continue
            target, interlock_reason = self._interlock(name, proposed)
            if interlock_reason:
                self._set_desired(st, target, "auto", interlock_reason)
            elif self._dwell_ok(st):
                self._set_desired(st, target, "auto", f"model:{prediction.model}")

    def on_status(self, name: str, state: str, cmd_id: str | None, ok: bool = True) -> None:
        st = self.actuators.get(name)
        if st is None:
            return
        st.confirmed = state
        if self._db:
            self._db.insert_status(self._clock(), name, state, cmd_id)
            if cmd_id and ok:
                self._db.mark_ack(cmd_id, self._clock(), state)
        self._emit("actuator", self._view(st))

    def override(self, name: str, state: str, ttl_s: float | None = None) -> dict:
        st = self._get(name)
        target, reason = self._interlock(name, state)
        if reason:
            raise OverrideBlocked(reason)
        st.mode = "manual"
        st.override_until = self._clock() + (self.override_ttl if ttl_s is None else ttl_s)
        self._set_desired(st, target, "manual", "dashboard override")
        self._emit("actuator", self._view(st))
        return self._view(st)

    def release(self, name: str) -> dict:
        st = self._get(name)
        st.mode, st.override_until = "auto", None
        self._emit("actuator", self._view(st))
        return self._view(st)

    def tick(self) -> None:
        """Expire overrides and re-send commands the controller has not confirmed."""
        now = self._clock()
        for st in self.actuators.values():
            if st.mode == "manual" and st.override_until is not None and now >= st.override_until:
                log.info("%s: override expired, back to auto", st.name)
                self.release(st.name)
            if st.desired is not None and st.confirmed != st.desired and now - st.last_sent_at >= self.ack_timeout:
                log.warning("%s: no confirmation of %s, re-sending", st.name, st.desired)
                self._send(st, st.desired, "retry", "no ack")

    # --- internals ---------------------------------------------------------------------------------
    def _get(self, name: str) -> ActuatorState:
        if name not in self.actuators:
            raise KeyError(name)
        return self.actuators[name]

    def _interlock(self, name: str, proposed: str | None) -> tuple[str | None, str | None]:
        """Returns (state, reason). reason is set only when an interlock changed the proposal."""
        for rule in self.interlocks:
            if rule["actuator"] != name:
                continue
            value = self.latest_values.get(rule["sensor"])
            if value is None:
                continue
            below = "min" in rule and value < rule["min"]
            above = "max" in rule and value > rule["max"]
            if (below or above) and proposed != rule["force"]:
                return rule["force"], f"interlock: {rule['sensor']}={value:g} {'<' if below else '>'} " \
                                      f"{rule['min'] if below else rule['max']}"
        return proposed, None

    def _dwell_ok(self, st: ActuatorState) -> bool:
        if st.last_change_sim is None or self.sim_ts is None:
            return True
        elapsed = _minutes_between(st.last_change_sim, self.sim_ts)
        return elapsed < 0 or elapsed >= self.min_dwell   # negative = dataset looped around

    def _set_desired(self, st: ActuatorState, target: str, source: str, reason: str) -> None:
        st.desired, st.reason = target, reason
        if target != st.last_sent:
            self._send(st, target, source, reason)

    def _send(self, st: ActuatorState, state: str, source: str, reason: str) -> None:
        cmd = {"cmd_id": uuid.uuid4().hex[:12], "actuator": st.name, "state": state, "source": source,
               "reason": reason, "ts": self._clock()}
        if st.last_sent != state:
            st.last_change_sim = self.sim_ts
        st.last_sent, st.last_sent_at = state, self._clock()
        if self._db:
            self._db.insert_command({"cmd_id": cmd["cmd_id"], "sent_at": cmd["ts"], "sim_ts": self.sim_ts,
                                     "actuator": st.name, "state": state, "source": source, "reason": reason})
        log.info("cmd %s: %s -> %s (%s: %s)", cmd["cmd_id"], st.name, state, source, reason)
        self._publish(st.name, cmd)
        self._emit("command", {**cmd, "sim_ts": self.sim_ts})

    def _view(self, st: ActuatorState) -> dict:
        return {"name": st.name, "mode": st.mode, "desired": st.desired, "confirmed": st.confirmed,
                "reason": st.reason, "override_until": st.override_until}

    def snapshot(self) -> dict:
        return {name: self._view(st) for name, st in self.actuators.items()}
