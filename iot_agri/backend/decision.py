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
    def on_sample(self, sample) -> dict[str, dict | None]:
        """Apply interlocks without depending on a successful model prediction.

        Returns {actuator: command sent or None} for every interlock that is active.
        """
        self.latest_values, self.sim_ts = sample.values, sample.ts
        forced = {}
        for name, st in self.actuators.items():
            target, reason = self._interlock(name, st.desired)
            if reason:
                if st.mode == "manual":
                    st.mode, st.override_until = "auto", None
                forced[name] = self._set_desired(st, target, "auto", reason)
                self._emit("actuator", self._view(st))
        return forced

    def on_prediction(self, sample, prediction, forced: dict | None = None) -> list[dict]:
        """Returns one decision trace per actuator. Pass `forced` if on_sample already ran for this sample."""
        if forced is None:
            forced = self.on_sample(sample)
        traces = []
        for name, st in self.actuators.items():
            proposed = prediction.outputs.get(name)
            if name in forced:        # an interlock beat the model and any manual override
                traces.append(self._trace(st, proposed, "interlock", forced[name]))
            elif st.mode == "manual":
                traces.append(self._trace(st, proposed, "manual"))
            elif proposed is None:
                traces.append(self._trace(st, None, "no-model"))
            else:
                dwell = self._dwell_info(st)
                if not dwell["ok"] and proposed != st.desired:
                    traces.append(self._trace(st, proposed, "held", dwell=dwell))
                elif not dwell["ok"]:
                    traces.append(self._trace(st, proposed, "unchanged", dwell=dwell))
                else:
                    cmd = self._set_desired(st, proposed, "auto", f"model:{prediction.model}")
                    traces.append(self._trace(st, proposed, "sent" if cmd else "unchanged", cmd, dwell))
        return traces

    def on_no_prediction(self, sample, forced: dict | None = None) -> list[dict]:
        """Model failed: only interlocks act. Returns the same per-actuator traces as on_prediction."""
        if forced is None:
            forced = self.on_sample(sample)
        return [self._trace(st, None, "interlock" if name in forced else "no-model", forced.get(name))
                for name, st in self.actuators.items()]

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
        if reason and target != state:
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
        """Return the forced state and reason whenever an interlock is active."""
        for rule in self.interlocks:
            if rule["actuator"] != name:
                continue
            value = self.latest_values.get(rule["sensor"])
            if value is None:
                continue
            below = "min" in rule and value < rule["min"]
            above = "max" in rule and value > rule["max"]
            if below or above:
                return rule["force"], f"interlock: {rule['sensor']}={value:g} {'<' if below else '>'} " \
                                      f"{rule['min'] if below else rule['max']}"
        return proposed, None

    def _dwell_info(self, st: ActuatorState) -> dict:
        if st.last_change_sim is None or self.sim_ts is None:
            return {"ok": True, "elapsed_min": None, "min": self.min_dwell}
        elapsed = _minutes_between(st.last_change_sim, self.sim_ts)
        # negative elapsed = the dataset looped around
        return {"ok": elapsed < 0 or elapsed >= self.min_dwell, "elapsed_min": elapsed, "min": self.min_dwell}

    def _dwell_ok(self, st: ActuatorState) -> bool:
        return self._dwell_info(st)["ok"]

    def _set_desired(self, st: ActuatorState, target: str, source: str, reason: str) -> dict | None:
        """Returns the command it sent, or None if the actuator was already commanded to `target`."""
        st.desired, st.reason = target, reason
        if target != st.last_sent:
            return self._send(st, target, source, reason)
        return None

    def interlock_view(self, name: str) -> list[dict]:
        """Every interlock guarding `name` with the value it is checking now, tripped or not."""
        out = []
        for rule in self.interlocks:
            if rule["actuator"] != name:
                continue
            value = self.latest_values.get(rule["sensor"])
            tripped = value is not None and (("min" in rule and value < rule["min"])
                                             or ("max" in rule and value > rule["max"]))
            bounds = []
            if "min" in rule:
                bounds.append(f"{rule['sensor']} >= {rule['min']:g}")
            if "max" in rule:
                bounds.append(f"{rule['sensor']} <= {rule['max']:g}")
            out.append({"rule": " and ".join(bounds), "force": rule["force"], "sensor": rule["sensor"],
                        "value": value, "tripped": bool(tripped), "note": rule.get("note", "")})
        return out

    def _trace(self, st: ActuatorState, proposed, action: str, cmd: dict | None = None,
               dwell: dict | None = None) -> dict:
        """What the engine did for one actuator this cycle:
        interlock | manual | held (min dwell) | sent | unchanged | no-model."""
        return {"actuator": st.name, "mode": st.mode, "proposed": proposed, "action": action,
                "interlocks": self.interlock_view(st.name), "dwell": dwell, "desired": st.desired,
                "reason": st.reason, "cmd_id": cmd["cmd_id"] if cmd else None}

    def describe(self) -> dict:
        return {"priority": ["interlock", "manual override", "model"], "min_dwell_minutes": self.min_dwell,
                "ack_timeout_s": self.ack_timeout, "override_ttl_s": self.override_ttl,
                "interlocks": [dict(r) for r in self.interlocks]}

    def _send(self, st: ActuatorState, state: str, source: str, reason: str) -> dict:
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
        return cmd

    def _view(self, st: ActuatorState) -> dict:
        return {"name": st.name, "mode": st.mode, "desired": st.desired, "confirmed": st.confirmed,
                "reason": st.reason, "override_until": st.override_until}

    def snapshot(self) -> dict:
        return {name: self._view(st) for name, st in self.actuators.items()}
