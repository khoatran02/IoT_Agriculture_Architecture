import pytest

from iot_agri.backend.decision import DecisionEngine, OverrideBlocked
from iot_agri.backend.inference import Prediction
from conftest import make_sample


class Harness:
    def __init__(self, cfg):
        self.now = 1000.0
        self.sent, self.events = [], []
        self.engine = DecisionEngine(cfg, lambda a, c: self.sent.append(c), lambda t, d: self.events.append((t, d)),
                                     clock=lambda: self.now)

    def predict(self, minute, **outputs):
        sample = make_sample(ts=f"2024-02-08T06:{minute:02d}:00", **outputs.pop("sensors", {}))
        full = {"fan": "OFF", "irrigation_pump": "OFF", "water_pump": "OFF"} | outputs
        self.engine.on_prediction(sample, Prediction("t", full))

    def cmds(self, actuator):
        return [(c["state"], c["source"]) for c in self.sent if c["actuator"] == actuator]


@pytest.fixture
def h(cfg):
    return Harness(cfg)


def test_first_prediction_sends_initial_commands_and_repeats_are_suppressed(h):
    h.predict(0, fan="ON")
    assert len(h.sent) == 3 and h.cmds("fan") == [("ON", "auto")]
    h.predict(5, fan="ON")
    assert len(h.sent) == 3


def test_min_dwell_blocks_fast_flip_flop_then_allows(h):
    h.predict(0, fan="ON")
    h.predict(5, fan="OFF")          # only 5 simulated minutes since the ON
    assert h.cmds("fan") == [("ON", "auto")]
    h.predict(10, fan="OFF")
    assert h.cmds("fan") == [("ON", "auto"), ("OFF", "auto")]


def test_dwell_does_not_block_after_dataset_wraps_around(h):
    h.predict(30, fan="ON")
    h.engine.on_prediction(make_sample(ts="2024-02-08T06:00:00"), Prediction("t", {"fan": "OFF"}))
    assert h.cmds("fan")[-1] == ("OFF", "auto")


def test_interlock_beats_model(h):
    h.predict(0, irrigation_pump="ON", sensors={"water_level": 3})
    assert h.cmds("irrigation_pump") == [("OFF", "auto")]
    assert h.engine.actuators["irrigation_pump"].reason.startswith("interlock")


def test_interlock_bypasses_dwell(h):
    h.predict(0, irrigation_pump="ON")
    h.predict(5, irrigation_pump="ON", sensors={"water_level": 3})
    assert h.cmds("irrigation_pump") == [("ON", "auto"), ("OFF", "auto")]


def test_manual_override_wins_over_model_until_released(h):
    h.predict(0, fan="OFF")
    h.engine.override("fan", "ON")
    h.predict(15, fan="OFF")
    assert h.cmds("fan") == [("OFF", "auto"), ("ON", "manual")]
    h.engine.release("fan")
    h.predict(30, fan="OFF")
    assert h.cmds("fan")[-1] == ("OFF", "auto")


def test_override_expires_by_ttl(h):
    h.predict(0)
    h.engine.override("fan", "ON", ttl_s=60)
    h.now += 61
    h.engine.tick()
    assert h.engine.actuators["fan"].mode == "auto"


def test_override_refused_by_interlock(h):
    h.predict(0, sensors={"water_level": 3})
    with pytest.raises(OverrideBlocked):
        h.engine.override("irrigation_pump", "ON")
    assert h.engine.actuators["irrigation_pump"].mode == "auto"


def test_interlock_cancels_standing_override_when_it_trips(h):
    h.predict(0)
    h.engine.override("irrigation_pump", "ON")
    h.predict(5, sensors={"water_level": 3})
    st = h.engine.actuators["irrigation_pump"]
    assert st.mode == "auto" and h.cmds("irrigation_pump")[-1] == ("OFF", "auto")


def test_unacked_command_is_resent_then_ack_stops_it(h):
    h.predict(0, fan="ON")
    h.now += 6
    h.engine.tick()
    assert [c["source"] for c in h.sent if c["actuator"] == "fan"] == ["auto", "retry"]
    h.engine.on_status("fan", "ON", h.sent[-1]["cmd_id"])
    h.now += 6
    h.engine.tick()
    assert len([c for c in h.sent if c["actuator"] == "fan"]) == 2


def test_unknown_actuator(h):
    with pytest.raises(KeyError):
        h.engine.override("sprinkler", "ON")


def test_interlock_bypasses_dwell_even_when_model_also_requests_off(h):
    h.predict(0, irrigation_pump="ON")
    h.predict(5, irrigation_pump="OFF", sensors={"water_level": 3})
    assert h.cmds("irrigation_pump")[-1] == ("OFF", "auto")


def test_interlock_trips_without_a_prediction(h):
    h.predict(0, irrigation_pump="ON")
    h.engine.override("irrigation_pump", "ON")
    h.engine.on_sample(make_sample(water_level=3))
    assert h.cmds("irrigation_pump")[-1] == ("OFF", "auto")
    assert h.engine.actuators["irrigation_pump"].mode == "auto"


def test_manual_off_allowed_while_dry_run_interlock_active(h):
    h.predict(0, sensors={"water_level": 3})
    assert h.engine.override("irrigation_pump", "OFF")["desired"] == "OFF"
