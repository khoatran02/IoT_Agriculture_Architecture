"""The step-by-step view: ingestion trace info, decision traces, MQTT tap, gateway playback/faults, ACK timing."""
import json

import pytest
from fastapi.testclient import TestClient

from iot_agri import topics
from iot_agri.backend.app import create_app
from iot_agri.backend.decision import DecisionEngine
from iot_agri.backend.inference import Prediction
from iot_agri.backend.ingestion import Ingestion
from iot_agri.backend.tap import MqttTap, classify
from iot_agri.edge.controller import ActuatorController
from iot_agri.edge.gateway import Gateway
from conftest import make_sample
from test_pipeline import OfflineMQTT, feed, transport  # noqa: F401  (transport: autouse fake MQTT)


class FakeNode:
    def __init__(self):
        self.sent, self.monitors = [], []

    def publish(self, topic, payload, **kwargs):
        self.sent.append((topic, payload, kwargs))

    def monitor(self, topic_filter, handler, qos=0):
        self.monitors.append((topic_filter, handler))


# --- ingestion --------------------------------------------------------------------------------------
def test_sample_keeps_how_it_was_assembled():
    out, now = [], [100.0]
    ing = Ingestion([{"name": "t", "valid_range": [0, 50]}, {"name": "h", "valid_range": [0, 100]}],
                    out.append, clock=lambda: now[0])
    base = dict(run_id="r", ts="2024-01-01T00:00:00", gw="gw")
    for sensor, value in (("t", 20), ("h", 55)):            # a good cycle first, so there is a value to fall back on
        ing.on_reading({**base, "seq": 6, "sensor": sensor, "value": value})
    msg = {**base, "seq": 7, "gw_ts": 99.5, "row": 42, "rows": 1000}
    ing.on_reading({**msg, "sensor": "t", "value": 21})
    now[0] = 100.25
    ing.on_reading({**msg, "sensor": "h", "value": 999})    # out of range
    info = out[1].info
    assert (info["row"], info["rows"], info["run_id"], info["gw_ts"]) == (42, 1000, "r", 99.5)
    assert info["raw"] == {"t": 21, "h": 999}                # what arrived ...
    assert out[1].values == {"t": 21, "h": 55} and out[1].quality["h"] == "imputed_range"   # ... vs what was used
    assert (info["first_recv"], info["last_recv"]) == (100.0, 100.25)
    assert info["recv"]["h"] == {"t": 100.25, "gw_ts": 99.5} and info["stats"]["samples"] == 2


# --- decision traces --------------------------------------------------------------------------------
@pytest.fixture
def engine(cfg):
    sent = []
    return DecisionEngine(cfg, lambda a, c: sent.append(c), clock=lambda: 1000.0), sent


def predict(engine, minute, **outputs):
    sample = make_sample(ts=f"2024-02-08T06:{minute:02d}:00", **outputs.pop("sensors", {}))
    full = {"fan": "OFF", "irrigation_pump": "OFF", "water_pump": "OFF"} | outputs
    return {t["actuator"]: t for t in engine.on_prediction(sample, Prediction("m", full))}


def test_trace_reports_sent_then_unchanged_then_held(engine):
    eng, sent = engine
    t = predict(eng, 0, fan="ON")
    assert t["fan"]["action"] == "sent" and t["fan"]["cmd_id"] == sent[0]["cmd_id"] and t["fan"]["proposed"] == "ON"
    assert predict(eng, 5, fan="ON")["fan"]["action"] == "unchanged"
    held = predict(eng, 7, fan="OFF")["fan"]            # 7 min after the ON, min dwell is 10
    assert held["action"] == "held" and held["cmd_id"] is None
    assert held["dwell"]["ok"] is False and held["dwell"]["min"] == 10 and held["dwell"]["elapsed_min"] == 7


def test_trace_shows_interlock_with_value_checked(engine):
    eng, sent = engine
    t = predict(eng, 0, irrigation_pump="ON", sensors={"water_level": 3})["irrigation_pump"]
    pump_cmds = [c for c in sent if c["actuator"] == "irrigation_pump"]
    assert t["action"] == "interlock" and t["cmd_id"] == pump_cmds[0]["cmd_id"] and t["proposed"] == "ON"
    assert pump_cmds[0]["state"] == "OFF" and pump_cmds[0]["reason"].startswith("interlock")
    rule = t["interlocks"][0]
    assert rule["tripped"] and rule["value"] == 3 and rule["force"] == "OFF" and rule["rule"] == "water_level >= 10"


def test_trace_lists_untripped_interlocks_and_manual_mode(engine):
    eng, _ = engine
    predict(eng, 0)
    eng.override("irrigation_pump", "ON")
    t = predict(eng, 5, irrigation_pump="OFF")["irrigation_pump"]
    assert t["action"] == "manual" and t["interlocks"][0]["tripped"] is False and t["interlocks"][0]["value"] == 100
    assert predict(eng, 5)["fan"]["interlocks"] == []        # no interlock guards the fan


def test_no_prediction_trace_marks_interlocks_only(engine):
    eng, sent = engine
    traces = {t["actuator"]: t for t in eng.on_no_prediction(make_sample(water_level=3))}
    assert traces["irrigation_pump"]["action"] == "interlock" and traces["fan"]["action"] == "no-model"
    assert traces["irrigation_pump"]["cmd_id"] == sent[0]["cmd_id"]


def test_passing_forced_avoids_losing_the_command_id(engine):
    eng, sent = engine
    sample = make_sample(water_level=3)
    forced = eng.on_sample(sample)
    full = Prediction("m", {"fan": "OFF", "irrigation_pump": "ON", "water_pump": "OFF"})
    t = {x["actuator"]: x for x in eng.on_prediction(sample, full, forced)}["irrigation_pump"]
    pump_cmds = [c for c in sent if c["actuator"] == "irrigation_pump"]
    assert t["cmd_id"] == forced["irrigation_pump"]["cmd_id"] and len(pump_cmds) == 1   # not sent twice


# --- MQTT tap ---------------------------------------------------------------------------------------
@pytest.mark.parametrize("topic,expected", [
    ("agri/f/sensor/humidity", ("telemetry", "gateway")),
    ("agri/f/actuator/fan/cmd", ("command", "rule engine")),
    ("agri/f/actuator/fan/status", ("status", "controller")),
    ("agri/f/node/gw-01/availability", ("availability", "gw-01")),
    ("agri/f/node/gw-01/state", ("gateway-state", "gw-01")),
    ("agri/f/node/gw-01/control", ("control", "dashboard")),
    ("agri/f/whatever", ("other", "?")),
])
def test_tap_classifies_topics(topic, expected):
    assert classify(topic) == expected


def test_tap_records_latency_and_survives_non_json():
    node, events, now = FakeNode(), [], [1_700_000_010.0]
    tap = MqttTap(node, "f", lambda t, d: events.append((t, d)), keep=1, clock=lambda: now[0])
    assert node.monitors[0][0] == "agri/f/#"
    on_message = node.monitors[0][1]
    on_message("agri/f/sensor/N", json.dumps({"gw_ts": 1_700_000_009.9, "ts": "2024-01-01T00:00:00"}).encode(), 1, False)
    on_message("agri/f/actuator/fan/status", json.dumps({"ts": 1_700_000_009.0}).encode(), 1, True)    # retained: no latency
    on_message("agri/f/sensor/N", b"\xffnot json", 0, False)
    assert [e[0] for e in events] == ["mqtt"] * 3
    assert tap.stats() == {"total": 3, "by_kind": {"telemetry": 2, "status": 1}}
    # keep=1 limits telemetry only: the first telemetry record fell out, the status record is untouched
    assert [r["id"] for r in tap.recent()] == [2, 3]
    assert events[0][1]["latency_ms"] == 100.0 and events[0][1]["from"] == "gateway"
    assert events[1][1]["latency_ms"] is None and events[1][1]["retain"] is True   # retained: age is meaningless
    assert isinstance(events[2][1]["payload"], str) and events[2][1]["latency_ms"] is None
    assert [r["id"] for r in tap.recent(1)] == [2, 3]          # limit is per kind


# --- gateway playback and faults --------------------------------------------------------------------
@pytest.fixture
def gateway(cfg):
    gw = Gateway(cfg, interval_s=1)
    gw.node = FakeNode()
    return gw


def sensor_msgs(gw):
    return {t.rsplit("/", 1)[1]: p for t, p, _ in gw.node.sent if "/sensor/" in t}


def test_gateway_telemetry_carries_dataset_row(gateway):
    gateway._i = 5
    gateway._publish_cycle()
    msgs = sensor_msgs(gateway)
    assert len(msgs) == 6 and {m["row"] for m in msgs.values()} == {5} and msgs["N"]["rows"] == len(gateway.dataset)


def test_gateway_pause_resume_step_interval(gateway):
    c = gateway._on_control
    c("", {"action": "pause"})
    assert gateway._paused and gateway.node.sent[-1][0].endswith("/state") and gateway.node.sent[-1][2]["retain"]
    c("", {"action": "step"})
    assert gateway._step.is_set() and gateway._paused
    c("", {"action": "resume"})
    assert not gateway._paused
    c("", {"action": "interval", "interval_s": 0.5})
    assert gateway.interval_s == 0.5
    for bad in ({"action": "interval", "interval_s": 0}, {"action": "interval", "interval_s": "fast"},
                {"action": "nope"}, {"action": "fault", "sensor": "ph", "kind": "spike"},
                {"action": "fault", "sensor": "N", "kind": "melt"}, {"action": "fault", "sensor": "N",
                                                                     "kind": "spike", "cycles": 0}):
        before = len(gateway.node.sent)
        c("", bad)
        assert len(gateway.node.sent) == before and gateway.interval_s == 0.5 and not gateway._faults


def test_dropout_fault_silences_the_sensor_then_expires(gateway):
    gateway._on_control("", {"action": "fault", "sensor": "humidity", "kind": "dropout", "cycles": 2})
    state = json.loads(json.dumps(gateway.node.sent[-1][1]))
    assert state["faults"] == {"humidity": {"kind": "dropout", "cycles": 2}}
    for expected_humidity_msgs in (0, 0, 1):
        gateway.node.sent.clear()
        gateway._publish_cycle()
        gateway._seq += 1
        assert ("humidity" in sensor_msgs(gateway)) == bool(expected_humidity_msgs)
        assert len(sensor_msgs(gateway)) == 5 + expected_humidity_msgs
    assert not gateway._faults


def test_spike_fault_publishes_out_of_range_value(gateway, cfg):
    gateway._on_control("", {"action": "fault", "sensor": "temperature", "kind": "spike", "cycles": 1})
    gateway.node.sent.clear()
    gateway._publish_cycle()
    hi = next(s["valid_range"][1] for s in cfg["sensors"] if s["name"] == "temperature")
    assert sensor_msgs(gateway)["temperature"]["value"] > hi
    gateway._on_control("", {"action": "fault", "sensor": "N", "kind": "dropout"})
    gateway._on_control("", {"action": "clear_faults"})
    assert not gateway._faults


def test_gateway_loop_pauses_and_steps_exactly_one_cycle(cfg):
    import threading
    import time
    gw = Gateway(cfg, interval_s=0.01, loop=False, max_samples=3)
    gw.node = FakeNode()
    gw.node.start = gw.node.stop = lambda: None
    gw._paused = True
    runner = threading.Thread(target=gw.run)
    runner.start()
    time.sleep(0.4)
    assert gw._seq == 0 and not sensor_msgs(gw)       # paused: nothing published
    gw._on_control("", {"action": "step"})
    deadline = time.monotonic() + 3
    while gw._seq < 1 and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(0.4)
    assert gw._seq == 1 and gw._paused                # exactly one cycle, then paused again
    gw._on_control("", {"action": "resume"})
    runner.join(5)
    assert not runner.is_alive() and gw._seq == 3


# --- controller ACK timing --------------------------------------------------------------------------
def test_controller_ack_echoes_command_time_and_reports_gpio(cfg):
    cfg["controller"]["actuation_delay_s"] = 0
    ctrl = ActuatorController(cfg)
    ctrl.node = FakeNode()
    ctrl._on_command(topics.actuator_cmd(cfg["site"], "fan"), {"cmd_id": "abc", "state": "ON", "ts": 123.5})
    _, ack, kwargs = ctrl.node.sent[-1]
    assert kwargs["retain"] and ack["cmd_id"] == "abc" and ack["state"] == "ON" and ack["cmd_ts"] == 123.5
    assert ack["recv_ts"] <= ack["applied_ts"] and ack["level"] == "HIGH" and ack["gpio"] == 17
    ctrl._report_all()
    assert ctrl.node.sent[-1][1]["cmd_id"] is None and "applied_ts" not in ctrl.node.sent[-1][1]
    ctrl._on_command(topics.actuator_cmd(cfg["site"], "fan"), {"cmd_id": "bad", "state": "MAYBE", "ts": 1.0})
    assert ctrl.node.sent[-1][1]["ok"] is False and ctrl.node.sent[-1][1]["cmd_ts"] == 1.0


# --- pipeline + API ---------------------------------------------------------------------------------
def test_cycle_trace_ack_timing_and_gateway_control_through_the_api(cfg):
    with TestClient(create_app(cfg)) as client:
        p = client.app.state.pipeline
        feed(p, make_sample(temperature=30, humidity=30, water_level=50))
        cycle = client.get("/api/cycles").json()[-1]
        assert [s["name"] for s in cycle["sensors"]] == ["temperature", "humidity", "water_level", "N", "P", "K"]
        assert cycle["sensors"][0]["raw"] == 30 and cycle["sensors"][0]["recv_t"] is not None
        assert cycle["db"]["sample_at"] > 0 and cycle["db"]["prediction_at"] > cycle["db"]["sample_at"]   # point write times
        assert cycle["prediction"]["model"] == "mlp_v1-int8"
        t = cycle["times"]
        assert t["first_recv"] <= t["last_recv"] <= t["ingested"] <= t["infer_start"] <= t["infer_end"] <= t["decided"]
        sent = [pl for topic, pl in p.node.sent if topic.endswith("/cmd")]
        assert len(cycle["decisions"]) == 3 and len(sent) == 3 and set(cycle["commands"]) == {c["cmd_id"] for c in sent}
        cmd = sent[0]

        topic = topics.actuator_status(cfg["site"], cmd["actuator"])
        p._on_status_msg(topic, {"actuator": cmd["actuator"], "state": cmd["state"], "gpio": 17, "level": "HIGH",
                                 "cmd_id": cmd["cmd_id"], "ok": True, "cmd_ts": cmd["ts"],
                                 "recv_ts": cmd["ts"] + .01, "applied_ts": cmd["ts"] + .21})
        snap = client.get("/api/state").json()
        assert snap["acks"][-1]["cmd_id"] == cmd["cmd_id"] and snap["acks"][-1]["recv_ts"] == cmd["ts"] + .01
        assert snap["acks"][-1]["replayed"] is False                  # a live ACK arrives within moments

        import time
        p._on_status_msg(topic, {"actuator": cmd["actuator"], "state": cmd["state"], "cmd_id": "old", "ok": True,
                                 "cmd_ts": 1.0, "recv_ts": 1.1, "applied_ts": time.time() - 60})
        assert client.get("/api/state").json()["acks"][-1]["replayed"] is True   # retained status after a restart
        assert snap["relays"][cmd["actuator"]]["level"] == "HIGH" and snap["db"]["samples"] == 1
        assert snap["mqtt"] == {"total": 0, "by_kind": {}} and snap["rules"]["min_dwell_minutes"] == 10
        assert snap["ws_clients"] == 0 and snap["sensors"][1]["valid_range"] == [0, 100]

        p._on_state_msg(topics.gateway_state(cfg["site"], "gw-01"), {"paused": True, "row": 3})
        p._on_state_msg(topics.gateway_state(cfg["site"], "someone-else"), {"paused": False})
        assert client.get("/api/state").json()["gateway"] == {"paused": True, "row": 3}

        assert client.post("/api/gateway/control", json={"action": "pause"}).json() == {"action": "pause"}
        assert p.node.sent[-1][0] == topics.gateway_control(cfg["site"], "gw-01")
        r = client.post("/api/gateway/control", json={"action": "fault", "sensor": "humidity", "kind": "spike",
                                                       "cycles": 4})
        assert r.json() == {"action": "fault", "sensor": "humidity", "kind": "spike", "cycles": 4}
        for bad in ({"action": "fault", "sensor": "ph", "kind": "spike"}, {"action": "fault", "sensor": "N"},
                    {"action": "interval"}):
            assert client.post("/api/gateway/control", json=bad).status_code == 422
        for bad in ({"action": "interval", "interval_s": 0.001}, {"action": "explode"},
                    {"action": "fault", "sensor": "N", "kind": "spike", "cycles": 0}):
            assert client.post("/api/gateway/control", json=bad).status_code == 422
        assert client.get("/api/mqtt").json() == []


def test_telemetry_flood_cannot_evict_commands_from_the_tap():
    node = FakeNode()
    tap = MqttTap(node, "f", keep=5)
    on_message = node.monitors[0][1]
    on_message("agri/f/actuator/fan/cmd", json.dumps({"cmd_id": "c1"}).encode(), 1, False)
    on_message("agri/f/node/gw-01/state", json.dumps({"paused": False}).encode(), 1, True)
    for i in range(50):
        on_message("agri/f/sensor/N", json.dumps({"seq": i}).encode(), 1, False)
    kinds = [r["kind"] for r in tap.recent()]
    assert kinds.count("telemetry") == 5 and "command" in kinds and "gateway-state" in kinds
    assert [r["id"] for r in tap.recent()] == sorted(r["id"] for r in tap.recent())
