"""Opt-in test against an isolated MQTT broker: AGRI_TEST_MQTT_PORT=1883 python -m pytest -q."""
import os
import time
import uuid

import pytest
from fastapi.testclient import TestClient

from iot_agri import topics
from iot_agri.backend.app import create_app
from iot_agri.edge.controller import ActuatorController
from iot_agri.edge.gateway import Gateway
from conftest import make_sample
from fakes import MemoryDatabase

pytestmark = pytest.mark.skipif(not os.environ.get("AGRI_TEST_MQTT_PORT"), reason="requires isolated MQTT broker")


def eventually(check, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(.02)
    assert check(), "MQTT pipeline did not reach the expected state"


def test_gateway_mlp_commands_controller_ack_and_rest(cfg, monkeypatch):
    cfg["mqtt"].update(host="127.0.0.1", port=int(os.environ["AGRI_TEST_MQTT_PORT"]))
    cfg["site"] = f"test-{uuid.uuid4().hex[:8]}"
    monkeypatch.setattr("iot_agri.backend.app.Database", MemoryDatabase)   # this test is about MQTT, not storage
    cfg["controller"]["actuation_delay_s"] = 0
    ctrl = ActuatorController(cfg)
    with TestClient(create_app(cfg)) as client:
        pipeline = client.app.state.pipeline
        ctrl.node.start()
        try:
            eventually(lambda: pipeline.node.client.is_connected() and ctrl.node.client.is_connected())
            eventually(lambda: len(pipeline.nodes) >= 2)
            gw = Gateway(cfg, interval_s=.03, loop=False, max_samples=25)
            assert gw.run() == 25
            eventually(lambda: len(client.get("/api/history").json()) == 25)
            eventually(lambda: all(c["acked_at"] is not None for c in client.get("/api/commands").json()))
            state = client.get("/api/state").json()
            assert state["prediction"]["model"] == "mlp_v1-int8"
            assert state["sample"]["seq"] == 24
            assert len(client.get("/api/commands").json()) >= 3
            assert all(a["desired"] == a["confirmed"] for a in state["actuators"].values())
            # The wire tap (its own MQTT client) saw the same traffic, and ACKs carry ordered hop timestamps.
            eventually(lambda: {"telemetry", "command", "status"} <= {m["kind"] for m in client.get("/api/mqtt").json()})
            ids = [m["id"] for m in client.get("/api/mqtt").json()]
            assert ids == sorted(set(ids))
            acks = state["acks"]
            assert acks and all(a["cmd_ts"] <= a["recv_ts"] <= a["applied_ts"] <= a["ack_at"] for a in acks)
            assert state["relays"]["fan"]["gpio"] == 17 and state["gateway"]["rows"] == len(gw.dataset)
            cycle = client.get("/api/cycles").json()[-1]
            assert cycle["row"] == 24 and cycle["run_id"] == gw.run_id and cycle["times"]["gateway"] <= cycle["times"]["ingested"]
            assert client.post("/api/override", json={"actuator": "irrigation_pump", "state": "ON"}).status_code == 200
            eventually(lambda: ctrl.relays["irrigation_pump"].state == "ON")
            low = make_sample(water_level=3)
            for name, value in low.values.items():
                ctrl.node.publish(topics.sensor(cfg["site"], name), {
                    "sensor": name, "value": value, "seq": 0, "run_id": "low-water",
                    "ts": low.ts, "gw": "test",
                })
            eventually(lambda: ctrl.relays["irrigation_pump"].state == "OFF")
            assert client.post("/api/override", json={"actuator": "irrigation_pump", "state": "ON"}).status_code == 409
            eventually(lambda: all(c["acked_at"] is not None for c in client.get("/api/commands").json()))
        finally:
            ctrl.node.stop()
