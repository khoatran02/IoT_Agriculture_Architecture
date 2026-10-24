import pytest
from fastapi.testclient import TestClient

from iot_agri.backend.app import create_app
from iot_agri.backend.pipeline import Pipeline
from conftest import make_sample
from fakes import MemoryDatabase


class OfflineMQTT:
    """Only the transport is replaced; inference, decision, API and DB are real."""
    def __init__(self, *args, **kwargs):
        self.sent = []

    def subscribe(self, *args):
        pass

    def monitor(self, *args):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def publish(self, topic, payload, **kwargs):
        self.sent.append((topic, payload))


@pytest.fixture(autouse=True)
def transport(monkeypatch):
    monkeypatch.setattr("iot_agri.backend.pipeline.MqttNode", OfflineMQTT)
    monkeypatch.setattr("iot_agri.backend.app.Database", MemoryDatabase)   # no InfluxDB server needed here


def feed(pipeline, sample):
    for name, value in sample.values.items():
        pipeline._on_sensor_msg("", {"sensor": name, "value": value, "seq": sample.seq,
                                      "run_id": "integration", "ts": sample.ts, "gw": sample.gateway})


def test_api_and_websocket_expose_real_mlp_prediction(cfg):
    with TestClient(create_app(cfg)) as client:
        with client.websocket_connect("/ws") as ws:
            assert ws.receive_json()["data"]["model"] == "mlp_v1-int8"
            p = client.app.state.pipeline
            feed(p, make_sample(temperature=45, humidity=30, water_level=50))
            while (event := ws.receive_json())["type"] != "prediction":
                pass
            assert event["data"]["details"]["out_of_range_features"] == ["temperature"]
        state = client.get("/api/state").json()
        pred = state["prediction"]
        assert pred["model"] == "mlp_v1-int8" and len(pred["scores"]) == 3
        assert pred["details"]["variant"] == "int8"
        assert len(client.get("/api/history").json()) == 1
        assert len(client.get("/api/commands").json()) == 3
        assert client.post("/api/override", json={"actuator": "fan", "state": "OFF"}).status_code == 200
        feed(p, make_sample(seq=1, water_level=3))
        assert client.post("/api/override", json={"actuator": "irrigation_pump", "state": "ON"}).status_code == 409
        assert client.delete("/api/override/fan").status_code == 200
        sample_ref, sim_ts, stored = client.app.state.db.predictions[0]     # what the platform asked the DB to store
        assert stored.model == "mlp_v1-int8" and stored.scores == pred["scores"] and stored.details == pred["details"]
        assert sim_ts == pred["ts"] and sample_ref > 0


@pytest.mark.parametrize("ttl", [0, -1, "NaN", "Infinity"])
def test_invalid_override_ttl_is_rejected(cfg, ttl):
    with TestClient(create_app(cfg)) as client:
        assert client.post("/api/override", json={"actuator": "fan", "state": "ON", "ttl_s": ttl}).status_code == 422


def test_inference_failure_does_not_bypass_interlock(cfg):
    db = MemoryDatabase()
    events = []
    p = Pipeline(cfg, db, emit=lambda *event: events.append(event))
    try:
        feed(p, make_sample(water_level=50))
        p.override("irrigation_pump", "ON", None)
        p.inference.model.predict = lambda _: (_ for _ in ()).throw(RuntimeError("failed"))
        feed(p, make_sample(seq=1, water_level=3))
        st = p.snapshot()["actuators"]["irrigation_pump"]
        assert st["desired"] == "OFF" and st["mode"] == "auto"
        assert p.snapshot()["prediction"] is None
        assert [e for e in events if e[0] == "prediction"][-1] == ("prediction", None)
        cycle = events[-1][1]
        assert events[-1][0] == "cycle" and cycle["prediction"] is None
        trace = {d["actuator"]: d for d in cycle["decisions"]}["irrigation_pump"]
        assert trace["action"] == "interlock" and trace["cmd_id"] == p.node.sent[-1][1]["cmd_id"]
        assert p.node.sent[-1][1]["state"] == "OFF"
    finally:
        p.stop()
