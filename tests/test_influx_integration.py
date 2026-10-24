"""Opt-in tests against a real InfluxDB 2.x:

    docker compose up -d --wait influxdb
    AGRI_TEST_INFLUX_URL=http://localhost:8086 python -m pytest -q tests/test_influx_integration.py

Each test uses its own throwaway bucket (created by the platform itself, deleted afterwards); nothing else is touched.
Set AGRI_TEST_INFLUX_ORG / AGRI_TEST_INFLUX_TOKEN if you changed the demo credentials.
"""
import os
import time
import uuid
from types import SimpleNamespace as NS

import pytest
from fastapi.testclient import TestClient
from influxdb_client import InfluxDBClient

from iot_agri import topics
from iot_agri.backend.app import create_app
from iot_agri.backend.db import Database
from iot_agri.backend.inference import Prediction
from conftest import make_sample
from test_pipeline import OfflineMQTT, feed

pytestmark = pytest.mark.skipif(not os.environ.get("AGRI_TEST_INFLUX_URL"), reason="requires an InfluxDB server")


def eventually(check, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except Exception:
            pass
        time.sleep(0.05)
    assert check(), "InfluxDB did not reach the expected state"


@pytest.fixture
def influx_cfg():
    cfg = {"url": os.environ["AGRI_TEST_INFLUX_URL"], "org": os.environ.get("AGRI_TEST_INFLUX_ORG", "my-org"),
           "token": os.environ.get("AGRI_TEST_INFLUX_TOKEN", "my-token"), "bucket": f"test-{uuid.uuid4().hex[:10]}"}
    yield cfg                      # the platform creates the bucket itself; that is part of what is tested
    with InfluxDBClient(url=cfg["url"], token=cfg["token"], org=cfg["org"]) as client:
        bucket = client.buckets_api().find_bucket_by_name(cfg["bucket"])
        if bucket:
            client.buckets_api().delete_bucket(bucket)


def make_row(seq, sim_ts="2023-11-27T06:26:00"):
    return NS(seq=seq, ts=sim_ts, gateway="gw-01", info={"run_id": "r1"},
              values={"temperature": 21, "humidity": 60.5, "water_level": 100, "N": 255, "P": 255, "K": 255},
              quality={"temperature": "ok"})


def test_round_trip_ack_join_and_counts_survive_restart(influx_cfg):
    db = Database(influx_cfg)                                       # creates the missing bucket
    assert db.ready and db.counts() == {"samples": 0, "predictions": 0, "commands": 0, "acks": 0, "actuator_status": 0}
    refs = [db.insert_sample(make_row(i)) for i in range(3)]        # three samples with the SAME dataset timestamp
    db.insert_prediction(refs[2], "2023-11-27T06:26:00", Prediction(
        "mlp", {"fan": "ON"}, {"fan": 0.9}, {"inference_ms": 0.4, "out_of_range_features": []}))
    now = time.time()
    db.insert_command({"cmd_id": "c1", "sent_at": now, "sim_ts": "2023-11-27T06:26:00", "actuator": "fan",
                       "state": "ON", "source": "auto", "reason": "model"})
    db.insert_command({"cmd_id": "c2", "sent_at": now + 0.1, "sim_ts": None, "actuator": "fan", "state": "OFF",
                       "source": "manual", "reason": "dashboard override"})
    db.mark_ack("c1", now + 0.25, "ON")
    db.mark_ack("c1", now + 30, "ON")                               # a later, replayed ACK must not win
    db.insert_status(now, "fan", "ON", "c1")
    eventually(lambda: len(db.recent_samples(10)) == 3 and any(c["acked_at"] for c in db.recent_commands(10)))

    samples = db.recent_samples(10)
    assert [s["seq"] for s in samples] == [0, 1, 2]                 # none overwritten, oldest first
    assert samples[0]["ts"] == "2023-11-27T06:26:00" and samples[0]["gateway"] == "gw-01"
    assert samples[0]["values"] == {"temperature": 21.0, "humidity": 60.5, "water_level": 100.0,
                                    "N": 255.0, "P": 255.0, "K": 255.0}
    assert samples[0]["quality"] == {"temperature": "ok"}
    assert [s["seq"] for s in db.recent_samples(2)] == [1, 2]       # limit keeps the newest

    c2, c1 = db.recent_commands(10)                                 # newest first
    assert (c1["cmd_id"], c1["state"], c1["source"], c1["sim_ts"]) == ("c1", "ON", "auto", "2023-11-27T06:26:00")
    assert abs(c1["acked_at"] - (now + 0.25)) < 0.01 and c1["ack_state"] == "ON"
    assert c2["cmd_id"] == "c2" and c2["acked_at"] is None and c2["sim_ts"] is None and c2["source"] == "manual"

    assert db.counts() == {"samples": 3, "predictions": 1, "commands": 2, "acks": 2, "actuator_status": 1}
    db.close()                                                      # flushes anything still queued

    again = Database(influx_cfg)                                    # a fresh process: counts come from the stored data
    assert again.counts() == {"samples": 3, "predictions": 1, "commands": 2, "acks": 2, "actuator_status": 1}
    assert len(again.recent_samples(10)) == 3
    again.close()


def test_a_rejected_write_is_counted_and_never_raises(influx_cfg):
    db = Database(influx_cfg)
    with InfluxDBClient(url=influx_cfg["url"], token=influx_cfg["token"], org=influx_cfg["org"]) as admin:
        admin.buckets_api().delete_bucket(admin.buckets_api().find_bucket_by_name(influx_cfg["bucket"]))
    db.insert_sample(make_row(0))                                   # the bucket is gone: the server answers 404
    eventually(lambda: db.write_errors >= 1)
    assert "404" in db.last_error and db.counts()["samples"] == 0   # the point was not stored, so it is not counted
    assert db.info()["write_errors"] >= 1
    db.close()


def test_platform_stores_history_through_the_api_and_it_survives_a_restart(cfg, influx_cfg, monkeypatch):
    monkeypatch.setattr("iot_agri.backend.pipeline.MqttNode", OfflineMQTT)
    cfg["influxdb"] = influx_cfg
    with TestClient(create_app(cfg)) as client:
        p = client.app.state.pipeline
        for i in range(5):                                          # all five share the default dataset timestamp
            feed(p, make_sample(seq=i, temperature=30, humidity=30, water_level=50))
        sent = [pl for topic, pl in p.node.sent if topic.endswith("/cmd")]
        assert sent
        first = sent[0]
        p._on_status_msg(topics.actuator_status(cfg["site"], first["actuator"]), {
            "actuator": first["actuator"], "state": first["state"], "gpio": 17, "level": "HIGH", "ok": True,
            "cmd_id": first["cmd_id"], "cmd_ts": first["ts"], "recv_ts": first["ts"] + .01,
            "applied_ts": first["ts"] + .2})
        eventually(lambda: len(client.get("/api/history").json()) == 5)
        eventually(lambda: any(c["acked_at"] for c in client.get("/api/commands").json()))
        history = client.get("/api/history").json()
        assert [h["seq"] for h in history] == [0, 1, 2, 3, 4] and history[0]["values"]["temperature"] == 30.0
        commands = {c["cmd_id"]: c for c in client.get("/api/commands").json()}
        assert set(commands) == {c["cmd_id"] for c in sent} and commands[first["cmd_id"]]["acked_at"] is not None
        state = client.get("/api/state").json()
        assert state["db"]["samples"] == 5 and state["db"]["predictions"] == 5 and state["db"]["commands"] == len(sent)
        assert state["db_info"]["connected"] is True and state["db_info"]["bucket"] == influx_cfg["bucket"]
        assert state["db_info"]["write_errors"] == 0

    with TestClient(create_app(cfg)) as restarted:                  # same bucket, brand-new process
        assert restarted.get("/api/state").json()["db"]["samples"] == 5
        assert [h["seq"] for h in restarted.get("/api/history").json()] == [0, 1, 2, 3, 4]
        assert {c["cmd_id"] for c in restarted.get("/api/commands").json()} == {c["cmd_id"] for c in sent}
