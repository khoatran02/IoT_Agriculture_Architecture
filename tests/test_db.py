"""InfluxDB storage layer without a server: what is written, and how it behaves when the database is unavailable."""
import time
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from iot_agri.backend import db as dbmod
from iot_agri.backend.app import create_app
from iot_agri.backend.db import Database, DatabaseUnavailable
from iot_agri.backend.inference import Prediction
from fakes import MemoryDatabase
from test_pipeline import OfflineMQTT


def sample(seq=3, **values):
    v = {"temperature": 21, "humidity": 60.5, "water_level": 100, "N": 255, "P": 255, "K": 255} | values
    return NS(seq=seq, ts="2023-11-27T06:26:00", gateway="gw-01", values=v, quality={"temperature": "ok"},
              info={"run_id": "abc"})


def line(point):
    return point.to_line_protocol()


# --- what gets written ------------------------------------------------------------------------------
def test_sample_point_layout():
    ln = line(dbmod.sample_point(sample(), 1_700_000_000_000_000_123))
    assert ln.startswith("sample,gateway=gw-01 ")
    assert "temperature=21" in ln and "temperature=21i" not in ln       # sensor values are floats, never integers
    assert "water_level=100" in ln and "water_level=100i" not in ln
    assert "seq=3i" in ln and 'sim_ts="2023-11-27T06:26:00"' in ln and 'run_id="abc"' in ln
    assert ln.endswith(" 1700000000000000123")


def test_non_finite_values_are_left_out_not_sent():
    ln = line(dbmod.sample_point(sample(humidity=float("nan"), K=float("inf")), 1))
    assert "humidity=" not in ln and "K=" not in ln and "temperature=21" in ln    # one NaN would reject the batch


def test_prediction_point_layout():
    pred = Prediction("mlp_v1-int8", {"fan": "ON", "water_pump": "OFF"}, {"fan": 0.93, "water_pump": 0.1},
                      {"inference_ms": 0.4, "data_status": "ok", "out_of_range_features": ["temperature"]})
    ln = line(dbmod.prediction_point(123, "2023-11-27T06:26:00", pred, 5))
    assert ln.startswith("prediction,model=mlp_v1-int8 ")
    for part in ("on_fan=1i", "on_water_pump=0i", "score_fan=0.93", "inference_ms=0.4", 'data_status="ok"',
                 "ood_count=1i", "sample_ref=123i"):
        assert part in ln
    assert "details_json=" in ln
    bare = line(dbmod.prediction_point(1, "t", Prediction("m", {"fan": "ON"}), 5))     # no scores, no details
    assert "on_fan=1i" in bare and "score_" not in bare and "details_json" not in bare and "ood_count=0i" in bare


def test_command_ack_and_status_points():
    cmd = {"cmd_id": "c1", "sent_at": 1700000000.5, "sim_ts": None, "actuator": "fan", "state": "ON",
           "source": "auto", "reason": "model:mlp"}
    ln = line(dbmod.command_point(cmd, 7))
    assert ln.startswith("command,actuator=fan,source=auto ") and 'cmd_id="c1"' in ln and "sent_at=1700000000.5" in ln
    assert "sim_ts" not in ln                                        # absent, not an empty string
    ack = line(dbmod.ack_point("c1", 1700000000.7, "ON", 8))
    assert ack.startswith("ack ") and 'cmd_id="c1"' in ack and "acked_at=1700000000.7" in ack and ack.endswith(" 8")
    ln = line(dbmod.status_point("fan", "ON", None, 9))
    assert ln.startswith("actuator_status,actuator=fan ") and 'cmd_id=""' in ln


# --- behaviour without a reachable server --------------------------------------------------------------
@pytest.fixture
def offline():
    """A Database pointed at a closed port, with its background writer swapped for a recorder."""
    d = Database({"url": "http://127.0.0.1:9", "org": "o", "bucket": "b", "token": "t"})
    real_writer, d._writer = d._writer, Mock()
    yield d
    real_writer.close()
    d._client.close()


def test_unreachable_server_is_reported_not_fatal(offline):
    assert offline.ready is False and offline.info()["connected"] is False and offline.info()["last_error"]
    assert offline.counts() == {"samples": 0, "predictions": 0, "commands": 0, "acks": 0, "actuator_status": 0}
    with pytest.raises(DatabaseUnavailable):
        offline.recent_samples()
    with pytest.raises(DatabaseUnavailable):
        offline.recent_commands()


def test_reads_do_not_hammer_a_down_server(offline, monkeypatch):
    calls = []
    monkeypatch.setattr(offline, "_connect", lambda: calls.append(1))
    for _ in range(5):
        with pytest.raises(DatabaseUnavailable):
            offline.recent_samples()
    assert calls == []                                              # inside the retry back-off: no reconnect attempts
    offline._last_attempt = time.monotonic() - dbmod.RECONNECT_EVERY_S - 1
    with pytest.raises(DatabaseUnavailable):
        offline.recent_samples()
    assert calls == [1]                                             # after the back-off: one attempt


def test_writes_never_raise_into_the_control_loop(offline):
    offline._writer.write.side_effect = RuntimeError("serialisation exploded")
    assert isinstance(offline.insert_sample(sample()), int)         # still returns a point time
    offline.mark_ack("c1", time.time(), "ON")
    assert offline.write_errors == 2 and "exploded" in offline.last_error
    assert offline.counts()["samples"] == 0                         # a point that was never queued is not counted


def test_counts_follow_queued_writes_and_roll_back_on_a_failed_batch(offline):
    for i in range(3):
        offline.insert_sample(sample(i))
    offline.insert_command({"cmd_id": "c", "sent_at": time.time(), "actuator": "fan", "state": "ON", "source": "auto"})
    assert offline.counts()["samples"] == 3 and offline.counts()["commands"] == 1
    # the writer thread reports a batch that InfluxDB finally rejected:
    batch = "sample,gateway=gw seq=1i 1\nsample,gateway=gw seq=2i 2\ncommand,actuator=fan,source=auto state=\"ON\" 3"
    offline._on_write_error(None, batch, Exception("(404)\nReason: Not Found"))
    assert offline.counts()["samples"] == 1 and offline.counts()["commands"] == 0
    assert offline.write_errors == 3 and offline.last_error.startswith("write failed: (404)")
    offline._on_write_error(None, b"ack cmd_id=\"x\" 4", Exception("boom"))        # bytes payloads too
    assert offline.write_errors == 4 and offline.counts()["acks"] == 0              # never goes negative


def test_point_times_are_unique_and_never_in_the_future(offline):
    times = [offline._next_ns() for _ in range(1000)]
    assert times == sorted(set(times))                              # strictly increasing: points never overwrite
    far = offline._next_ns(time.time() + 3600)
    assert far <= time.time_ns() + 10**9                            # a caller's clock skew cannot hide data
    assert offline._next_ns(time.time() - 5) > times[-1]            # ...nor reorder it


# --- API behaviour when history cannot be read -----------------------------------------------------------
class BrokenReads(MemoryDatabase):
    def recent_samples(self, limit=200):
        raise DatabaseUnavailable("InfluxDB down")

    recent_commands = recent_samples


def test_history_api_answers_503_not_500_when_influx_is_down(cfg, monkeypatch):
    monkeypatch.setattr("iot_agri.backend.pipeline.MqttNode", OfflineMQTT)
    monkeypatch.setattr("iot_agri.backend.app.Database", BrokenReads)
    with TestClient(create_app(cfg)) as client:
        for path in ("/api/history", "/api/commands"):
            r = client.get(path)
            assert r.status_code == 503 and "InfluxDB down" in r.json()["detail"]
        state = client.get("/api/state")                            # live state does not depend on stored history
        assert state.status_code == 200 and state.json()["db_info"]["engine"] == "memory"
