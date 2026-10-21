from iot_agri.backend.ingestion import Ingestion

SENSORS = [{"name": "t", "valid_range": [0, 50]}, {"name": "h", "valid_range": [0, 100]}]


def reading(sensor, value, seq, run="r1", ts="2024-01-01T00:00:00"):
    return {"sensor": sensor, "value": value, "seq": seq, "run_id": run, "ts": ts, "gw": "gw"}


def setup(**kw):
    out = []
    return Ingestion(SENSORS, out.append, **kw), out


def test_groups_readings_into_one_sample():
    ing, out = setup()
    ing.on_reading(reading("t", 20, 0))
    assert out == []
    ing.on_reading(reading("h", 55, 0))
    assert len(out) == 1 and out[0].values == {"t": 20, "h": 55} and set(out[0].quality.values()) == {"ok"}


def test_duplicate_delivery_is_ignored():
    ing, out = setup()
    for _ in range(2):
        ing.on_reading(reading("t", 20, 0))
        ing.on_reading(reading("h", 55, 0))
    assert len(out) == 1 and ing.stats["duplicate"] == 2


def test_restarted_gateway_is_not_mistaken_for_duplicates():
    ing, out = setup()
    for run in ("a", "b"):
        ing.on_reading(reading("t", 20, 0, run))
        ing.on_reading(reading("h", 55, 0, run))
    assert len(out) == 2


def test_out_of_range_value_is_replaced_by_last_good():
    ing, out = setup()
    ing.on_reading(reading("t", 20, 0)); ing.on_reading(reading("h", 55, 0))
    ing.on_reading(reading("t", 999, 1)); ing.on_reading(reading("h", 56, 1))
    assert out[1].values["t"] == 20 and out[1].quality["t"] == "imputed_range" and out[1].quality["h"] == "ok"


def test_missing_sensor_is_imputed_when_next_cycle_arrives():
    ing, out = setup()
    ing.on_reading(reading("t", 20, 0)); ing.on_reading(reading("h", 55, 0))
    ing.on_reading(reading("t", 21, 1))                       # h never answers in cycle 1
    ing.on_reading(reading("t", 22, 2))                       # cycle 2 arrives -> cycle 1 is final
    assert out[1].values == {"t": 21, "h": 55} and out[1].quality["h"] == "imputed_missing"


def test_cycle_without_any_good_value_is_dropped():
    ing, out = setup()
    ing.on_reading(reading("t", 20, 0))
    ing.on_reading(reading("t", 21, 1))                       # h has never reported
    assert out == [] and ing.stats["dropped"] == 1


def test_stale_cycle_is_flushed_by_timer():
    now = [0.0]
    ing, out = setup(stale_after_s=5, clock=lambda: now[0])
    ing.on_reading(reading("t", 20, 0)); ing.on_reading(reading("h", 55, 0))
    ing.on_reading(reading("t", 21, 1))
    now[0] = 10
    ing.flush_stale()
    assert len(out) == 2 and out[1].quality["h"] == "imputed_missing"


def test_malformed_and_unknown_are_counted_not_raised():
    ing, out = setup()
    ing.on_reading({"value": 1})
    ing.on_reading(reading("ph", 7, 0))
    assert out == [] and ing.stats["malformed"] == 1 and ing.stats["unknown_sensor"] == 1
