import pytest

from iot_agri.backend.ingestion import Sample
from iot_agri.config import load_config


@pytest.fixture
def cfg():
    return load_config()


def make_sample(seq=0, ts="2024-02-08T06:00:00", **overrides):
    values = {"temperature": 20.0, "humidity": 60.0, "water_level": 100.0, "N": 255.0, "P": 255.0, "K": 255.0}
    values.update(overrides)
    return Sample(seq, ts, "gw-01", values, {n: "ok" for n in values})
