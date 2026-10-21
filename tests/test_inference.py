import sys
import types

import pytest

from iot_agri.backend.inference import InferenceModel, InferenceService, Prediction, ThresholdModel, load_model
from conftest import make_sample

SENSORS = ["temperature", "humidity", "water_level", "N", "P", "K"]
ACTS = ["fan", "irrigation_pump", "water_pump"]


def test_threshold_model_follows_the_rules(cfg):
    m = load_model(cfg["inference"], SENSORS, ACTS)
    out = m.predict([make_sample(temperature=30, humidity=30, water_level=40)]).outputs
    assert out == {"fan": "ON", "irrigation_pump": "ON", "water_pump": "ON"}
    out = m.predict([make_sample(temperature=15, humidity=70, water_level=100)]).outputs
    assert out == {"fan": "OFF", "irrigation_pump": "OFF", "water_pump": "OFF"}


def test_threshold_model_has_hysteresis(cfg):
    m = load_model(cfg["inference"], SENSORS, ACTS)
    m.predict([make_sample(water_level=40)])
    assert m.predict([make_sample(water_level=67)]).outputs["water_pump"] == "ON"    # between on_below and off_at
    assert m.predict([make_sample(water_level=72)]).outputs["water_pump"] == "OFF"


def test_custom_model_is_loaded_by_dotted_path():
    class Always(InferenceModel):
        name, window_size = "always", 3

        def predict(self, window):
            return Prediction.from_scores(self.name, {a: 0.9 for a in self.actuator_names})

    mod = types.ModuleType("fake_model_mod")
    mod.Always = Always
    sys.modules["fake_model_mod"] = mod
    m = load_model({"model": "fake_model_mod:Always", "params": {}}, SENSORS, ACTS)
    assert m.predict([make_sample()]).outputs == {a: "ON" for a in ACTS}


def test_service_pads_short_window_and_survives_model_errors():
    seen = []

    class Windowed(InferenceModel):
        name, window_size = "w", 4

        def predict(self, window):
            seen.append(len(window))
            return Prediction(self.name, {})

    InferenceService(Windowed(SENSORS, ACTS)).run([make_sample()])
    assert seen == [4]

    class Broken(Windowed):
        def predict(self, window):
            raise RuntimeError("boom")

    assert InferenceService(Broken(SENSORS, ACTS)).run([make_sample()]) is None


def test_dl_template_is_importable_but_not_wired(cfg):
    m = load_model({"model": "iot_agri.backend.models.dl_template:DLModel"}, SENSORS, ACTS)
    assert len(m.features([make_sample()] * m.window_size)[0]) == 6
    with pytest.raises(NotImplementedError):
        m.predict([make_sample()] * m.window_size)
