"""Inference service. The model behind it is pluggable: implement InferenceModel and name it in config.yaml.

Contract: predict(window) gets the last `window_size` cleaned Samples (oldest first, front-padded with the
first sample while the buffer is still filling) and returns the desired ON/OFF state per actuator.
"""
import importlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

from .ingestion import Sample

log = logging.getLogger(__name__)


@dataclass
class Prediction:
    model: str
    outputs: dict[str, str]                  # actuator name -> "ON" | "OFF"
    scores: dict[str, float] | None = None   # optional P(ON) per actuator, shown on the dashboard

    @classmethod
    def from_scores(cls, model: str, scores: dict[str, float], threshold: float = 0.5) -> "Prediction":
        return cls(model, {a: "ON" if p >= threshold else "OFF" for a, p in scores.items()}, dict(scores))


class InferenceModel(ABC):
    name = "model"
    window_size = 1

    def __init__(self, sensor_names: list[str], actuator_names: list[str], **params):
        self.sensor_names, self.actuator_names = sensor_names, actuator_names

    def features(self, window: list[Sample]) -> list[list[float]]:
        """window -> [window_size][n_sensors] matrix, columns in config.yaml sensor order."""
        return [[s.values[n] for n in self.sensor_names] for s in window]

    @abstractmethod
    def predict(self, window: list[Sample]) -> Prediction: ...


class ThresholdModel(InferenceModel):
    """Rule-based stand-in with hysteresis, fitted by eye to the CSV. Replace with the DL model later."""

    name = "threshold"
    _SENSOR = {"fan": "temperature", "irrigation_pump": "humidity", "water_pump": "water_level"}

    def __init__(self, sensor_names, actuator_names, **params):
        super().__init__(sensor_names, actuator_names)
        self.params = params
        self._state = {a: "OFF" for a in actuator_names}

    def predict(self, window: list[Sample]) -> Prediction:
        latest = window[-1].values
        for act, p in self.params.items():
            if act not in self._state:
                continue
            x = latest[self._SENSOR[act]]
            if act == "fan":      # hot -> cool down
                self._state[act] = "ON" if x >= p["on_at"] else "OFF" if x < p["off_below"] else self._state[act]
            else:                 # dry / low -> switch on until the recovery level is reached
                self._state[act] = "ON" if x < p["on_below"] else "OFF" if x >= p["off_at"] else self._state[act]
        return Prediction(self.name, dict(self._state))


def load_model(inference_cfg: dict, sensor_names: list[str], actuator_names: list[str]) -> InferenceModel:
    kind, params = inference_cfg.get("model", "threshold"), inference_cfg.get("params") or {}
    if kind == "threshold":
        return ThresholdModel(sensor_names, actuator_names, **params)
    module_name, _, class_name = kind.partition(":")   # "package.module:ClassName"
    cls = getattr(importlib.import_module(module_name), class_name)
    return cls(sensor_names, actuator_names, **params)


class InferenceService:
    def __init__(self, model: InferenceModel):
        self.model = model

    def run(self, window: list[Sample]) -> Prediction | None:
        need = self.model.window_size
        if len(window) < need:
            window = [window[0]] * (need - len(window)) + window
        try:
            return self.model.predict(window[-need:])
        except Exception:
            log.exception("model %s failed; no decision this cycle", self.model.name)
            return None
