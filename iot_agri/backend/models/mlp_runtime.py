"""Standalone LiteRT inference, adapted from AIoT-AI-models/aiot_ai/runtime.py.

Only deployment code is included. Provenance is recorded in models/provenance.json.
"""
import json
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import numpy as np

FEATURES = ["temperature", "humidity", "water_level", "N", "P", "K"]
ACTUATORS = ["fan", "irrigation_pump", "water_pump"]


def read_json(path):
    return json.loads(Path(path).read_text())


@dataclass
class Preprocessor:
    features: list
    mean: np.ndarray
    scale: np.ndarray
    minimum: np.ndarray
    maximum: np.ndarray

    def transform(self, x):
        x = np.asarray(x, dtype=np.float32)
        if x.ndim != 2 or x.shape[1] != len(self.features) or not np.isfinite(x).all():
            raise ValueError("Expected finite sensor matrix with correct feature order")
        return ((x - self.mean) / self.scale).astype(np.float32)

    def sensor_vector(self, sensors):
        if not isinstance(sensors, dict):
            raise ValueError("sensors must be an object")
        values = []
        for feature in self.features:
            value = sensors.get(feature)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"Missing or non-numeric sensor: {feature}")
            values.append(value)
        x = np.asarray([values], dtype=np.float32)
        self.transform(x)  # validate finite values
        ood = [
            f
            for i, f in enumerate(self.features)
            if x[0, i] < self.minimum[i] or x[0, i] > self.maximum[i]
        ]
        return x, ood

    @classmethod
    def load(cls, path):
        raw = read_json(path)
        obj = cls(
            raw["features"],
            *[
                np.asarray(raw[k], dtype=np.float32)
                for k in ["mean", "scale", "minimum", "maximum"]
            ],
        )
        if (
            not all(
                v.shape == (len(obj.features),) and np.isfinite(v).all()
                for v in [obj.mean, obj.scale, obj.minimum, obj.maximum]
            )
            or (obj.scale <= 0).any()
        ):
            raise ValueError("Invalid preprocessing artifact")
        return obj


def runtime_api():
    try:
        from ai_edge_litert.interpreter import Interpreter, OpResolverType

        return Interpreter, OpResolverType, "ai_edge_litert"
    except ImportError as error:
        raise RuntimeError(
            "LiteRT is required. Install dependencies with python -m pip install -r requirements.txt"
        ) from error


def make_interpreter(path, threads=1):
    interpreter_class, resolver_type, backend = runtime_api()
    interpreter = interpreter_class(
        model_path=str(path),
        num_threads=threads,
        experimental_op_resolver_type=resolver_type.BUILTIN_WITHOUT_DEFAULT_DELEGATES,
    )
    interpreter.allocate_tensors()
    return interpreter, backend


def quantize(values, detail):
    if not np.isfinite(values).all():
        raise ValueError("Cannot quantize non-finite values")
    dtype = np.dtype(detail["dtype"])
    if np.issubdtype(dtype, np.integer):
        scale, zero = detail["quantization"]
        if scale <= 0:
            raise ValueError("Quantization scale must be positive")
        bounds = np.iinfo(dtype)
        return np.clip(np.rint(values / scale) + zero, bounds.min, bounds.max).astype(dtype)
    return np.asarray(values, dtype=dtype)


def dequantize(values, detail):
    if np.issubdtype(np.dtype(detail["dtype"]), np.integer):
        scale, zero = detail["quantization"]
        if scale <= 0:
            raise ValueError("Output quantization scale must be positive")
        return (values.astype(np.float32) - zero) * scale
    return values.astype(np.float32)


class EdgeModel:
    def __init__(self, run_dir, variant="int8", threads=1):
        if variant not in {"fp32", "int8"}:
            raise ValueError("variant must be fp32 or int8")
        self.directory = Path(run_dir)
        self.variant = variant
        metadata = read_json(self.directory / "model_metadata.json")
        self.prep = Preprocessor.load(self.directory / "preprocessing.json")
        if self.prep.features != FEATURES or metadata["actuators"] != ACTUATORS:
            raise ValueError("Unexpected feature or actuator order")
        self.thresholds = np.asarray(read_json(self.directory / "thresholds.json")["values"])
        if (
            self.thresholds.shape != (3,)
            or not np.isfinite(self.thresholds).all()
            or (self.thresholds < 0).any()
            or (self.thresholds > 1).any()
        ):
            raise ValueError("Invalid decision thresholds")
        self.model_version = metadata["model_version"]
        self.preprocessing_version = "standard-v1"
        self.interpreter, self.backend = make_interpreter(
            self.directory / f"model_{variant}.tflite", threads
        )
        self.input = self.interpreter.get_input_details()[0]
        self.output = self.interpreter.get_output_details()[0]
        if list(self.input["shape"]) != [1, 6] or list(self.output["shape"]) != [1, 3]:
            raise ValueError("Unexpected model tensor shape")
        expected_dtype = np.int8 if variant == "int8" else np.float32
        if self.input["dtype"] != expected_dtype or self.output["dtype"] != expected_dtype:
            raise ValueError("Model dtype does not match variant")

    def predict_normalized(self, normalized):
        scores = []
        for sample in normalized:
            tensor = quantize(sample.reshape(1, -1), self.input)
            self.interpreter.set_tensor(self.input["index"], tensor)
            self.interpreter.invoke()
            result = self.interpreter.get_tensor(self.output["index"])
            scores.append(dequantize(result, self.output)[0])
        return np.asarray(scores, dtype=np.float32).reshape(-1, 3)

    def predict_raw(self, raw):
        return self.predict_normalized(self.prep.transform(raw))

    def predict_sensors(self, sensors):
        """Predict directly from six sensor values, without telemetry or device metadata."""
        started = perf_counter()
        x, ood = self.prep.sensor_vector(sensors)
        scores = self.predict_raw(x)[0]
        return {
            "model_version": self.model_version,
            "preprocessing_version": self.preprocessing_version,
            "on_scores": dict(zip(ACTUATORS, map(float, scores))),
            "proposed_states": dict(zip(ACTUATORS, map(bool, scores >= self.thresholds))),
            "data_status": "out_of_training_range" if ood else "ok",
            "out_of_range_features": ood,
            "inference_ms": (perf_counter() - started) * 1000,
        }
