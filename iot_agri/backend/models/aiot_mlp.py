"""Adapter for the bundled MLP and the repository's standalone LiteRT runtime."""
from pathlib import Path

from .mlp_runtime import ACTUATORS, FEATURES, EdgeModel

from ..inference import InferenceModel, Prediction


class AIoTMLP(InferenceModel):
    window_size = 1  # the MLP was trained on individual rows, not temporal windows

    def __init__(self, sensor_names, actuator_names, model_dir, variant="int8", threads=1):
        super().__init__(sensor_names, actuator_names)
        if len(sensor_names) != len(FEATURES) or set(sensor_names) != set(FEATURES):
            raise ValueError(f"MLP requires exactly these sensors: {FEATURES}")
        if len(actuator_names) != len(ACTUATORS) or set(actuator_names) != set(ACTUATORS):
            raise ValueError(f"MLP requires exactly these actuators: {ACTUATORS}")
        self.runtime = EdgeModel(Path(model_dir), variant=variant, threads=threads)
        self.name = f"{self.runtime.model_version}-{variant}"

    def predict(self, window):
        result = self.runtime.predict_sensors(window[-1].values)
        return Prediction(
            model=self.name,
            outputs={a: "ON" if result["proposed_states"][a] else "OFF" for a in self.actuator_names},
            scores={a: result["on_scores"][a] for a in self.actuator_names},
            details={k: result[k] for k in (
                "model_version", "preprocessing_version", "data_status",
                "out_of_range_features", "inference_ms",
            )} | {"variant": self.runtime.variant},
        )
