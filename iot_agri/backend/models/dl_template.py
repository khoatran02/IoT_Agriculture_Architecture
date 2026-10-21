"""Slot for the deep-learning model. Fill in __init__ and predict, then in config.yaml set:

    inference:
      model: iot_agri.backend.models.dl_template:DLModel
      params: {weights_path: models/actuator_net.pt}

Input : self.features(window) -> window_size x 6 matrix; columns are temperature, humidity, water_level, N, P, K
        (config.yaml sensor order). Samples are already range-checked and gap-filled by ingestion.
Output: P(ON) per actuator for fan, irrigation_pump, water_pump. The training labels are the
        Fan_actuator_ON / Watering_plant_pump_ON / Water_pump_actuator_ON columns of the CSV.
Check : python -m iot_agri.tools.evaluate  scores whatever model config.yaml names against those labels.
"""
from ..inference import InferenceModel, Prediction


class DLModel(InferenceModel):
    name = "dl"
    window_size = 12   # 12 samples x 5 simulated minutes = 1 hour of history; match your training setup

    def __init__(self, sensor_names, actuator_names, weights_path=None, threshold=0.5, **params):
        super().__init__(sensor_names, actuator_names)
        self.threshold = threshold
        # TODO: load weights / scaler here, e.g. self.net = torch.load(weights_path).eval()

    def predict(self, window):
        x = self.features(window)
        # TODO: run the network on x and map its outputs to {actuator: probability_on}.
        raise NotImplementedError("DL model not wired in yet")
        scores = {}  # noqa: F841  e.g. {"fan": 0.93, "irrigation_pump": 0.02, "water_pump": 0.61}
        return Prediction.from_scores(self.name, scores, self.threshold)
