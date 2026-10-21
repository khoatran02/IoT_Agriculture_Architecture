import random

from .dataset import Dataset


class SimulatedSensor:
    """One CSV column replayed as a physical sensor. read(i) is the value at tick i, or None if it did not answer."""

    def __init__(self, name: str, series: list[float], unit: str, interface: str,
                 dropout_prob: float = 0.0, rng: random.Random | None = None):
        self.name, self.unit, self.interface = name, unit, interface
        self._series = series
        self._dropout_prob = dropout_prob
        self._rng = rng or random.Random()

    def read(self, i: int) -> float | int | None:
        if self._dropout_prob and self._rng.random() < self._dropout_prob:
            return None
        v = self._series[i]
        return int(v) if v.is_integer() else v


def build_sensors(sensors_cfg: list[dict], dataset: Dataset, dropout_prob: float = 0.0) -> list[SimulatedSensor]:
    return [
        SimulatedSensor(s["name"], dataset.column(s["column"]), s.get("unit", ""), s.get("interface", ""),
                        dropout_prob)
        for s in sensors_cfg
    ]
