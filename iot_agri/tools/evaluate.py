"""Score the model named in config.yaml against the CSV's actuator labels (offline, no broker needed).

    python -m iot_agri.tools.evaluate [--config config.yaml]
"""
import argparse
from collections import deque

from ..backend.inference import InferenceService, load_model
from ..backend.ingestion import Sample
from ..config import load_config, resolve_path
from ..device.dataset import Dataset


def evaluate(cfg: dict) -> dict[str, dict]:
    ds = Dataset(resolve_path(cfg, cfg["dataset"]["path"]))
    sensor_names = [s["name"] for s in cfg["sensors"]]
    actuators = cfg["actuators"]
    cols = {s["name"]: ds.column(s["column"]) for s in cfg["sensors"]}
    labels = {a["name"]: ds.column(a["dataset_column"]) for a in actuators}
    service = InferenceService(load_model(cfg["inference"], sensor_names, [a["name"] for a in actuators]))
    window = deque(maxlen=cfg["backend"]["window_size"])
    counts = {a: [0, 0, 0, 0] for a in labels}   # [actual ON right, actual ON wrong, actual OFF right, actual OFF wrong]
    for i, ts in enumerate(ds.timestamps):
        window.append(Sample(i, ts, "offline", {n: cols[n][i] for n in sensor_names}, {}))
        pred = service.run(list(window))
        for name, truth in labels.items():
            predicted_on, actual_on = pred.outputs[name] == "ON", truth[i] == 1.0
            counts[name][(0 if actual_on else 2) + (0 if predicted_on == actual_on else 1)] += 1
    out = {}
    for name, (on_right, on_wrong, off_right, off_wrong) in counts.items():
        n = on_right + on_wrong + off_right + off_wrong
        precision = on_right / (on_right + off_wrong) if on_right + off_wrong else 0.0
        recall = on_right / (on_right + on_wrong) if on_right + on_wrong else 0.0
        out[name] = {"accuracy": (on_right + off_right) / n, "precision": precision, "recall": recall, "n": n}
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config")
    cfg = load_config(ap.parse_args().config)
    print(f"model: {cfg['inference']['model']}  ({len(Dataset(resolve_path(cfg, cfg['dataset']['path'])))} rows, time-sorted)")
    print(f"{'actuator':18s} {'accuracy':>9s} {'precision':>10s} {'recall':>8s}")
    for name, m in evaluate(cfg).items():
        print(f"{name:18s} {m['accuracy']:9.3f} {m['precision']:10.3f} {m['recall']:8.3f}")


if __name__ == "__main__":
    main()
