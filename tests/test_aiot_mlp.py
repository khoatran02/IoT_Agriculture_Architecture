import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

from iot_agri.backend.inference import InferenceService, load_model
from conftest import make_sample

FIXTURE = json.loads((Path(__file__).parent / "fixtures/mlp_v1_predictions.json").read_text())


def model(cfg, variant="int8", sensors=None, actuators=None):
    inference = copy.deepcopy(cfg["inference"])
    inference["params"]["variant"] = variant
    return load_model(inference, sensors or [s["name"] for s in cfg["sensors"]],
                      actuators or [a["name"] for a in cfg["actuators"]], cfg["_base_dir"])


@pytest.mark.parametrize("variant", ["int8", "fp32"])
def test_matches_historical_validation_predictions(cfg, variant):
    m = model(cfg, variant)
    for row in FIXTURE["samples"]:
        pred = m.predict([make_sample(**row["sensors"])])
        expected = row["variants"][variant]
        assert pred.outputs == expected["outputs"]
        assert pred.scores == pytest.approx(expected["scores"], abs=1e-6)
        assert pred.details["model_version"] == "mlp_v1"
        assert pred.details["inference_ms"] >= 0


def test_uses_saved_per_actuator_threshold_instead_of_half(cfg):
    row = next(r for r in FIXTURE["samples"] if r["source_row_id"] == 35465)
    pred = model(cfg).predict([make_sample(**row["sensors"])])
    assert .5 < pred.scores["water_pump"] < .74
    assert pred.outputs["water_pump"] == "OFF"


def test_feature_order_and_paths_do_not_depend_on_cwd(cfg, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    m = model(cfg, sensors=[s["name"] for s in reversed(cfg["sensors"])],
              actuators=[a["name"] for a in reversed(cfg["actuators"])])
    row = FIXTURE["samples"][0]
    assert m.predict([make_sample(**row["sensors"])]).outputs == row["variants"]["int8"]["outputs"]
    assert cfg["inference"]["params"]["model_dir"] == "models/mlp_v1"


def test_rejects_mismatched_sensor_or_actuator_names(cfg):
    with pytest.raises(ValueError, match="sensors"):
        model(cfg, sensors=["temperature"])
    with pytest.raises(ValueError, match="actuators"):
        model(cfg, actuators=["fan", "pump", "water_pump"])


def test_ood_diagnostics_and_invalid_sample(cfg):
    m = model(cfg)
    pred = m.predict([make_sample(temperature=45)])
    assert pred.details["data_status"] == "out_of_training_range"
    assert pred.details["out_of_range_features"] == ["temperature"]
    assert InferenceService(m).run([make_sample(temperature=float("nan"))]) is None
    assert InferenceService(m).run([]) is None


def test_invalid_thresholds_fail_at_startup(cfg, tmp_path):
    import shutil
    package = Path(cfg["_base_dir"]) / cfg["inference"]["params"]["model_dir"]
    dest = tmp_path / "broken"
    shutil.copytree(package, dest)
    (dest / "thresholds.json").write_text('{"values": [-1, 0.5, 0.5]}')
    cfg["inference"]["params"]["model_dir"] = str(dest)
    with pytest.raises(ValueError, match="Invalid decision thresholds"):
        model(cfg)


def test_standalone_copy_runs_without_external_model_repo(cfg, tmp_path):
    import shutil
    root = Path(cfg["_base_dir"])
    standalone = tmp_path / "farm-platform"
    standalone.mkdir()
    for directory in ("iot_agri", "models", "datasets"):
        shutil.copytree(root / directory, standalone / directory,
                        ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copy2(root / "config.yaml", standalone / "config.yaml")
    code = '''
import importlib.abc
import sys
from pathlib import Path

class BlockExternalModel(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'aiot_ai', 'tensorflow', 'pandas', 'sklearn'}:
            raise ModuleNotFoundError(fullname)

sys.meta_path.insert(0, BlockExternalModel())
from iot_agri.backend.inference import load_model
from iot_agri.backend.ingestion import Sample
from iot_agri.config import load_config
from iot_agri.edge.gateway import Gateway
from iot_agri.backend.models import mlp_runtime
cfg = load_config()
gw = Gateway(cfg, max_samples=1)
values = {sensor.name: sensor.read(0) for sensor in gw.sensors}
assert len(gw.dataset) == 37920
assert Path(mlp_runtime.__file__).is_relative_to(Path.cwd())
for variant in ('int8', 'fp32'):
    cfg['inference']['params']['variant'] = variant
    model = load_model(cfg['inference'], list(values),
                       [a['name'] for a in cfg['actuators']], cfg['_base_dir'])
    pred = model.predict([Sample(0, gw.dataset.timestamps[0], 'test', values, {})])
    assert set(pred.outputs) == {'fan', 'irrigation_pump', 'water_pump'}
    assert pred.details['model_version'] == 'mlp_v1'
assert 'aiot_ai' not in sys.modules
'''
    subprocess.run([sys.executable, "-c", code], cwd=standalone, check=True, capture_output=True)


def test_default_assets_are_inside_repo(cfg):
    from iot_agri.config import resolve_path
    root = Path(cfg["_base_dir"])
    dataset = resolve_path(cfg, cfg["dataset"]["path"])
    bundle = resolve_path(cfg, cfg["inference"]["params"]["model_dir"])
    assert dataset.is_relative_to(root) and bundle.is_relative_to(root)
    assert dataset.is_file() and (bundle / "model_metadata.json").is_file()
