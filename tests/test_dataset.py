from iot_agri.config import resolve_path
from iot_agri.device.dataset import Dataset
from iot_agri.device.sensors import build_sensors


def test_shipped_dataset_is_time_sorted_and_has_no_blank_timestamps(cfg):
    ds = Dataset(resolve_path(cfg, cfg["dataset"]["path"]))
    assert len(ds) == 37920 and all(ds.timestamps) and ds.timestamps == sorted(ds.timestamps)


def test_every_config_column_exists_and_each_sensor_reads_its_own_column(cfg):
    ds = Dataset(resolve_path(cfg, cfg["dataset"]["path"]))
    for a in cfg["actuators"]:
        assert set(ds.column(a["dataset_column"])) <= {0.0, 1.0}
    sensors = {s.name: s for s in build_sensors(cfg["sensors"], ds)}
    assert set(sensors) == {"temperature", "humidity", "water_level", "N", "P", "K"}
    assert sensors["temperature"].read(0) == ds.column("tempreature")[0]


def test_blank_date_rows_are_dropped(tmp_path):
    p = tmp_path / "d.csv"
    p.write_text("date,x\n2024-01-02 00:00:00,2\n,9\n2024-01-01 00:00:00,1\n")
    ds = Dataset(p)
    assert ds.timestamps == ["2024-01-01T00:00:00", "2024-01-02T00:00:00"] and ds.column("x") == [1.0, 2.0]
