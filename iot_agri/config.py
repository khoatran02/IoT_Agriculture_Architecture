import logging
import os
from pathlib import Path

import yaml

DEFAULT_CONFIG = Path(__file__).resolve().parent.parent / "config.yaml"


def load_config(path=None) -> dict:
    path = Path(path or os.environ.get("AGRI_CONFIG") or DEFAULT_CONFIG).resolve()
    cfg = yaml.safe_load(path.read_text())
    cfg["_base_dir"] = str(path.parent)
    if host := os.environ.get("MQTT_HOST"):
        cfg["mqtt"]["host"] = host
    if port := os.environ.get("MQTT_PORT"):
        cfg["mqtt"]["port"] = int(port)
    return cfg


def resolve_path(cfg: dict, p) -> Path:
    p = Path(p)
    return p if p.is_absolute() else (Path(cfg["_base_dir"]) / p).resolve()


def setup_logging(level=logging.INFO) -> None:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    logging.getLogger("paho").setLevel(logging.WARNING)
