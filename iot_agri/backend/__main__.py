"""Platform layer: ingestion, inference, decision engine, DB and REST/WebSocket API in one process.

    python -m iot_agri.backend [--port 8000]
"""
import argparse

import uvicorn

from ..config import load_config, setup_logging
from .app import create_app


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config")
    ap.add_argument("--host")
    ap.add_argument("--port", type=int)
    args = ap.parse_args()
    setup_logging()
    cfg = load_config(args.config)
    uvicorn.run(create_app(cfg), host=args.host or cfg["backend"]["host"], port=args.port or cfg["backend"]["port"],
                log_level="warning")


if __name__ == "__main__":
    main()
