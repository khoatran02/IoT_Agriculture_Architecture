import csv
from pathlib import Path


class Dataset:
    """The CSV, sorted by timestamp (the raw file is not in time order). One row = one 5-minute tick.

    Rows with a blank date (2 in the shipped file) are dropped: they have no place on the time axis.
    """

    def __init__(self, path):
        with open(Path(path), newline="") as f:
            rows = [r for r in csv.DictReader(f) if r["date"].strip()]
        rows.sort(key=lambda r: r["date"])  # stable: duplicate timestamps keep file order
        self._rows = rows
        self.timestamps = [r["date"].replace(" ", "T") for r in rows]
        self._cols: dict[str, list[float]] = {}

    def __len__(self) -> int:
        return len(self._rows)

    def column(self, name: str) -> list[float]:
        if name not in self._cols:
            self._cols[name] = [float(r[name]) for r in self._rows]
        return self._cols[name]
