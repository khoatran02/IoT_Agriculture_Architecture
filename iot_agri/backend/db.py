import json
import sqlite3
import threading
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS samples(
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, seq INTEGER, gateway TEXT, recv_at REAL,
    values_json TEXT, quality_json TEXT);
CREATE TABLE IF NOT EXISTS predictions(
    id INTEGER PRIMARY KEY AUTOINCREMENT, sample_id INTEGER, ts TEXT, model TEXT,
    outputs_json TEXT, scores_json TEXT);
CREATE TABLE IF NOT EXISTS commands(
    id INTEGER PRIMARY KEY AUTOINCREMENT, cmd_id TEXT UNIQUE, sent_at REAL, sim_ts TEXT, actuator TEXT,
    state TEXT, source TEXT, reason TEXT, acked_at REAL, ack_state TEXT);
CREATE TABLE IF NOT EXISTS actuator_status(
    id INTEGER PRIMARY KEY AUTOINCREMENT, recv_at REAL, actuator TEXT, state TEXT, cmd_id TEXT);
"""


class Database:
    """SQLite, shared between the MQTT thread, the ticker and API requests (hence the lock)."""

    def __init__(self, path):
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(str(path), check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(SCHEMA)

    def _write(self, sql: str, args=()) -> int:
        with self._lock:
            cur = self._db.execute(sql, args)
            self._db.commit()
            return cur.lastrowid

    def _read(self, sql: str, args=()) -> list[sqlite3.Row]:
        with self._lock:
            return self._db.execute(sql, args).fetchall()

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def insert_sample(self, s) -> int:
        return self._write("INSERT INTO samples(ts,seq,gateway,recv_at,values_json,quality_json) VALUES(?,?,?,?,?,?)",
                           (s.ts, s.seq, s.gateway, s.recv_at, json.dumps(s.values), json.dumps(s.quality)))

    def insert_prediction(self, sample_id: int, ts: str, p) -> int:
        return self._write("INSERT INTO predictions(sample_id,ts,model,outputs_json,scores_json) VALUES(?,?,?,?,?)",
                           (sample_id, ts, p.model, json.dumps(p.outputs), json.dumps(p.scores)))

    def insert_command(self, c: dict) -> int:
        return self._write("INSERT INTO commands(cmd_id,sent_at,sim_ts,actuator,state,source,reason) "
                           "VALUES(:cmd_id,:sent_at,:sim_ts,:actuator,:state,:source,:reason)", c)

    def mark_ack(self, cmd_id: str, at: float, state: str) -> None:
        self._write("UPDATE commands SET acked_at=?, ack_state=? WHERE cmd_id=? AND acked_at IS NULL",
                    (at, state, cmd_id))

    def insert_status(self, at: float, actuator: str, state: str, cmd_id) -> int:
        return self._write("INSERT INTO actuator_status(recv_at,actuator,state,cmd_id) VALUES(?,?,?,?)",
                           (at, actuator, state, cmd_id))

    def recent_samples(self, limit: int = 200) -> list[dict]:
        rows = self._read("SELECT * FROM samples ORDER BY id DESC LIMIT ?", (limit,))
        return [{"seq": r["seq"], "ts": r["ts"], "gateway": r["gateway"], "values": json.loads(r["values_json"]),
                 "quality": json.loads(r["quality_json"])} for r in reversed(rows)]

    def recent_commands(self, limit: int = 50) -> list[dict]:
        return [dict(r) for r in self._read("SELECT * FROM commands ORDER BY id DESC LIMIT ?", (limit,))]
