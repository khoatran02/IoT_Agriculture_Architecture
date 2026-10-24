"""Test doubles."""


class MemoryDatabase:
    """Stands in for backend.db.Database where a test needs the platform but not an InfluxDB server.
    Same interface; it records what was written so tests can assert on it."""

    def __init__(self, *_args, **_kwargs):
        self.samples, self.predictions, self.commands, self.acks, self.statuses = [], [], [], [], []
        self._n = 0

    def _ns(self) -> int:
        self._n += 1
        return self._n * 10**9              # a fake, strictly increasing "point time"

    def insert_sample(self, s):
        self.samples.append(s)
        return self._ns()

    def insert_prediction(self, sample_ref, sim_ts, p):
        self.predictions.append((sample_ref, sim_ts, p))
        return self._ns()

    def insert_command(self, c):
        self.commands.append(dict(c))
        return self._ns()

    def mark_ack(self, cmd_id, at, state):
        self.acks.append({"cmd_id": cmd_id, "acked_at": at, "state": state})

    def insert_status(self, at, actuator, state, cmd_id):
        self.statuses.append({"at": at, "actuator": actuator, "state": state, "cmd_id": cmd_id})
        return self._ns()

    def recent_samples(self, limit=200):
        return [{"seq": s.seq, "ts": s.ts, "gateway": s.gateway, "values": dict(s.values), "quality": dict(s.quality)}
                for s in self.samples[-limit:]]

    def recent_commands(self, limit=50):
        first_ack = {}
        for a in self.acks:
            first_ack.setdefault(a["cmd_id"], a)
        return [{**c, "acked_at": first_ack.get(c["cmd_id"], {}).get("acked_at"),
                 "ack_state": first_ack.get(c["cmd_id"], {}).get("state")} for c in reversed(self.commands[-limit:])]

    def counts(self):
        return {"samples": len(self.samples), "predictions": len(self.predictions), "commands": len(self.commands),
                "acks": len(self.acks), "actuator_status": len(self.statuses)}

    def info(self):
        return {"engine": "memory", "url": "memory", "org": "-", "bucket": "-", "connected": True,
                "write_errors": 0, "last_error": None}

    def close(self):
        pass
