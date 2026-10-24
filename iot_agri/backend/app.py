"""REST + WebSocket API and the static dashboard. The pipeline lives for the lifetime of the app."""
import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .db import Database, DatabaseUnavailable
from .decision import OverrideBlocked
from .pipeline import Pipeline

log = logging.getLogger(__name__)
STATIC = Path(__file__).resolve().parent.parent / "client" / "static"


class OverrideRequest(BaseModel):
    actuator: str
    state: Literal["ON", "OFF"]
    ttl_s: float | None = Field(default=None, gt=0, allow_inf_nan=False)


class GatewayControl(BaseModel):
    """Playback control for the simulated gateway (forwarded over MQTT)."""
    action: Literal["pause", "resume", "step", "interval", "fault", "clear_faults"]
    interval_s: float | None = Field(default=None, ge=0.02, le=60, allow_inf_nan=False)
    sensor: str | None = None
    kind: Literal["dropout", "spike"] | None = None
    cycles: int = Field(default=3, ge=1, le=1000)


class Hub:
    """Fans pipeline events out to every connected WebSocket client."""

    def __init__(self):
        self.clients: set[asyncio.Queue] = set()

    def broadcast(self, message: dict) -> None:
        for q in self.clients:
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:   # slow client: drop rather than stall the pipeline
                pass


def create_app(cfg: dict) -> FastAPI:
    hub = Hub()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        loop = asyncio.get_running_loop()
        db = Database(cfg["influxdb"])
        try:
            # Pipeline callbacks hand events from paho/ticker threads to the event loop.
            pipeline = Pipeline(cfg, db, emit=lambda t, d: loop.call_soon_threadsafe(
                hub.broadcast, {"type": t, "data": d},
            ))
            app.state.db, app.state.pipeline = db, pipeline
            try:
                pipeline.start()
                yield
            finally:
                pipeline.stop()
        finally:
            db.close()

    app = FastAPI(title="IoT Agriculture Platform", lifespan=lifespan)

    def snapshot() -> dict:
        return {**app.state.pipeline.snapshot(), "ws_clients": len(hub.clients)}

    @app.get("/api/state")
    def state():
        return snapshot()

    @app.get("/api/cycles")
    def cycles(limit: int = 60):
        return app.state.pipeline.recent_cycles(min(max(limit, 1), 100))

    @app.get("/api/mqtt")
    def mqtt_traffic(limit: int = 300):
        return app.state.pipeline.tap.recent(min(max(limit, 1), 400))

    def stored(read, limit: int, cap: int):
        try:
            return read(min(max(limit, 1), cap))
        except DatabaseUnavailable as e:
            raise HTTPException(503, f"history unavailable: {e}")

    @app.get("/api/history")
    def history(limit: int = 200):
        return stored(app.state.db.recent_samples, limit, 5000)

    @app.get("/api/commands")
    def commands(limit: int = 50):
        return stored(app.state.db.recent_commands, limit, 500)

    @app.post("/api/override")
    def override(req: OverrideRequest):
        try:
            return app.state.pipeline.override(req.actuator, req.state, req.ttl_s)
        except KeyError:
            raise HTTPException(404, f"unknown actuator {req.actuator!r}")
        except OverrideBlocked as e:
            raise HTTPException(409, str(e))

    @app.delete("/api/override/{actuator}")
    def release(actuator: str):
        try:
            return app.state.pipeline.release(actuator)
        except KeyError:
            raise HTTPException(404, f"unknown actuator {actuator!r}")

    @app.post("/api/gateway/control")
    def gateway_control(req: GatewayControl):
        try:
            return app.state.pipeline.gateway_control(req.model_dump(exclude_none=True))
        except ValueError as e:
            raise HTTPException(422, str(e))

    @app.websocket("/ws")
    async def ws(socket: WebSocket):
        await socket.accept()
        queue: asyncio.Queue = asyncio.Queue(maxsize=500)
        hub.clients.add(queue)

        async def send():
            await socket.send_json({"type": "snapshot", "data": snapshot()})
            while True:
                await socket.send_json(await queue.get())

        async def receive():   # only to notice the client leaving
            while True:
                await socket.receive_text()

        tasks = [asyncio.create_task(send()), asyncio.create_task(receive())]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in tasks:
                t.cancel()
            hub.clients.discard(queue)

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(STATIC / "index.html")

    app.mount("/static", StaticFiles(directory=STATIC), name="static")
    return app
