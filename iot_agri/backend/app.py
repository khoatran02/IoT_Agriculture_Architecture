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

from ..config import resolve_path
from .db import Database
from .decision import OverrideBlocked
from .pipeline import Pipeline

log = logging.getLogger(__name__)
STATIC = Path(__file__).resolve().parent.parent / "client" / "static"


class OverrideRequest(BaseModel):
    actuator: str
    state: Literal["ON", "OFF"]
    ttl_s: float | None = Field(default=None, gt=0, allow_inf_nan=False)


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
        db = Database(resolve_path(cfg, cfg["backend"]["db_path"]))
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

    @app.get("/api/state")
    def state():
        return app.state.pipeline.snapshot()

    @app.get("/api/history")
    def history(limit: int = 200):
        return app.state.db.recent_samples(min(max(limit, 1), 5000))

    @app.get("/api/commands")
    def commands(limit: int = 50):
        return app.state.db.recent_commands(min(max(limit, 1), 500))

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

    @app.websocket("/ws")
    async def ws(socket: WebSocket):
        await socket.accept()
        queue: asyncio.Queue = asyncio.Queue(maxsize=500)
        hub.clients.add(queue)

        async def send():
            await socket.send_json({"type": "snapshot", "data": app.state.pipeline.snapshot()})
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
