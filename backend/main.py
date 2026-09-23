import asyncio
import threading

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

import config
from consumer import consume_loop

app = FastAPI()
app.mount("/static", StaticFiles(directory="static", html=True), name="static")

connected_clients: dict[str, list[WebSocket]] = {name: [] for name in config.TOPICS}
main_loop: asyncio.AbstractEventLoop | None = None


def make_handler(name: str):
    def handle_message(value):
        # Bridges the plain consumer thread into the async event loop —
        # calling async code directly from a thread would crash.
        asyncio.run_coroutine_threadsafe(broadcast(name, value), main_loop)
    return handle_message


async def broadcast(name: str, data: dict):
    for ws in list(connected_clients[name]):
        try:
            await ws.send_json(data)
        except Exception:
            connected_clients[name].remove(ws)


@app.on_event("startup")
async def startup():
    global main_loop
    main_loop = asyncio.get_event_loop()

    for name, topic in config.TOPICS.items():
        thread = threading.Thread(
            target=consume_loop,
            args=(topic, make_handler(name)),
            daemon=True,  # dies with the app instead of lingering after shutdown
        )
        thread.start()


@app.websocket("/ws/{name}")
async def websocket_endpoint(websocket: WebSocket, name: str):
    if name not in config.TOPICS:
        await websocket.close(code=4404)
        return
    await websocket.accept()
    connected_clients[name].append(websocket)
    try:
        while True:
            await websocket.receive_text()  # keeps the connection open; payload unused
    except WebSocketDisconnect:
        connected_clients[name].remove(websocket)
