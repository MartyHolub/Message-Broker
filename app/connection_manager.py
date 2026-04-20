import asyncio

from fastapi import WebSocket

from app.protocol import MessageFormat


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: dict[str, set[WebSocket]] = {}
        self.websocket_topics: dict[WebSocket, set[str]] = {}
        self.websocket_formats: dict[WebSocket, MessageFormat] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self.websocket_topics[websocket] = set()
            self.websocket_formats[websocket] = "json"

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            topics = self.websocket_topics.pop(websocket, set())
            for topic in topics:
                subscribers = self.active_connections.get(topic)
                if not subscribers:
                    continue
                subscribers.discard(websocket)
                if not subscribers:
                    del self.active_connections[topic]
            self.websocket_formats.pop(websocket, None)

    async def subscribe(self, websocket: WebSocket, topic: str) -> None:
        async with self._lock:
            self.active_connections.setdefault(topic, set()).add(websocket)
            self.websocket_topics.setdefault(websocket, set()).add(topic)

    async def set_format(self, websocket: WebSocket, fmt: MessageFormat) -> None:
        async with self._lock:
            self.websocket_formats[websocket] = fmt

    async def get_format(self, websocket: WebSocket) -> MessageFormat:
        async with self._lock:
            return self.websocket_formats.get(websocket, "json")

    async def get_subscribers(self, topic: str) -> list[WebSocket]:
        async with self._lock:
            return list(self.active_connections.get(topic, set()))
