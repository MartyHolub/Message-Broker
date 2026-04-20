from __future__ import annotations

from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.connection_manager import ConnectionManager
from app.database import async_session_factory
from app.models import QueuedMessage
from app.protocol import (
    ALLOWED_FORMATS,
    decode_incoming_message,
    decode_payload,
    encode_payload,
    serialize_for_format,
)


router = APIRouter()
manager = ConnectionManager()


def _build_delivery_message(message: QueuedMessage) -> dict[str, Any]:
    return {
        "action": "deliver",
        "topic": message.topic,
        "message_id": message.id,
        "payload": decode_payload(message.payload),
    }


async def _send_to_websocket(websocket: WebSocket, data: dict[str, Any]) -> None:
    fmt = await manager.get_format(websocket)
    text_data, bytes_data = serialize_for_format(data, fmt)
    if text_data is not None:
        await websocket.send_text(text_data)
    elif bytes_data is not None:
        await websocket.send_bytes(bytes_data)


async def _load_pending_messages(session: AsyncSession, topic: str) -> list[QueuedMessage]:
    result = await session.execute(
        select(QueuedMessage)
        .where(QueuedMessage.topic == topic, QueuedMessage.is_delivered.is_(False))
        .order_by(QueuedMessage.id.asc())
    )
    return list(result.scalars().all())


async def _mark_message_delivered(session: AsyncSession, message_id: int) -> None:
    await session.execute(
        update(QueuedMessage)
        .where(QueuedMessage.id == message_id)
        .values(is_delivered=True),
    )


@router.websocket("/broker")
async def broker_endpoint(websocket: WebSocket) -> None:
    await manager.connect(websocket)
    try:
        while True:
            frame = await websocket.receive()
            incoming_message = decode_incoming_message(frame.get("text"), frame.get("bytes"))

            action = incoming_message.get("action")

            if action == "subscribe":
                topic = incoming_message.get("topic")
                if not isinstance(topic, str) or not topic:
                    await websocket.send_json({"action": "error", "message": "Invalid topic"})
                    continue

                fmt = incoming_message.get("format", "json")
                if fmt not in ALLOWED_FORMATS:
                    await websocket.send_json({"action": "error", "message": "Invalid format"})
                    continue

                await manager.set_format(websocket, fmt)
                await manager.subscribe(websocket, topic)

                async with async_session_factory() as session:
                    pending_messages = await _load_pending_messages(session, topic)

                for pending_message in pending_messages:
                    await _send_to_websocket(websocket, _build_delivery_message(pending_message))

            elif action == "publish":
                topic = incoming_message.get("topic")
                if not isinstance(topic, str) or not topic:
                    await websocket.send_json({"action": "error", "message": "Invalid topic"})
                    continue

                payload = incoming_message.get("payload")

                async with async_session_factory() as session:
                    stored_message = QueuedMessage(
                        topic=topic,
                        payload=encode_payload(payload),
                        is_delivered=False,
                    )
                    session.add(stored_message)
                    await session.flush()
                    await session.commit()
                    await session.refresh(stored_message)

                delivery = _build_delivery_message(stored_message)
                subscribers = await manager.get_subscribers(topic)
                for subscriber in subscribers:
                    try:
                        await _send_to_websocket(subscriber, delivery)
                    except WebSocketDisconnect:
                        await manager.disconnect(subscriber)
                    except RuntimeError:
                        await manager.disconnect(subscriber)

            elif action == "ack":
                message_id = incoming_message.get("message_id")
                if not isinstance(message_id, int):
                    await websocket.send_json({"action": "error", "message": "Invalid message_id"})
                    continue

                async with async_session_factory() as session:
                    await _mark_message_delivered(session, message_id)
                    await session.commit()

            else:
                await websocket.send_json({"action": "error", "message": "Unknown action"})

    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except RuntimeError:
        await manager.disconnect(websocket)
