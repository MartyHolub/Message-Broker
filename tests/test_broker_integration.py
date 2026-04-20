import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.broker import manager
from app.database import init_db, reconfigure_database
from app.main import app


@pytest.fixture(autouse=True)
async def isolated_db(tmp_path: Path) -> None:
    db_path = tmp_path / "test_broker.db"
    await reconfigure_database(f"sqlite+aiosqlite:///{db_path}")
    await init_db(drop_existing=True)


@pytest.mark.asyncio
async def test_successful_connection_and_disconnection() -> None:
    def scenario() -> None:
        with TestClient(app) as client:
            with client.websocket_connect("/broker") as ws:
                ws.send_json({"action": "subscribe", "topic": "topic-x", "format": "json"})
                assert "topic-x" in manager.active_connections
            assert "topic-x" not in manager.active_connections

    await asyncio.to_thread(scenario)


@pytest.mark.asyncio
async def test_message_routed_to_subscribed_topic() -> None:
    def scenario() -> None:
        with TestClient(app) as client:
            with client.websocket_connect("/broker") as subscriber:
                subscriber.send_json(
                    {"action": "subscribe", "topic": "topic-x", "format": "json"}
                )

                with client.websocket_connect("/broker") as publisher:
                    publisher.send_json(
                        {
                            "action": "publish",
                            "topic": "topic-x",
                            "payload": {"temp": 22.5},
                        }
                    )

                delivered = subscriber.receive_json()
                assert delivered["action"] == "deliver"
                assert delivered["topic"] == "topic-x"
                assert delivered["payload"] == {"temp": 22.5}
                subscriber.send_json({"action": "ack", "message_id": delivered["message_id"]})

    await asyncio.to_thread(scenario)


@pytest.mark.asyncio
async def test_message_not_delivered_to_unsubscribed_topic() -> None:
    def scenario() -> None:
        with TestClient(app) as client:
            with client.websocket_connect("/broker") as subscriber:
                subscriber.send_json(
                    {"action": "subscribe", "topic": "topic-x", "format": "json"}
                )

                with client.websocket_connect("/broker") as publisher:
                    publisher.send_json(
                        {"action": "publish", "topic": "topic-y", "payload": {"value": 1}}
                    )
                    publisher.send_json(
                        {"action": "publish", "topic": "topic-x", "payload": {"value": 2}}
                    )

                delivered = subscriber.receive_json()
                assert delivered["topic"] == "topic-x"
                assert delivered["payload"] == {"value": 2}
                subscriber.send_json({"action": "ack", "message_id": delivered["message_id"]})

    await asyncio.to_thread(scenario)
