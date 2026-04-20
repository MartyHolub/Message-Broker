import asyncio
import time

import pytest
from fastapi.testclient import TestClient

from app.broker import manager
from app.database import init_db
from app.main import app


@pytest.fixture(autouse=True)
def isolated_db() -> None:
    asyncio.run(init_db(drop_existing=True))
    manager.active_connections.clear()
    manager.websocket_topics.clear()
    manager.websocket_formats.clear()


def _wait_until(predicate, timeout: float = 1.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def test_successful_connection_and_disconnection() -> None:
    with TestClient(app) as client:
        with client.websocket_connect("/broker") as ws:
            ws.send_json({"action": "subscribe", "topic": "topic-x", "format": "json"})
            assert ws.receive_json() == {"action": "subscribed", "topic": "topic-x"}
            assert "topic-x" in manager.active_connections
        assert _wait_until(lambda: "topic-x" not in manager.active_connections)


def test_message_routed_to_subscribed_topic() -> None:
    with TestClient(app) as client:
        with client.websocket_connect("/broker") as subscriber:
            subscriber.send_json({"action": "subscribe", "topic": "topic-x", "format": "json"})
            assert subscriber.receive_json() == {"action": "subscribed", "topic": "topic-x"}

            with client.websocket_connect("/broker") as publisher:
                publisher.send_json(
                    {"action": "publish", "topic": "topic-x", "payload": {"temp": 22.5}}
                )
                published = publisher.receive_json()
                assert published["action"] == "published"
                assert published["topic"] == "topic-x"

            delivered = subscriber.receive_json()
            assert delivered["action"] == "deliver"
            assert delivered["topic"] == "topic-x"
            assert delivered["payload"] == {"temp": 22.5}
            subscriber.send_json({"action": "ack", "message_id": delivered["message_id"]})
            assert subscriber.receive_json() == {
                "action": "acked",
                "message_id": delivered["message_id"],
            }


def test_message_not_delivered_to_unsubscribed_topic() -> None:
    with TestClient(app) as client:
        with client.websocket_connect("/broker") as subscriber:
            subscriber.send_json({"action": "subscribe", "topic": "topic-x", "format": "json"})
            assert subscriber.receive_json() == {"action": "subscribed", "topic": "topic-x"}

            with client.websocket_connect("/broker") as publisher:
                publisher.send_json(
                    {"action": "publish", "topic": "topic-y", "payload": {"value": 1}}
                )
                _ = publisher.receive_json()
                publisher.send_json(
                    {"action": "publish", "topic": "topic-x", "payload": {"value": 2}}
                )
                _ = publisher.receive_json()

            delivered = subscriber.receive_json()
            assert delivered["topic"] == "topic-x"
            assert delivered["payload"] == {"value": 2}
            subscriber.send_json({"action": "ack", "message_id": delivered["message_id"]})
            assert subscriber.receive_json() == {
                "action": "acked",
                "message_id": delivered["message_id"],
            }
