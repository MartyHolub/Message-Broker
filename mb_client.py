import argparse
import asyncio
import json
from typing import Any

import msgpack
import websockets
from websockets.asyncio.client import ClientConnection


def encode_message(data: dict[str, Any], message_format: str) -> str | bytes:
    if message_format == "json":
        return json.dumps(data)
    return msgpack.packb(data, use_bin_type=True)


def decode_message(data: str | bytes) -> dict[str, Any]:
    if isinstance(data, str):
        return json.loads(data)
    return msgpack.unpackb(data, raw=False)


async def send_message(
    connection: ClientConnection,
    payload: dict[str, Any],
    message_format: str,
) -> None:
    encoded = encode_message(payload, message_format)
    await connection.send(encoded)


async def recv_message(connection: ClientConnection) -> dict[str, Any]:
    data = await connection.recv()
    if not isinstance(data, (str, bytes)):
        raise ValueError("Unsupported WebSocket message type")
    return decode_message(data)


async def run_subscriber(
    url: str,
    topic: str,
    message_format: str,
    max_messages: int | None,
) -> None:
    async with websockets.connect(url) as ws:
        await send_message(
            ws,
            {
                "action": "subscribe",
                "topic": topic,
                "format": message_format,
            },
            message_format,
        )
        received = 0
        while max_messages is None or received < max_messages:
            message = await recv_message(ws)
            if message.get("action") != "deliver":
                continue
            print(message)
            await send_message(
                ws,
                {
                    "action": "ack",
                    "message_id": message["message_id"],
                },
                message_format,
            )
            received += 1


async def run_publisher(
    url: str,
    topic: str,
    message_format: str,
    count: int,
    interval: float,
    payload: dict[str, Any] | None,
) -> None:
    async with websockets.connect(url) as ws:
        for index in range(count):
            publish_payload = payload if payload is not None else {"sequence": index}
            await send_message(
                ws,
                {
                    "action": "publish",
                    "topic": topic,
                    "payload": publish_payload,
                },
                message_format,
            )
            if interval > 0:
                await asyncio.sleep(interval)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Message Broker client")
    parser.add_argument("--url", default="ws://127.0.0.1:8000/broker")
    parser.add_argument("--mode", choices=["publisher", "subscriber"], required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--format", choices=["json", "msgpack"], default="json")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--interval", type=float, default=0.0)
    parser.add_argument("--payload", type=str, default=None, help="JSON payload for publisher")
    parser.add_argument("--max-messages", type=int, default=None)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    payload = json.loads(args.payload) if args.payload else None

    if args.mode == "publisher":
        await run_publisher(
            url=args.url,
            topic=args.topic,
            message_format=args.format,
            count=args.count,
            interval=args.interval,
            payload=payload,
        )
    else:
        await run_subscriber(
            url=args.url,
            topic=args.topic,
            message_format=args.format,
            max_messages=args.max_messages,
        )


if __name__ == "__main__":
    asyncio.run(main())
