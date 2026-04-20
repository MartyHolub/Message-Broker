import argparse
import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any

import msgpack
import websockets
from websockets.asyncio.client import ClientConnection


@dataclass
class BenchmarkResult:
    message_format: str
    publishers: int
    subscribers: int
    messages_per_publisher: int
    elapsed_seconds: float
    throughput_msg_per_sec: float


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
    await connection.send(encode_message(payload, message_format))


async def recv_message(connection: ClientConnection) -> dict[str, Any]:
    incoming = await connection.recv()
    if not isinstance(incoming, (str, bytes)):
        raise ValueError("Unsupported frame type")
    return decode_message(incoming)


async def subscriber_worker(
    url: str,
    topic: str,
    message_format: str,
    expected_messages: int,
    ready_event: asyncio.Event,
) -> None:
    async with websockets.connect(url) as ws:
        await send_message(
            ws,
            {"action": "subscribe", "topic": topic, "format": message_format},
            message_format,
        )
        subscribed_confirmation = await recv_message(ws)
        if subscribed_confirmation.get("action") != "subscribed":
            raise RuntimeError("Broker did not confirm subscription")
        ready_event.set()
        received = 0
        while received < expected_messages:
            message = await recv_message(ws)
            if message.get("action") != "deliver":
                continue
            await send_message(
                ws,
                {"action": "ack", "message_id": message["message_id"]},
                message_format,
            )
            received += 1


async def publisher_worker(
    url: str,
    topic: str,
    message_format: str,
    messages_to_send: int,
) -> None:
    async with websockets.connect(url) as ws:
        for index in range(messages_to_send):
            await send_message(
                ws,
                {
                    "action": "publish",
                    "topic": topic,
                    "payload": {"publisher_sequence": index},
                },
                message_format,
            )
            published_confirmation = await recv_message(ws)
            if published_confirmation.get("action") != "published":
                raise RuntimeError("Broker did not confirm publish")


async def run_benchmark(
    url: str,
    topic: str,
    message_format: str,
    publishers: int,
    subscribers: int,
    messages_per_publisher: int,
) -> BenchmarkResult:
    ready_events = [asyncio.Event() for _ in range(subscribers)]
    expected_messages_per_subscriber = publishers * messages_per_publisher

    subscriber_tasks = [
        asyncio.create_task(
            subscriber_worker(
                url=url,
                topic=topic,
                message_format=message_format,
                expected_messages=expected_messages_per_subscriber,
                ready_event=ready_events[index],
            )
        )
        for index in range(subscribers)
    ]

    await asyncio.gather(*[event.wait() for event in ready_events])

    start_time = time.perf_counter()
    publisher_tasks = [
        asyncio.create_task(
            publisher_worker(
                url=url,
                topic=topic,
                message_format=message_format,
                messages_to_send=messages_per_publisher,
            )
        )
        for _ in range(publishers)
    ]

    await asyncio.gather(*publisher_tasks)
    await asyncio.gather(*subscriber_tasks)
    elapsed = time.perf_counter() - start_time
    total_deliveries = subscribers * expected_messages_per_subscriber
    throughput = total_deliveries / elapsed

    return BenchmarkResult(
        message_format=message_format,
        publishers=publishers,
        subscribers=subscribers,
        messages_per_publisher=messages_per_publisher,
        elapsed_seconds=elapsed,
        throughput_msg_per_sec=throughput,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark for custom message broker")
    parser.add_argument("--url", default="ws://127.0.0.1:8000/broker")
    parser.add_argument("--topic", default="benchmark")
    parser.add_argument("--publishers", type=int, default=5)
    parser.add_argument("--subscribers", type=int, default=5)
    parser.add_argument("--messages-per-publisher", type=int, default=10_000)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    for message_format in ("json", "msgpack"):
        result = await run_benchmark(
            url=args.url,
            topic=args.topic,
            message_format=message_format,
            publishers=args.publishers,
            subscribers=args.subscribers,
            messages_per_publisher=args.messages_per_publisher,
        )
        print(
            f"{result.message_format}: "
            f"elapsed={result.elapsed_seconds:.3f}s, "
            f"throughput={result.throughput_msg_per_sec:.2f} msg/s, "
            f"publishers={result.publishers}, "
            f"subscribers={result.subscribers}, "
            f"messages/publisher={result.messages_per_publisher}"
        )


if __name__ == "__main__":
    asyncio.run(main())
