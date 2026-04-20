import json
from typing import Any, Literal

import msgpack

MessageFormat = Literal["json", "msgpack"]
ALLOWED_FORMATS: set[MessageFormat] = {"json", "msgpack"}


def encode_payload(payload: Any) -> bytes:
    return msgpack.packb(payload, use_bin_type=True)


def decode_payload(payload: bytes) -> Any:
    return msgpack.unpackb(payload, raw=False)


def decode_incoming_message(raw_text: str | None, raw_bytes: bytes | None) -> dict[str, Any]:
    if raw_text is not None:
        data = json.loads(raw_text)
    elif raw_bytes is not None:
        data = msgpack.unpackb(raw_bytes, raw=False)
    else:
        raise ValueError("Unsupported WebSocket frame.")

    if not isinstance(data, dict):
        raise ValueError("Message payload must be an object.")
    return data


def serialize_for_format(data: dict[str, Any], fmt: MessageFormat) -> tuple[str | None, bytes | None]:
    if fmt == "json":
        return json.dumps(data), None
    return None, msgpack.packb(data, use_bin_type=True)
