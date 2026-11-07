#!/usr/bin/env python3
"""Bounded, read-only observer for a capsule Codex app-server thread.

The helper starts ``codex app-server proxy`` for an already-private Unix
socket.  It speaks the proxy's RFC 6455 WebSocket transport directly; the
messages inside it are JSON-RPC, not JSONL.  JSONL is used only for the
private observation record written by ``observe``.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import math
import os
import secrets
import selectors
import struct
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, BinaryIO


MAX_HEADER_BYTES = 64 * 1024
MAX_MESSAGE_BYTES = 8 * 1024 * 1024
MAX_PAGES = 10_000
MAX_PENDING_MESSAGES = 10_000
WEBSOCKET_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class ObserverError(RuntimeError):
    pass


class ReadTimeout(ObserverError):
    def __init__(self, partial: bool):
        self.partial = partial
        super().__init__("proxy read timed out mid-frame" if partial else "proxy read timed out")


def compact_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def require_object(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ObserverError(f"{where} is not an object")
    return value


def observer_paths(root_text: str, socket_text: str) -> tuple[Path, Path]:
    return Path(root_text), Path(socket_text)


def private_log(root: Path, text: str) -> tuple[Path, BinaryIO]:
    path = Path(text)
    if not path.is_absolute():
        path = root / path
    parent = path.parent.resolve(strict=True)
    try:
        parent.relative_to(root)
    except ValueError as error:
        raise ObserverError("observation log is outside the private root") from error
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    return path, os.fdopen(fd, "wb", buffering=0)


def masked_client_frame(opcode: int, payload: bytes, *, fin: bool = True,
                        mask: bytes | None = None) -> bytes:
    if len(payload) > MAX_MESSAGE_BYTES:
        raise ObserverError("outgoing WebSocket message exceeds size budget")
    mask = secrets.token_bytes(4) if mask is None else mask
    if len(mask) != 4:
        raise ObserverError("WebSocket mask must be four bytes")
    first = (0x80 if fin else 0) | opcode
    length = len(payload)
    if length < 126:
        header = bytes((first, 0x80 | length))
    elif length <= 0xFFFF:
        header = bytes((first, 0x80 | 126)) + struct.pack("!H", length)
    else:
        header = bytes((first, 0x80 | 127)) + struct.pack("!Q", length)
    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    return header + mask + masked


class DeadlineReader:
    def __init__(self, source: BinaryIO):
        self.source = source
        self.selector = selectors.DefaultSelector()
        self.selector.register(source, selectors.EVENT_READ)

    def exact(self, count: int, deadline: float) -> bytes:
        result = bytearray()
        while len(result) < count:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not self.selector.select(remaining):
                raise ReadTimeout(bool(result))
            part = os.read(self.source.fileno(), count - len(result))
            if not part:
                raise ObserverError("proxy closed its output")
            result.extend(part)
        return bytes(result)

    def header(self, deadline: float) -> bytes:
        result = bytearray()
        while not result.endswith(b"\r\n\r\n"):
            try:
                result.extend(self.exact(1, deadline))
            except ReadTimeout as error:
                raise ReadTimeout(bool(result) or error.partial) from error
            if len(result) > MAX_HEADER_BYTES:
                raise ObserverError("WebSocket response headers exceed size budget")
        return bytes(result)


class DeadlineWriter:
    def __init__(self, sink: BinaryIO):
        self.sink = sink
        os.set_blocking(sink.fileno(), False)
        self.selector = selectors.DefaultSelector()
        self.selector.register(sink, selectors.EVENT_WRITE)

    def all(self, data: bytes, deadline: float) -> None:
        view = memoryview(data)
        written = 0
        while written < len(view):
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not self.selector.select(remaining):
                raise ObserverError("proxy write timed out")
            try:
                count = os.write(self.sink.fileno(), view[written:])
            except BlockingIOError:
                continue
            if count <= 0:
                raise ObserverError("proxy closed its input")
            written += count


def validate_upgrade(raw: bytes, expected: str) -> None:
    try:
        lines = raw.decode("ascii").split("\r\n")
    except UnicodeDecodeError as error:
        raise ObserverError("WebSocket upgrade headers are not ASCII") from error
    if len(lines[0].split()) < 2 or lines[0].split()[1] != "101":
        raise ObserverError(f"WebSocket upgrade refused: {lines[0]}")
    headers: dict[str, list[str]] = {}
    for line in lines[1:]:
        if not line:
            continue
        if ":" not in line:
            raise ObserverError("malformed WebSocket upgrade header")
        name, value = line.split(":", 1)
        headers.setdefault(name.strip().lower(), []).append(value.strip())
    tokens = lambda name: {
        token.strip().lower()
        for value in headers.get(name, [])
        for token in value.split(",")
    }
    if "websocket" not in tokens("upgrade") or "upgrade" not in tokens("connection"):
        raise ObserverError("WebSocket upgrade headers are incomplete")
    accepts = headers.get("sec-websocket-accept", [])
    if len(accepts) != 1 or not secrets.compare_digest(accepts[0], expected):
        raise ObserverError("WebSocket upgrade authentication failed")


class ProxySession:
    def __init__(self, executable: str, socket_path: Path, timeout: float,
                 max_message_bytes: int = MAX_MESSAGE_BYTES):
        if not math.isfinite(timeout) or timeout <= 0:
            raise ObserverError("timeout must be finite and positive")
        self.timeout = timeout
        self.max_message_bytes = max_message_bytes
        self.child = subprocess.Popen(
            [executable, "app-server", "proxy", "--sock", str(socket_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            # A proxy diagnostic cannot be allowed to fill a pipe and stall
            # the observer.  The capsule owns its app-server diagnostics;
            # this helper reports typed transport/protocol failures.
            stderr=subprocess.DEVNULL,
        )
        if self.child.stdin is None or self.child.stdout is None:
            self.close()
            raise ObserverError("proxy pipes were unavailable")
        self.input = self.child.stdin
        self.writer = DeadlineWriter(self.input)
        self.reader = DeadlineReader(self.child.stdout)
        self.notifications: list[dict[str, Any]] = []
        self.prior_notifications: list[dict[str, Any]] = []
        self.unmatched_replies: list[dict[str, Any]] = []
        self.next_id = 1
        self.fragment_opcode: int | None = None
        self.fragment = bytearray()
        try:
            self._upgrade()
        except BaseException:
            self.close()
            raise

    def _upgrade(self) -> None:
        nonce = secrets.token_bytes(16)
        key = base64.b64encode(nonce).decode("ascii")
        expected = base64.b64encode(hashlib.sha1(key.encode() + WEBSOCKET_GUID).digest()).decode()
        request = (
            "GET / HTTP/1.1\r\nHost: capsule-observer\r\nUpgrade: websocket\r\n"
            f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode("ascii")
        deadline = time.monotonic() + self.timeout
        self.writer.all(request, deadline)
        validate_upgrade(self.reader.header(deadline), expected)

    def send_frame(self, opcode: int, payload: bytes, *, fin: bool = True) -> None:
        self.writer.all(
            masked_client_frame(opcode, payload, fin=fin),
            time.monotonic() + self.timeout,
        )

    def send_json(self, value: Any) -> None:
        self.send_frame(1, compact_json(value))

    def _frame(self, deadline: float) -> tuple[bool, int, bytes]:
        progressed = False
        try:
            raw_head = self.reader.exact(2, deadline)
            progressed = True
            first, second = raw_head
            fin = bool(first & 0x80)
            if first & 0x70:
                raise ObserverError("WebSocket frame uses an unnegotiated extension")
            opcode = first & 0x0F
            if second & 0x80:
                raise ObserverError("server WebSocket frame is masked")
            length = second & 0x7F
            if length == 126:
                length = struct.unpack("!H", self.reader.exact(2, deadline))[0]
                if length < 126:
                    raise ObserverError("WebSocket frame uses a non-minimal length")
            elif length == 127:
                raw = self.reader.exact(8, deadline)
                if raw[0] & 0x80:
                    raise ObserverError("WebSocket frame length has its high bit set")
                length = struct.unpack("!Q", raw)[0]
                if length <= 0xFFFF:
                    raise ObserverError("WebSocket frame uses a non-minimal length")
            if length > self.max_message_bytes:
                raise ObserverError("WebSocket frame exceeds size budget")
            if opcode >= 8 and (not fin or length > 125):
                raise ObserverError("invalid WebSocket control frame")
            return fin, opcode, self.reader.exact(length, deadline)
        except ReadTimeout as error:
            raise ReadTimeout(progressed or error.partial) from error

    def receive_json(self, deadline: float) -> dict[str, Any]:
        while True:
            try:
                fin, opcode, payload = self._frame(deadline)
            except ReadTimeout as error:
                raise ReadTimeout(error.partial or self.fragment_opcode is not None) from error
            if opcode == 8:
                if len(payload) == 1:
                    raise ObserverError("malformed WebSocket close frame")
                try:
                    self.send_frame(8, payload)
                except (BrokenPipeError, ObserverError):
                    pass
                raise ObserverError("proxy closed the WebSocket")
            if opcode == 9:
                self.send_frame(10, payload)
                continue
            if opcode == 10:
                continue
            if opcode == 2:
                raise ObserverError("proxy sent a binary WebSocket message")
            if opcode == 1:
                if self.fragment_opcode is not None:
                    raise ObserverError("new WebSocket message during fragmentation")
                if fin:
                    message = payload
                else:
                    self.fragment_opcode = opcode
                    self.fragment = bytearray(payload)
                    continue
            elif opcode == 0:
                if self.fragment_opcode is None:
                    raise ObserverError("unexpected WebSocket continuation")
                self.fragment.extend(payload)
                if len(self.fragment) > self.max_message_bytes:
                    raise ObserverError("fragmented WebSocket message exceeds size budget")
                if not fin:
                    continue
                message = bytes(self.fragment)
                self.fragment.clear()
                self.fragment_opcode = None
            else:
                raise ObserverError(f"unsupported WebSocket opcode {opcode}")
            try:
                return require_object(json.loads(message), "JSON-RPC message")
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ObserverError(f"invalid JSON-RPC message: {error}") from error

    def request(self, method: str, params: Any) -> dict[str, Any]:
        request_id = self.next_id
        self.next_id += 1
        self.send_json({"id": request_id, "method": method, "params": params})
        deadline = time.monotonic() + self.timeout
        while True:
            message = self.receive_json(deadline)
            # Server requests have a method even when their id happens to equal ours.
            if "method" in message:
                if "id" in message:
                    raise ObserverError("unsupported JSON-RPC request from app-server")
                self._enqueue(self.notifications, message)
                continue
            if message.get("id") != request_id:
                self._enqueue(self.unmatched_replies, message)
                continue
            if "error" in message:
                raise ObserverError(f"{method} refused: {json.dumps(message['error'], ensure_ascii=False)}")
            if "result" not in message:
                raise ObserverError(f"{method} response has no result")
            return require_object(message["result"], f"{method} result")

    def notify(self, method: str, params: Any) -> None:
        self.send_json({"method": method, "params": params})

    def initialize(self) -> None:
        self.request("initialize", {
            "clientInfo": {"name": "capsule-observer", "version": "1"},
            "capabilities": {"experimentalApi": True},
        })
        self.notify("initialized", None)

    def _enqueue(self, queue: list[dict[str, Any]], message: dict[str, Any]) -> None:
        if (len(self.notifications) + len(self.prior_notifications)
                + len(self.unmatched_replies) >= MAX_PENDING_MESSAGES):
            raise ObserverError("unrelated JSON-RPC messages exceed queue budget")
        queue.append(message)

    def begin_observation(self) -> None:
        """Preserve initialization notifications outside the measured interval."""
        self.prior_notifications.extend(self.notifications)
        self.notifications.clear()

    def notification(self, deadline: float) -> dict[str, Any]:
        if self.notifications:
            return self.notifications.pop(0)
        while True:
            message = self.receive_json(deadline)
            if "method" in message:
                if "id" in message:
                    raise ObserverError("unsupported JSON-RPC request from app-server")
                return message
            self._enqueue(self.unmatched_replies, message)

    def close(self) -> None:
        child = getattr(self, "child", None)
        if child is None:
            return
        try:
            if child.poll() is None:
                child.terminate()
            try:
                child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                child.kill()
                try:
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired as error:
                    raise ObserverError("proxy did not exit after kill") from error
        finally:
            self.child = None

    def __enter__(self) -> "ProxySession":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def paginated(session: Any, method: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    cursor: str | None = None
    seen: set[str] = set()
    for _ in range(MAX_PAGES):
        page_params = dict(params)
        if cursor is not None:
            page_params["cursor"] = cursor
        result = session.request(method, page_params)
        data = result.get("data")
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            raise ObserverError(f"{method} result has invalid data")
        records.extend(data)
        next_cursor = result.get("nextCursor")
        if next_cursor is None:
            return records
        if not isinstance(next_cursor, str) or not next_cursor:
            raise ObserverError(f"{method} returned an invalid nextCursor")
        if next_cursor in seen:
            raise ObserverError(f"{method} repeated a pagination cursor")
        seen.add(next_cursor)
        cursor = next_cursor
    raise ObserverError(f"{method} exceeded pagination budget")


def model_catalog(session: Any) -> dict[str, Any]:
    models = paginated(session, "model/list", {"limit": 100, "includeHidden": True})
    for model in models:
        if not isinstance(model.get("id"), str) or not isinstance(model.get("model"), str):
            raise ObserverError("model/list entry lacks string id or native model")
    return {
        "models": models,
        "note": "catalog membership is not an entitlement or execution witness",
    }


def full_history(session: Any, thread_id: str) -> dict[str, Any]:
    read = session.request("thread/read", {"threadId": thread_id, "includeTurns": False})
    thread = require_object(read.get("thread"), "thread/read result.thread")
    if thread.get("id") != thread_id:
        raise ObserverError("thread/read returned another thread")
    descending = paginated(session, "thread/turns/list", {
        "threadId": thread_id,
        "limit": 100,
        "sortDirection": "desc",
        "itemsView": "full",
    })
    chronological = list(reversed(descending))
    # Preserve every full item and every userMessage content array verbatim.
    user_content = []
    for turn in chronological:
        turn_id = turn.get("id")
        items = turn.get("items")
        if not isinstance(turn_id, str) or not turn_id:
            raise ObserverError("thread/turns/list returned a turn without an id")
        if not isinstance(items, list):
            raise ObserverError(f"full turn {turn_id} has no items array")
        for item in items:
            if not isinstance(item, dict) or not isinstance(item.get("type"), str):
                raise ObserverError(f"full turn {turn_id} has a malformed item")
            if item["type"] == "userMessage":
                content = item.get("content")
                if not isinstance(content, list) or not all(isinstance(part, dict) for part in content):
                    raise ObserverError(f"userMessage in turn {turn_id} has malformed content")
                user_content.append({"turnId": turn_id, "content": content})
    return {"thread": thread, "turns": chronological, "userMessageContent": user_content}


def relevant_notification(message: dict[str, Any], thread_id: str) -> dict[str, Any] | None:
    method = message.get("method")
    params = message.get("params")
    if not isinstance(method, str) or not isinstance(params, dict):
        return None
    if params.get("threadId") != thread_id:
        return None
    if method != "thread/tokenUsage/updated" and not method.startswith("turn/"):
        return None
    record: dict[str, Any] = {"method": method, "params": params}
    if method == "thread/tokenUsage/updated":
        turn_id = params.get("turnId")
        if not isinstance(turn_id, str) or not turn_id:
            raise ObserverError("token usage notification has no turnId")
        token_usage = require_object(params.get("tokenUsage"), "token usage notification.tokenUsage")
        last = require_object(token_usage.get("last"), "token usage notification.tokenUsage.last")
        tokens = last.get("inputTokens")
        if not isinstance(tokens, int) or isinstance(tokens, bool):
            raise ObserverError("token usage notification last.inputTokens is not an integer")
        record["signal"] = (
            {"turnId": turn_id, "lastInputTokens": tokens}
            if tokens > 0
            else None
        )
    return record


def notification_turn_id(record: dict[str, Any]) -> str | None:
    params = record["params"]
    direct = params.get("turnId")
    if isinstance(direct, str) and direct:
        return direct
    turn = params.get("turn")
    if isinstance(turn, dict) and isinstance(turn.get("id"), str) and turn["id"]:
        return turn["id"]
    return None


def observe(session: ProxySession, thread_id: str, duration: float,
            output: BinaryIO) -> dict[str, Any]:
    if not math.isfinite(duration) or duration <= 0 or duration > 86_400:
        raise ObserverError("observe duration must be finite and in (0, 86400] seconds")
    # Initialization may itself announce unrelated state.  The observation
    # interval starts with this subscription request; notifications received
    # while its reply is pending remain queued by request().
    session.begin_observation()
    resumed = session.request("thread/resume", {"threadId": thread_id, "excludeTurns": True})
    resumed_thread = require_object(resumed.get("thread"), "thread/resume result.thread")
    if resumed_thread.get("id") != thread_id:
        raise ObserverError("thread/resume returned another thread")
    ready = {"event": "subscribed", "threadId": thread_id,
             "timestamp": time.time_ns(), "durationSeconds": duration}
    output.write(compact_json(ready) + b"\n")
    print(json.dumps(ready, separators=(",", ":")), flush=True)
    deadline = time.monotonic() + duration
    count = 0
    turn_ids: set[str] = set()
    token_signals: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        try:
            message = session.notification(deadline)
        except ReadTimeout as error:
            if not error.partial and time.monotonic() >= deadline:
                break
            raise
        relevant = relevant_notification(message, thread_id)
        if relevant is None:
            continue
        turn_id = notification_turn_id(relevant)
        if relevant["method"].startswith("turn/") and turn_id is not None:
            turn_ids.add(turn_id)
        if relevant["method"] == "thread/tokenUsage/updated" and relevant["signal"] is not None:
            relevant["signal"]["turnObservedAtReceipt"] = relevant["signal"]["turnId"] in turn_ids
            token_signals.append(relevant["signal"])
        record = {"timestamp": time.time_ns(), **relevant}
        output.write(compact_json(record) + b"\n")
        count += 1
    completed_signals = [
        {**signal, "turnObserved": signal["turnId"] in turn_ids}
        for signal in token_signals
    ]
    summary = {
        "event": "observation-complete",
        "threadId": thread_id,
        "timestamp": time.time_ns(),
        "records": count,
        "tokenUsage": "OBSERVED" if token_signals else "UNOBSERVED",
        "tokenSignals": completed_signals,
    }
    output.write(compact_json(summary) + b"\n")
    return summary


class FakeSession:
    def __init__(self, replies: list[dict[str, Any]]):
        self.replies = list(replies)

    def request(self, _method: str, _params: Any) -> dict[str, Any]:
        if not self.replies:
            raise AssertionError("unexpected request")
        return self.replies.pop(0)


def server_frame(opcode: int, payload: bytes, *, fin: bool = True) -> bytes:
    first = (0x80 if fin else 0) | opcode
    length = len(payload)
    if length < 126:
        return bytes((first, length)) + payload
    if length <= 0xFFFF:
        return bytes((first, 126)) + struct.pack("!H", length) + payload
    return bytes((first, 127)) + struct.pack("!Q", length) + payload


def fake_pipe_session(incoming: bytes, *, timeout: float = 0.1,
                      max_message_bytes: int = MAX_MESSAGE_BYTES
                      ) -> tuple[ProxySession, BinaryIO, BinaryIO]:
    server_read, server_write = os.pipe()
    client_read, client_write = os.pipe()
    if incoming:
        os.write(server_write, incoming)
    session = ProxySession.__new__(ProxySession)
    session.timeout = timeout
    session.max_message_bytes = max_message_bytes
    session.reader = DeadlineReader(os.fdopen(server_read, "rb", buffering=0))
    session.input = os.fdopen(client_write, "wb", buffering=0)
    session.writer = DeadlineWriter(session.input)
    session.notifications = []
    session.prior_notifications = []
    session.unmatched_replies = []
    session.next_id = 1
    session.fragment_opcode = None
    session.fragment = bytearray()
    return session, os.fdopen(server_write, "wb", buffering=0), os.fdopen(
        client_read, "rb", buffering=0)


def expect_error(fragment: str, action: Any) -> None:
    try:
        action()
    except ObserverError as error:
        assert fragment in str(error), (fragment, str(error))
    else:
        raise AssertionError(f"expected ObserverError containing {fragment!r}")


def self_test() -> None:
    frame = masked_client_frame(1, b"hello", mask=b"\x01\x02\x03\x04")
    assert frame == b"\x81\x85\x01\x02\x03\x04igohn"
    accepted = (
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
        b"Connection: keep-alive, Upgrade\r\nSec-WebSocket-Accept: expected\r\n\r\n"
    )
    validate_upgrade(accepted, "expected")
    expect_error("authentication failed", lambda: validate_upgrade(accepted, "wrong"))
    expect_error("upgrade refused", lambda: validate_upgrade(
        b"HTTP/1.1 403 Forbidden\r\n\r\n", "expected"))

    fragmented = (
        server_frame(1, b'{"ok":', fin=False)
        + server_frame(9, b"p")
        + server_frame(0, b"true}")
    )
    pipe, server_sink, client_source = fake_pipe_session(fragmented)
    assert pipe.receive_json(time.monotonic() + 0.1) == {"ok": True}
    pong = os.read(client_source.fileno(), 7)
    assert pong[0] == 0x8A and pong[1] & 0x80 and len(pong) == 7
    server_sink.close()
    client_source.close()

    pipe, server_sink, client_source = fake_pipe_session(
        bytes((0x09, 0)), max_message_bytes=4)
    expect_error("control frame", lambda: pipe.receive_json(time.monotonic() + 0.1))
    server_sink.close()
    client_source.close()
    pipe, server_sink, client_source = fake_pipe_session(
        server_frame(1, b"12345"), max_message_bytes=4)
    expect_error("size budget", lambda: pipe.receive_json(time.monotonic() + 0.1))
    server_sink.close()
    client_source.close()

    pipe, server_sink, client_source = fake_pipe_session(b"", timeout=0.01)
    try:
        pipe.receive_json(time.monotonic() + 0.01)
    except ReadTimeout as error:
        assert not error.partial
    else:
        raise AssertionError("expected clean-boundary ReadTimeout")
    server_sink.close()
    client_source.close()
    pipe, server_sink, client_source = fake_pipe_session(b"\x81", timeout=0.01)
    try:
        pipe.receive_json(time.monotonic() + 0.01)
    except ReadTimeout as error:
        assert error.partial
    else:
        raise AssertionError("expected mid-frame ReadTimeout")
    server_sink.close()
    client_source.close()

    interleaved = (
        server_frame(1, compact_json({"method": "turn/started", "params": {"threadId": "t"}}))
        + server_frame(1, compact_json({"id": 99, "result": {"old": True}}))
        + server_frame(1, compact_json({"id": 1, "result": {"answer": True}}))
    )
    pipe, server_sink, client_source = fake_pipe_session(interleaved)
    assert pipe.request("test", {}) == {"answer": True}
    assert pipe.notifications[0]["method"] == "turn/started"
    assert pipe.unmatched_replies == [{"id": 99, "result": {"old": True}}]
    server_sink.close()
    client_source.close()
    fake = FakeSession([
        {"data": [{"id": "picker", "model": "native-a"}], "nextCursor": "two"},
        {"data": [{"id": "picker-b", "model": "native-b"}], "nextCursor": None},
    ])
    assert [item["model"] for item in model_catalog(fake)["models"]] == ["native-a", "native-b"]
    fake = FakeSession([
        {"thread": {"id": "thread", "status": "idle"}},
        {"data": [{"id": "new", "items": []}], "nextCursor": "older"},
        {"data": [{"id": "old", "items": [{"type": "userMessage", "content": [
            {"type": "text", "text": "kept"}]}]}], "nextCursor": None},
    ])
    history = full_history(fake, "thread")
    assert [turn["id"] for turn in history["turns"]] == ["old", "new"]
    assert history["userMessageContent"][0]["content"][0]["text"] == "kept"
    good = {"method": "thread/tokenUsage/updated", "params": {
        "threadId": "thread", "turnId": "turn", "tokenUsage": {
            "last": {"inputTokens": 17}, "total": {"inputTokens": 999}}}}
    assert relevant_notification(good, "thread")["signal"] == {
        "turnId": "turn", "lastInputTokens": 17}
    bad = json.loads(json.dumps(good))
    bad["params"]["threadId"] = "another"
    assert relevant_notification(bad, "thread") is None
    malformed = json.loads(json.dumps(good))
    malformed["params"]["tokenUsage"] = None
    expect_error("tokenUsage", lambda: relevant_notification(malformed, "thread"))
    fake = FakeSession([
        {"thread": {"id": "another"}},
        {"data": [], "nextCursor": None},
    ])
    expect_error("another thread", lambda: full_history(fake, "thread"))
    fake = FakeSession([
        {"thread": {"id": "thread"}},
        {"data": [{"id": "broken"}], "nextCursor": None},
    ])
    expect_error("no items array", lambda: full_history(fake, "thread"))

    class BadResume:
        notifications: list[dict[str, Any]] = []
        prior_notifications: list[dict[str, Any]] = []

        def begin_observation(self) -> None:
            pass

        def request(self, _method: str, _params: Any) -> dict[str, Any]:
            return {}

    expect_error("result.thread", lambda: observe(
        BadResume(), "thread", 0.01, io.BytesIO()))
    print("self-test: ok")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--self-test", action="store_true", help="run fixture-only offline checks")
    result.add_argument("--codex", help="Codex executable that provides app-server proxy")
    result.add_argument("--private-root", help="owner-only capsule root containing the socket/log")
    result.add_argument("--socket", help="private app-server control socket under --private-root")
    result.add_argument("--timeout", type=float, default=10.0, help="per-exchange seconds (default 10)")
    commands = result.add_subparsers(dest="command")
    commands.add_parser("models", help="print the complete native model catalog as JSON")
    history = commands.add_parser("history", help="print full chronological exact-thread history as JSON")
    history.add_argument("--thread-id", required=True)
    watching = commands.add_parser("observe", help="subscribe and record exact-thread token/turn notifications")
    watching.add_argument("--thread-id", required=True)
    watching.add_argument("--duration", required=True, type=float)
    watching.add_argument("--log", required=True, help="new private JSONL path, absolute or under root")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.self_test:
        if args.command or args.codex or args.private_root or args.socket:
            raise ObserverError("--self-test accepts no live proxy options or command")
        self_test()
        return 0
    if not args.command:
        raise ObserverError("a command is required")
    if not args.codex or not args.private_root or not args.socket:
        raise ObserverError("--codex, --private-root and --socket are required")
    root, socket_path = observer_paths(args.private_root, args.socket)
    with ProxySession(args.codex, socket_path, args.timeout) as session:
        session.initialize()
        if args.command == "models":
            print(json.dumps(model_catalog(session), ensure_ascii=False, indent=2))
        elif args.command == "history":
            print(json.dumps(full_history(session, args.thread_id), ensure_ascii=False, indent=2))
        elif args.command == "observe":
            log_path, output = private_log(root, args.log)
            try:
                summary = observe(session, args.thread_id, args.duration, output)
            finally:
                output.close()
            print(json.dumps({**summary, "path": str(log_path)}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ObserverError as error:
        print(f"codex-observer: {error}", file=sys.stderr)
        raise SystemExit(2)
