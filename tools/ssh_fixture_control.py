"""Loopback-only test control and method-only RPC audit (never logs prompt text)."""

from __future__ import annotations

import json
import socket
import threading
from typing import Any


class FixtureControl:
    def __init__(self, port: int | None, events: Any) -> None:
        self.events = events
        self.transports: set[Any] = set()
        self.lock = threading.Lock()
        if port is not None:
            listener = socket.socket()
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", port))
            listener.listen(4)
            threading.Thread(target=self.serve, args=(listener,), daemon=True).start()

    def add(self, transport: Any) -> None:
        with self.lock:
            self.transports.add(transport)

    def remove(self, transport: Any) -> None:
        with self.lock:
            self.transports.discard(transport)

    def serve(self, listener: socket.socket) -> None:
        while True:
            client, _ = listener.accept()
            with client:
                client.settimeout(5)
                command = client.recv(64).strip()
                if command == b"DROP":
                    with self.lock:
                        transports = tuple(self.transports)
                    for transport in transports:
                        transport.close()
                    self.events.write("forced_transport_loss", count=len(transports))
                    client.sendall(b"OK\n")
                else:
                    client.sendall(b"UNKNOWN\n")


class ClientRpcAudit:
    def __init__(self, events: Any, channel_id: int) -> None:
        self.events = events
        self.channel_id = channel_id
        self.data = bytearray()
        self.upgraded = False

    def receive(self, chunk: bytes) -> None:
        self.data.extend(chunk)
        if not self.upgraded:
            boundary = self.data.find(b"\r\n\r\n")
            if boundary < 0:
                return
            del self.data[: boundary + 4]
            self.upgraded = True
        while len(self.data) >= 2:
            opcode = self.data[0] & 15
            length = self.data[1] & 127
            offset = 2
            if length in (126, 127):
                size = 2 if length == 126 else 8
                if len(self.data) < offset + size:
                    return
                length = int.from_bytes(self.data[offset : offset + size], "big")
                offset += size
            masked = self.data[1] & 128
            if masked:
                offset += 4
            if len(self.data) < offset + length:
                return
            payload = bytes(self.data[offset : offset + length])
            if masked:
                mask = self.data[offset - 4 : offset]
                payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
            del self.data[: offset + length]
            if opcode == 1:
                try:
                    message = json.loads(payload)
                    self.events.write("client_rpc", channel_id=self.channel_id,
                                      method=message.get("method", "approval_response"))
                except (ValueError, AttributeError):
                    self.events.write("client_rpc_invalid", channel_id=self.channel_id)
