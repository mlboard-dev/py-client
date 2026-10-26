"""Finite reads for the local viewer, including the unauthenticated first header."""
from __future__ import annotations

from dataclasses import dataclass
import io
import math
import time


@dataclass(frozen=True)
class ViewerLimits:
    connections: int = 32
    header_bytes: int = 32768
    header_seconds: float = 5
    idle_seconds: float = 5
    body_seconds: float = 10

    def __post_init__(self):
        if type(self.connections) is not int or not 1 <= self.connections <= 128:
            raise ValueError("viewer connections must be 1..128")
        if type(self.header_bytes) is not int or not 256 <= self.header_bytes <= 65536:
            raise ValueError("viewer header bytes must be 256..65536")
        for value in (self.header_seconds, self.idle_seconds, self.body_seconds):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 < value <= 300:
                raise ValueError("viewer read deadlines must be finite and within 0..300 seconds")


class HeaderLimitError(ValueError):
    pass


class DeadlineSocketReader(io.RawIOBase):
    def __init__(self, connection, idle_seconds):
        super().__init__()
        self.connection = connection
        self.idle_seconds = idle_seconds
        self.deadline = None

    def readable(self):
        return True

    def check_deadline(self):
        remaining = self.idle_seconds
        if self.deadline is not None:
            remaining = min(remaining, self.deadline - time.monotonic())
            if remaining <= 0:
                raise TimeoutError("viewer request read deadline exceeded")
        self.connection.settimeout(remaining)

    def readinto(self, target):
        self.check_deadline()
        return self.connection.recv_into(target)


class BoundedRequestReader(io.BufferedReader):
    def __init__(self, connection, limits):
        super().__init__(DeadlineSocketReader(connection, limits.idle_seconds))
        self.limits = limits
        self.header_remaining = None

    def begin_headers(self):
        self.raw.deadline = time.monotonic() + self.limits.header_seconds
        self.header_remaining = self.limits.header_bytes

    def finish_read(self):
        self.header_remaining = None
        self.raw.deadline = None
        self.raw.connection.settimeout(self.limits.idle_seconds)

    def begin_body(self):
        self.raw.deadline = time.monotonic() + self.limits.body_seconds

    def readline(self, size=-1):
        if self.header_remaining is None:
            return super().readline(size)
        self.raw.check_deadline()
        maximum = self.header_remaining + 1
        line = super().readline(min(size, maximum) if size >= 0 else maximum)
        self.header_remaining -= len(line)
        if self.header_remaining < 0:
            raise HeaderLimitError("request headers exceed byte limit")
        return line
