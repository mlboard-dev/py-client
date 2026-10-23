"""Bounded framed loopback IPC and detached collector discovery."""
from __future__ import annotations
import contextlib
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import threading
import time

from .validation import dumps

MAX_FRAME = 256 * 1024


def frame(value):
    body = dumps(value).encode()
    if len(body) > MAX_FRAME:
        raise ValueError("IPC frame too large")
    return struct.pack("!I",len(body)) + body


def extract(buffer):
    result = []
    while len(buffer) >= 4 and len(result) < 128:
        length = struct.unpack("!I",buffer[:4])[0]
        if length > MAX_FRAME:
            raise ValueError("IPC frame too large")
        if len(buffer) < length + 4:
            break
        result.append(json.loads(buffer[4:4+length]))
        del buffer[:4+length]
    return result


def connect(root, *, start=True, timeout=5):
    root = Path(root).resolve()
    deadline, launched, last_launch = time.monotonic()+timeout, False, 0.0
    while time.monotonic() < deadline:
        sock = None
        try:
            endpoint = json.loads((root/"collector.json").read_text())
            sock = socket.create_connection(("127.0.0.1",int(endpoint["port"])),timeout=max(.001,min(.2,deadline-time.monotonic())))
            sock.sendall(frame({"kind":"auth","secret":endpoint["secret"]}))
            sock.setblocking(False)
            return sock
        except (OSError,ValueError,KeyError):
            if sock:
                sock.close()
        if start and (not launched or time.monotonic()-last_launch > 1):
            root.mkdir(parents=True,exist_ok=True)
            diagnostics = root/"diagnostics"
            diagnostics.mkdir(exist_ok=True)
            log = diagnostics/"collector.log"
            if log.exists() and log.stat().st_size > 16384:
                log.unlink()
            env = os.environ.copy()
            package_root = str(Path(__file__).resolve().parent.parent)
            env["PYTHONPATH"] = package_root + os.pathsep + env.get("PYTHONPATH","")
            with log.open("ab") as stderr:
                process = subprocess.Popen([sys.executable,"-m","mlboard._collector","--root",str(root)],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=stderr,close_fds=True,start_new_session=True,env=env)
            threading.Thread(target=process.wait,daemon=True,name="mlboard-collector-reaper").start()
            launched, last_launch = True,time.monotonic()
        time.sleep(min(.04,max(0,deadline-time.monotonic())))
    raise TimeoutError("collector unavailable; queued memory is not durable")


def command(root, message, *, timeout=10):
    deadline = time.monotonic()+timeout
    sock = connect(root,timeout=min(timeout,5))
    try:
        sock.settimeout(max(.001,deadline-time.monotonic()))
        message = {**message,"id":"cli"}
        sock.sendall(frame(message))
        buffer = bytearray()
        while time.monotonic()<deadline:
            sock.settimeout(max(.001,deadline-time.monotonic()))
            chunk = sock.recv(65536)
            if not chunk:
                raise OSError("collector disconnected")
            buffer.extend(chunk)
            for reply in extract(buffer):
                if reply.get("id") == "cli":
                    if not reply.get("ok"):
                        raise RuntimeError(reply.get("error","collector failed"))
                    return reply
        raise TimeoutError("collector command timed out")
    finally:
        sock.close()
