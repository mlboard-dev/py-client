"""Bounded built-in CPU value validation. Never invoke user tensor conversions."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import re
import unicodedata
import urllib.parse
import ipaddress

MAX_INT = (1 << 63) - 1
MAX_EVENT = 64 * 1024
MAX_FIELDS = 128


class ValidationError(ValueError):
    pass


def integer(value, name="integer"):
    if type(value) is not int or not 0 <= value <= MAX_INT:
        raise ValidationError(f"{name} must be a nonnegative int63")
    return str(value)


def text(value, name="text", limit=8192):
    if type(value) is not str or len(value) > limit or len(value.encode("utf-8")) > limit:
        raise ValidationError(f"{name} must be a string of at most {limit} UTF-8 bytes")
    return value


def run_path(value):
    text(value, "run_path", 1024)
    if unicodedata.normalize("NFC", value) != value or "\\" in value or any(unicodedata.category(c)=="Cc" for c in value):
        raise ValidationError("run_path must be NFC and use forward slashes")
    pieces = value.split("/")
    if len(pieces) > 16 or any(not p or p in (".", "..") or p.strip() != p or len(p.encode()) > 128 for p in pieces):
        raise ValidationError("run_path contains an invalid path segment")
    return value


def json_value(value, *, budget=32768, max_depth=6):
    remaining = [budget]
    def visit(v, depth):
        if depth > max_depth:
            raise ValidationError("JSON nesting limit exceeded")
        typ = type(v)
        remaining[0] -= 8
        if typ is str:
            text(v, limit=min(remaining[0], 16384))
            remaining[0] -= len(v.encode())
        elif v is None or typ is bool:
            pass
        elif typ is int:
            if abs(v) > 9007199254740991:
                raise ValidationError("JSON integer exceeds exact double range")
        elif typ is float:
            if not math.isfinite(v):
                raise ValidationError("JSON values must be finite")
        elif typ in (list, tuple):
            if len(v) > 2048:
                raise ValidationError("JSON array limit exceeded")
            v = [visit(item, depth + 1) for item in v]
        elif typ is dict:
            if len(v) > 256:
                raise ValidationError("JSON object limit exceeded")
            result = {}
            for key,item in v.items():
                key = text(key,"JSON key",512)
                remaining[0] -= len(key.encode("utf-8")) + 4
                if remaining[0]<0:
                    raise ValidationError("JSON size limit exceeded")
                result[key] = visit(item,depth+1)
            v = result
        else:
            raise ValidationError("only built-in CPU JSON values are supported")
        if remaining[0] < 0:
            raise ValidationError("JSON size limit exceeded")
        return v
    return visit(value, 0)


def scalar(value):
    if type(value) not in (float, int):
        raise ValidationError("scalar must be a built-in int/float; explicitly convert CPU tensors")
    if type(value) is int and abs(value) > 9007199254740991:
        raise ValidationError("scalar exceeds exact double range")
    value = float(value)
    if math.isnan(value):
        return {"value": None, "value_state": "nan"}
    if math.isinf(value):
        return {"value": None, "value_state": "pos_inf" if value > 0 else "neg_inf"}
    return {"value": value, "value_state": "finite"}


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def dumps(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)


def digest(payload):
    # Versioned local identity hash: identical normalized stored bytes are resent.
    # Server independently canonicalizes the complete event for its protocol hash.
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def check_name(name):
    text(name,"metric name",256)
    if not name:
        raise ValidationError("metric name must be nonempty")
    return name


def normalize_server(value):
    """Canonical credential target: scheme, host, default port and base path."""
    text(value,"server URL",4096)
    if any(ord(c)<=32 or ord(c)==127 for c in value) or "\\" in value:
        raise ValidationError("server URL contains whitespace/control/backslash")
    parsed=urllib.parse.urlsplit(value)
    scheme=parsed.scheme.lower()
    if scheme not in ("http","https") or not parsed.hostname or parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
        raise ValidationError("server must be an http(s) URL without credentials/query/fragment")
    try:
        port=parsed.port
        host=parsed.hostname.encode("idna").decode("ascii").lower()
        try:
            address=ipaddress.ip_address(host)
            host='['+address.compressed+']' if address.version==6 else address.compressed
        except ValueError:
            if not re.fullmatch(r"[a-z0-9.-]+",host):
                raise ValidationError("invalid server hostname")
    except (ValueError,UnicodeError):
        raise ValidationError("invalid server host/port") from None
    if port is not None and not 1<=port<=65535:
        raise ValidationError("invalid server port")
    suffix='' if port is None or port==({"http":80,"https":443}[scheme]) else ':'+str(port)
    path=parsed.path.rstrip('/')
    if any(ord(c)<=32 or ord(c)==127 for c in urllib.parse.unquote(path)):
        raise ValidationError("invalid server base path")
    return scheme+'://'+host+suffix+path
