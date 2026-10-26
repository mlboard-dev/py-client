"""Config-only exact JSON numbers; scalar/context validation remains binary64.

An exact token is neither a Python string nor a float. Standard json.dumps will
reject it; this module's serializer emits its validated JSON number verbatim.
"""
from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
import json
import math
import re

_NUMBER = re.compile(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z', re.ASCII)
# Python permits configuring its decimal int conversion limit down to 640
# digits. Larger config tokens stay lexical, independent of that global setting.
_BUILTIN_INT_DIGITS = 640

@dataclass(frozen=True)
class JSONNumber:
    raw: str
    def __post_init__(self):
        if type(self.raw) is not str or len(self.raw) > 65536 or not _NUMBER.fullmatch(self.raw):
            raise ValueError('invalid exact JSON number')
    def __str__(self):
        return self.raw
    def __float__(self):
        raise TypeError('exact config number requires an explicit decimal conversion')
    def decimal(self):
        return Decimal(self.raw)


def loads_config_json(raw, *, fields=False):
    if len(raw.encode('utf-8') if isinstance(raw, str) else raw) > 16 * 1024**2:
        raise ValueError('config response exceeds lossless byte budget')
    value = json.loads(raw, parse_int=JSONNumber, parse_float=JSONNumber)
    # Iterative conversion retains tokens only in config fields, not metrics.
    root = [value]
    stack = [(root, 0, not fields)]
    nodes = 0
    while stack:
        parent, key, exact = stack.pop()
        nodes += 1
        if nodes > 1000000:
            raise ValueError('config response exceeds lossless node budget')
        item = parent[key]
        if isinstance(item, JSONNumber):
            if exact:
                if (len(item.raw.lstrip('-')) <= _BUILTIN_INT_DIGITS
                        and re.fullmatch(r'-?(?:0|[1-9][0-9]*)', item.raw) and item.raw != '-0'):
                    parent[key] = int(item.raw)
            else:
                parent[key] = float(item.raw) if any(c in item.raw for c in '.eE') else int(item.raw)
        elif type(item) is dict:
            for child in item:
                stack.append((item, child, exact or (fields and child == 'config')))
        elif type(item) is list:
            for index in range(len(item)):
                stack.append((item, index, exact))
    return root[0]


def dumps_config_json(value):
    pieces, stack, seen = [], [('value', value)], set()
    nodes = 0
    while stack:
        kind, item = stack.pop()
        if kind == 'raw':
            pieces.append(item)
            continue
        if kind == 'exit':
            seen.remove(item)
            continue
        nodes += 1
        if nodes > 1000000:
            raise ValueError('JSON serialization exceeds node budget')
        if isinstance(item, JSONNumber):
            pieces.append(item.raw)
        elif type(item) in (dict, list, tuple):
            identity = id(item)
            if identity in seen:
                raise ValueError('circular JSON value')
            seen.add(identity)
            stack.append(('exit', identity))
            array = type(item) is not dict
            if not array and any(type(key) is not str for key in item):
                raise TypeError('JSON object keys must be strings')
            entries = list(enumerate(item)) if array else sorted(item.items())
            pieces.append('[' if array else '{')
            stack.append(('raw', ']' if array else '}'))
            for index in range(len(entries) - 1, -1, -1):
                key, child = entries[index]
                stack.append(('value', child))
                if not array:
                    stack.append(('raw', json.dumps(key, ensure_ascii=False) + ':'))
                if index:
                    stack.append(('raw', ','))
        else:
            pieces.append(json.dumps(item, ensure_ascii=False, allow_nan=False, separators=(',', ':')))
    return ''.join(pieces)


def config_value(value):
    # Detached bounded copy; only config gets exact integers/Decimal support.
    holder = [None]
    stack = [(holder, 0, value, 0)]
    nodes = 0
    while stack:
        parent, key, item, depth = stack.pop()
        nodes += 1
        if depth > 20 or nodes + len(stack) > 4096:
            raise ValueError('config exceeds depth 20 or 4096 JSON nodes')
        typ = type(item)
        if typ is dict:
            if nodes + len(stack) + len(item) > 4096:
                raise ValueError("config exceeds 4096 JSON nodes")
            result = {}
            if any(type(k) is not str for k in item):
                raise ValueError('config keys must be strings')
            for child, val in item.items():
                stack.append((result, child, val, depth + 1))
            parent[key] = result
        elif typ in (list, tuple):
            if nodes + len(stack) + len(item) > 4096:
                raise ValueError("config exceeds 4096 JSON nodes")
            result = [None] * len(item)
            for index, val in enumerate(item):
                stack.append((result, index, val, depth + 1))
            parent[key] = result
        elif typ is Decimal:
            if not item.is_finite():
                raise ValueError('config decimal must be finite')
            parent[key] = JSONNumber(str(item))
        elif isinstance(item, JSONNumber) or item is None or typ in (str, bool, int):
            parent[key] = item
        elif typ is float and math.isfinite(item):
            parent[key] = item
        else:
            raise ValueError('config requires built-in JSON values or explicit exact decimals')
    value = holder[0]
    if type(value) is not dict:
        raise ValueError('config must be an object')
    if len(dumps_config_json(value).encode('utf-8')) > 65536:
        raise ValueError('config exceeds 64 KiB')
    return value
