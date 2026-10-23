"""Incremental CRC-checked TensorBoard scalar importer (optional protobuf dep).

Only complete TFRecords advance offsets, atomically with their imported events.
The original event files are never modified. Histogram/image/audio/graph/tensor
arrays are counted as unsupported; they are never mislabelled as scalars.
"""
from __future__ import annotations
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import struct
import uuid

from .ipc import command, frame
from .store import readonly
from .validation import dumps, integer, run_path, scalar


def crc32c(data):
    crc = 0xffffffff
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc>>1) ^ (0x82f63b78 if crc&1 else 0)
    return crc ^ 0xffffffff


def masked_crc(data):
    crc = crc32c(data)
    return (((crc>>15)|(crc<<17)) + 0xa282ead8) & 0xffffffff


def records(source, offset=0):
    """Yield (start,next_offset,bytes). A partial final frame is left for watch."""
    with Path(source).open('rb') as stream:
        stream.seek(offset)
        while True:
            start = stream.tell()
            header = stream.read(12)
            if len(header)<12:
                return
            length, checksum = struct.unpack('<QI',header)
            if masked_crc(header[:8])!=checksum:
                raise ValueError(f"TensorBoard length CRC mismatch at offset {start}")
            if length>1024*1024:
                raise ValueError(f"TensorBoard record exceeds 1 MiB at offset {start}")
            payload = stream.read(length)
            footer = stream.read(4)
            if len(payload)!=length or len(footer)!=4:
                return
            if masked_crc(payload)!=struct.unpack('<I',footer)[0]:
                raise ValueError(f"TensorBoard data CRC mismatch at offset {start}")
            yield start,stream.tell(),payload


def scalar_tensor(tensor):
    if tensor.tensor_shape.unknown_rank or any(d.size!=1 for d in tensor.tensor_shape.dim):
        return None
    types = {1:('f','float_val'),2:('d','double_val'),3:('i','int_val'),9:('q','int64_val'),22:('I','uint32_val'),23:('Q','uint64_val')}
    if tensor.dtype not in types:
        return None
    fmt,name = types[tensor.dtype]
    if tensor.tensor_content:
        size = struct.calcsize('<'+fmt)
        if len(tensor.tensor_content)!=size:
            return None
        return struct.unpack('<'+fmt,tensor.tensor_content)[0]
    values = getattr(tensor,name)
    return values[0] if len(values)==1 else None


def import_directory(source, root, *, project='tensorboard'):
    try:
        from tensorboard.compat.proto.event_pb2 import Event
    except ImportError:
        raise ImportError("TensorBoard import needs its optional dependency: install 'mlboard[tensorboard]' into your project virtual environment") from None
    source = Path(source).resolve()
    if not source.is_dir():
        raise ValueError("TensorBoard source must be an existing directory")
    files = sorted(source.rglob('events.out.tfevents.*'))
    if len(files)>10000:
        raise ValueError("TensorBoard import exceeds 10000 files; select a smaller directory")
    offsets = {}
    if (Path(root)/'spool.sqlite3').exists():
        with readonly(root) as db:
            offsets = {r['source']:(r['identity'],r['offset']) for r in db.execute('SELECT * FROM import_offsets')}
    imported,skipped,complete_frames = 0,0,0
    for file in files:
        info = file.stat()
        identity = f'{info.st_dev}:{info.st_ino}'
        previous,offset = offsets.get(str(file),(identity,0))
        if previous!=identity or info.st_size<offset:
            raise ValueError("TensorBoard source was replaced/truncated; choose a new import store")
        parent = file.parent
        relative = parent.relative_to(source).as_posix()
        path = source.name if relative=='.' else relative
        run_path(path)
        rid = str(uuid.uuid5(uuid.NAMESPACE_URL,'mlboard:tb:'+project+':'+str(parent)))
        sid = str(uuid.uuid5(uuid.UUID(rid),identity+':'+file.name))
        pid = 'tb-'+hashlib.sha256((identity+file.name).encode()).hexdigest()[:16]
        manifest = {'project':project,'run_path':path,'name':parent.name,'run_id':rid,'session_id':sid,'session_ordinal':None,'producer_id':pid,'primary_producer':pid,'token':str(uuid.uuid5(uuid.UUID(sid),'importer')),'config':{'source':'tensorboard','source_directory':str(parent)},'tags':['tensorboard-import'],'sync':'off','retention':'record'}
        events,batch_start,batch_end = [],offset,offset
        frames_in_batch = 0
        def flush():
            nonlocal events,batch_start,frames_in_batch
            if not frames_in_batch:
                return
            command(root,{'kind':'import_batch','manifest':manifest,'source':str(file),'identity':identity,'expected_offset':batch_start,'next_offset':batch_end,'events':events},timeout=10)
            events = []
            batch_start = batch_end
            frames_in_batch = 0
        for start,end,payload in records(file,offset):
            proto = Event()
            try:
                proto.ParseFromString(payload)
            except Exception:
                raise ValueError(f'invalid TensorBoard protobuf at offset {start}') from None
            fields = []
            for value in proto.summary.value:
                tag = value.tag
                if not tag or len(tag.encode())>256:
                    skipped += 1
                    continue
                number = None
                if value.HasField('simple_value'):
                    number = value.simple_value
                elif value.HasField('tensor') and value.metadata.plugin_data.plugin_name in ('scalars',''):
                    number = scalar_tensor(value.tensor)
                if number is None:
                    skipped += 1
                    continue
                fields.append({'name':tag,'kind':'scalar','context':{},**scalar(number)})
            if fields:
                if proto.step<0 or not math.isfinite(proto.wall_time):
                    raise ValueError(f'invalid TensorBoard step/time at offset {start}')
                when = dt.datetime.fromtimestamp(proto.wall_time,dt.timezone.utc).isoformat(timespec='microseconds').replace('+00:00','Z')
                # Last duplicate tag in a single source event has explicit precedence.
                fields = list({f['name']:f for f in fields}.values())
                for index in range(0,len(fields),128):
                    event = {'schema_version':'mlboard.event/v1','run_id':rid,'session_id':sid,'producer_id':pid,'seq':integer((start<<8)+(index//128),'seq'),'event_time':when,'step':integer(proto.step,'step'),'fields':fields[index:index+128]}
                    if len(dumps(event).encode())>65536:
                        raise ValueError('imported TensorBoard event exceeds 64 KiB; shorten tag names')
                    events.append(event)
                    imported += 1
            batch_end = end
            complete_frames += 1
            frames_in_batch += 1
            if frames_in_batch>=32 or sum(len(dumps(e)) for e in events)>96*1024:
                flush()
        flush()
    return {'files':len(files),'complete_frames':complete_frames,'imported_events':imported,'unsupported_values':skipped,'compatibility':'simple_value and scalar numeric TensorProto only; unsupported values skipped explicitly'}
