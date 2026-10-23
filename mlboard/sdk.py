"""Training-first SDK. log() does no network, filesystem I/O or tensor conversion."""
from __future__ import annotations

from collections import OrderedDict, deque
from dataclasses import asdict, dataclass
import json
import math
import os
from pathlib import Path
import select
import socket
import threading
import time
import uuid

from .ipc import command, connect, extract, frame
from .validation import MAX_EVENT, MAX_FIELDS, ValidationError, check_name, dumps, integer, json_value, now, run_path as validate_path, scalar, text, normalize_server


@dataclass(frozen=True)
class LogReceipt:
    accepted: bool
    seq: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class FinishReceipt:
    complete: bool
    wait_for: str
    accepted: int
    durable: int
    dropped: int
    reason: str | None = None


class Run:
    def __init__(self, project, run_path, config=None, tags=None, *, spool_dir="./outputs/mlboard-runs", sync="off", server=None, api_key=None, mode="safe", retention="record", run_id=None, session_id=None, session_ordinal=None, producer_id="rank0", primary_producer="rank0", name=None, create_project=False, queue_events=512, queue_bytes=8*1024**2, elapsed_offset=None, _start_sender=True):
        text(project,"project",256)
        validate_path(run_path)
        if mode not in ("safe","strict") or sync not in ("off","background") or retention not in ("record","cache"):
            raise ValidationError("invalid mode, sync or retention")
        if sync == "off" and retention == "cache":
            raise ValidationError("retention=cache requires sync=background")
        if type(queue_events) is not int or not 1 <= queue_events <= 65536 or type(queue_bytes) is not int or not 1024 <= queue_bytes <= 64*1024**2:
            raise ValidationError("queue limits must be bounded positive integers")
        self.id = str(uuid.UUID(run_id)) if run_id else str(uuid.uuid4())
        self.run_id = self.id
        self.session_id = str(uuid.UUID(session_id)) if session_id else str(uuid.uuid4())
        self.producer_id = text(producer_id,"producer_id",128)
        text(primary_producer,"primary_producer",128)
        if session_ordinal is not None:
            integer(session_ordinal,"session_ordinal")
        if elapsed_offset is not None and (type(elapsed_offset) not in (float,int) or not math.isfinite(elapsed_offset) or elapsed_offset < 0):
            raise ValidationError("elapsed_offset must be finite and nonnegative")
        self.mode, self.sync, self.retention = mode,sync,retention
        self.spool_dir = str(Path(spool_dir).absolute())
        self._pid, self._started = os.getpid(),time.monotonic()
        self._resumed = run_id is not None
        self._elapsed_offset = elapsed_offset
        self._queue = deque()
        self._bytes = 0
        self._max_events,self._max_bytes = queue_events,queue_bytes
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._finished = False
        self._finish_frame = None
        self._finish_ack = threading.Event()
        self._seq = 0
        self._accepted = self._dropped = self._durable = 0
        self._rejected = 0
        self._snapshot = {"collector_ready":False,"updated_at":None,"sync":"off" if sync=="off" else "pending"}
        self._snapshot_at = 0.0
        self._last_error = None
        self._wait_remote = False
        self._register = {"kind":"register","id":"register","token":uuid.uuid4().hex,"run_id":self.id,"session_id":self.session_id,"session_ordinal":session_ordinal,"producer_id":producer_id,"primary_producer":primary_producer,"project":project,"run_path":run_path,"config":json_value(config or {}),"tags":json_value(tags or []),"name":text(name or run_path.split('/')[-1],"name",200),"retention":retention,"sync":sync,"server":normalize_server(server or os.environ.get("MLBOARD_SERVER","http://127.0.0.1:8080")),"api_key":api_key or os.environ.get("MLBOARD_API_KEY"),"create_project":bool(create_project)}
        if type(self._register["config"]) is not dict or type(self._register["tags"]) is not list or any(type(t) is not str for t in self._register["tags"]):
            raise ValidationError("config must be an object; tags must be a list of strings")
        self._register_frame = frame(self._register)
        if _start_sender:
            threading.Thread(target=self._sender,daemon=True,name="mlboard-ipc").start()

    def _failure(self, exc):
        if not getattr(exc,"_mlboard_drop_counted",False):
            self._dropped += 1
            exc._mlboard_drop_counted = True
        if self.mode == "strict":
            raise exc
        return LogReceipt(False,reason=str(exc)[:256])

    def _enqueue(self, fields, *, step=None, epoch=None, elapsed_seconds=None, processed_samples=None, context=None, logs=None, media=None):
        if os.getpid() != self._pid:
            return self._failure(ValidationError("Run cannot be reused after fork; initialize a new producer"))
        if self._finished:
            return self._failure(ValidationError("run is finished"))
        try:
            if context is not None:
                context = json_value(context,budget=2048,max_depth=3)
                if type(context) is not dict:
                    raise ValidationError("context must be an object")
                if any(type(v) not in (str,int,float,bool,type(None)) for v in context.values()):
                    raise ValidationError("context values must be primitive JSON values")
            if len(fields)>MAX_FIELDS:
                raise ValidationError("at most 128 fields per event")
            checked = []
            identities = set()
            for field in fields:
                f = dict(field)
                check_name(f["name"])
                f["context"] = context or f.get("context",{})
                identity = (f["name"],f["kind"],dumps(f["context"]))
                if identity in identities:
                    raise ValidationError("duplicate field in event")
                identities.add(identity)
                checked.append(f)
            elapsed = time.monotonic()-self._started
            event = {"schema_version":"mlboard.event/v1","run_id":self.id,"session_id":self.session_id,"producer_id":self.producer_id,"event_time":now(),"session_elapsed_seconds":elapsed,"fields":checked}
            if elapsed_seconds is not None:
                if type(elapsed_seconds) not in (int,float) or not math.isfinite(elapsed_seconds) or elapsed_seconds<0:
                    raise ValidationError("elapsed_seconds must be finite and nonnegative")
                event["elapsed_seconds"] = elapsed_seconds
            elif self._elapsed_offset is not None:
                event["elapsed_seconds"] = self._elapsed_offset+elapsed
            elif self._register.get("session_ordinal") in (None,0) and not self._register.get("resume"):
                # Assigned session ordinal may be >0 on explicit run_id resume;
                # do not invent a cumulative offset for those sessions.
                if not getattr(self,"_resumed",False):
                    event["elapsed_seconds"] = elapsed
            if step is not None:
                event["step"] = integer(step,"step")
            if epoch is not None:
                if type(epoch) not in (int,float) or not math.isfinite(epoch) or epoch<0:
                    raise ValidationError("epoch must be finite and nonnegative")
                event["epoch"] = epoch
            if processed_samples is not None:
                event["processed_samples"] = integer(processed_samples,"processed_samples")
            if logs:
                event["logs"] = logs
            if not self._lock.acquire(blocking=False):
                return self._failure(ValidationError("queue_busy"))
            try:
                if self._finished:
                    return self._failure(ValidationError("run is finished"))
                seq = self._seq
                self._seq += 1
                event["seq"] = integer(seq,"seq")
                body = dumps(event).encode()
                if len(body)>MAX_EVENT:
                    raise ValidationError("event exceeds 64 KiB")
                message = {"kind":"media" if media else "event","id":f"event-{seq}","event":event}
                if media:
                    message["media"] = media
                encoded = frame(message)
                if len(self._queue)>=self._max_events or self._bytes+len(encoded)>self._max_bytes:
                    return self._failure(ValidationError("queue_full: drop_newest"))
                self._queue.append((message["id"],encoded))
                self._bytes += len(encoded)
                self._accepted += 1
            finally:
                self._lock.release()
            self._wake.set()
            return LogReceipt(True,seq=str(seq))
        except (ValueError,TypeError,KeyError,OverflowError) as exc:
            return self._failure(exc)

    def log(self, values, *, step=None, epoch=None, context=None, elapsed_seconds=None, processed_samples=None):
        try:
            if type(values) is not dict or not values or len(values)>MAX_FIELDS:
                raise ValidationError("log requires 1–128 named CPU scalar values")
            fields = [{"name":check_name(k),"kind":"scalar",**scalar(v)} for k,v in values.items()]
        except (ValueError,TypeError) as exc:
            return self._failure(exc)
        return self._enqueue(fields,step=step,epoch=epoch,context=context,elapsed_seconds=elapsed_seconds,processed_samples=processed_samples)

    def log_scalar(self, name, value, *, step=None, epoch=None, context=None, elapsed_seconds=None, processed_samples=None):
        """Record one metric with this call's coordinates; other metrics are independent."""
        try:
            check_name(name)
        except (ValueError,TypeError) as exc:
            return self._failure(exc)
        return self.log({name:value},step=step,epoch=epoch,context=context,elapsed_seconds=elapsed_seconds,processed_samples=processed_samples)

    def log_record(self, name, kind, payload, *, step=None, epoch=None, context=None):
        try:
            if kind not in ("histogram","confusion_matrix","curve","table","text","media"):
                raise ValidationError("unsupported structured kind")
            payload = json_value(payload,budget=48*1024)
        except (ValueError,TypeError) as exc:
            return self._failure(exc)
        return self._enqueue([{"name":name,"kind":kind,"payload":payload}],step=step,epoch=epoch,context=context)

    def log_text(self, name, value, **coordinates):
        try:
            payload = {"schema_version":"mlboard.text/v1","text":text(value,"text",16384)}
        except ValueError as exc:
            return self._failure(exc)
        return self.log_record(name,"text",payload,**coordinates)

    def log_message(self, message, level="info", *, step=None):
        try:
            if level not in ("debug","info","warning","error"):
                raise ValidationError("invalid log level")
            message = text(message,"log message",8192)
        except ValueError as exc:
            return self._failure(exc)
        return self._enqueue([],step=step,logs=[{"level":level,"message":message}])

    def log_histogram(self, name, *, bin_edges, counts, **coordinates):
        try:
            edges = json_value(bin_edges,budget=16384)
            frequencies = json_value(counts,budget=16384)
            if type(edges) is not list or type(frequencies) is not list or len(edges)!=len(frequencies)+1 or not frequencies:
                raise ValidationError("histogram needs N+1 edges and N counts")
            if any(type(x) not in (int,float) for x in edges) or any(a>=b for a,b in zip(edges,edges[1:])):
                raise ValidationError("histogram edges must increase")
            if any(type(c) is not int or c<0 for c in frequencies):
                raise ValidationError("histogram counts must be nonnegative integers")
            if coordinates.get("step") is None:
                raise ValidationError("native histogram requires step")
            step = integer(coordinates["step"],"step")
        except (ValueError,TypeError) as exc:
            return self._failure(exc)
        return self.log_record(name,"histogram",{"schema_version":"mlboard.histogram/v1","step":step,"edges":edges,"counts":frequencies,"underflow":0,"overflow":0,"sample_count":sum(frequencies),"bin_semantics":"left_closed_right_open_last_closed"},**coordinates)

    def log_confusion_matrix(self, name, matrix, *, labels=None, **coordinates):
        try:
            matrix = json_value(matrix,budget=32768)
            if type(matrix) is not list or not matrix or any(type(row) is not list or len(row)!=len(matrix) for row in matrix) or any(type(v) is not int or v<0 for row in matrix for v in row):
                raise ValidationError("confusion matrix must be a finite nonnegative square matrix")
            if labels is not None and (type(labels) is not list or len(labels)!=len(matrix) or any(type(v) is not str for v in labels)):
                raise ValidationError("labels must match matrix dimensions")
            if coordinates.get("step") is None:
                raise ValidationError("native confusion matrix requires step")
            step = integer(coordinates["step"],"step")
        except (ValueError,TypeError) as exc:
            return self._failure(exc)
        names = labels or [str(i) for i in range(len(matrix))]
        return self.log_record(name,"confusion_matrix",{"schema_version":"mlboard.confusion-matrix/v1","step":step,"labels":[{"class_id":str(i),"label":label} for i,label in enumerate(names)],"rows":"actual","columns":"predicted","counts":matrix,"sample_count":sum(sum(row) for row in matrix)},**coordinates)

    def log_threshold_curve(self, name, *, thresholds, tp, fp, fn, tn, positive_class_id="1", averaging="binary", **coordinates):
        """Explicit bounded CPU helper; derives rates/AUC from supplied confusion counts."""
        try:
            counts = [json_value(v,budget=8192) for v in (tp,fp,fn,tn)]
            n = len(counts[0])
            if not 2<=n<=512 or any(type(v) is not list or len(v)!=n or any(type(c) is not int or c<0 for c in v) for v in counts):
                raise ValidationError("curve requires 2..512 equal nonnegative integer count arrays")
            if type(thresholds) not in (list,tuple) or len(thresholds)!=n:
                raise ValidationError("thresholds must match counts")
            if averaging not in ("binary","micro","per_class"):
                raise ValidationError("count-based curves support only binary, micro or per_class averaging")
            threshold_values,states = [],[]
            previous = -math.inf
            for t in thresholds:
                if type(t) not in (float,int) or math.isnan(t) or t<previous:
                    raise ValidationError("thresholds must increase; NaN is invalid")
                previous = t
                threshold_values.append(t if math.isfinite(t) else None)
                states.append("finite" if math.isfinite(t) else "neg_inf" if t<0 else "pos_inf")
            tp,fp,fn,tn = counts
            ratio = lambda a,b: a/b if b else None
            fpr = [ratio(a,a+b) for a,b in zip(fp,tn)]
            tpr = [ratio(a,a+b) for a,b in zip(tp,fn)]
            precision = [ratio(a,a+b) for a,b in zip(tp,fp)]
            pairs = sorted((x,y) for x,y in zip(fpr,tpr) if x is not None and y is not None)
            auc = sum((b[0]-a[0])*(a[1]+b[1])/2 for a,b in zip(pairs,pairs[1:])) if len(pairs)>=2 else None
            if coordinates.get("step") is None:
                raise ValidationError("native threshold curve requires step")
            payload = {"schema_version":"mlboard.threshold-curve/v1","step":integer(coordinates["step"],"step"),"positive_class_id":text(positive_class_id,"positive_class_id",128),"averaging":averaging,"thresholds":threshold_values,"threshold_state":states,"tp":tp,"fp":fp,"fn":fn,"tn":tn,"fpr":fpr,"tpr":tpr,"precision":precision,"recall":tpr,"undefined_rates":"null","auc":{"value":auc,"method":"trapezoid_fpr_ascending","axis":"fpr_tpr"}}
        except (ValueError,TypeError,IndexError) as exc:
            return self._failure(exc)
        return self.log_record(name,"curve",payload,**coordinates)

    def log_roc(self, name, **counts):
        return self.log_threshold_curve(name,**counts)

    def log_pr(self, name, **counts):
        return self.log_threshold_curve(name,**counts)

    def log_table(self, name, columns, rows, **coordinates):
        try:
            columns,rows = json_value(columns,budget=4096),json_value(rows,budget=32768)
            if type(columns) is not list or any(type(c) is not str for c in columns) or not columns or type(rows) is not list or any(type(r) is not list or len(r)!=len(columns) for r in rows):
                raise ValidationError("table row widths must match named columns")
        except (ValueError,TypeError) as exc:
            return self._failure(exc)
        return self.log_record(name,"table",{"schema_version":"mlboard.table/v1","columns":columns,"rows":rows},**coordinates)

    def log_media(self, name, path, *, step=None, epoch=None, caption=None, kind="file", metadata=None, context=None):
        try:
            # Path construction does not stat/read/encode the referenced file.
            path = text(str(path),"media path",4096)
            metadata = json_value(metadata or {},budget=4096)
            if type(metadata) is not dict:
                raise ValidationError("media metadata must be an object")
            kind = text(kind,"media kind",64)
            aid = str(uuid.uuid4())
            payload = {"schema_version":"mlboard.media/v1","artifact_id":aid,"media_type":kind}
            if caption is not None:
                payload["caption"] = text(caption,"caption",4096)
            item = {"artifact_id":aid,"path":str(Path(path).absolute()),"name":check_name(name),"kind":kind,"metadata":metadata}
        except (ValueError,TypeError) as exc:
            return self._failure(exc)
        return self._enqueue([{"name":name,"kind":"media","payload":payload}],step=step,epoch=epoch,context=context,media=item)

    def status(self):
        return {**self._snapshot,"run_id":self.id,"session_id":self.session_id,"producer_id":self.producer_id,"accepted":self._accepted,"durable":self._durable,"dropped":self._dropped,"collector_rejected":self._rejected,"queued_events":len(self._queue),"queued_bytes":self._bytes,"stale":time.monotonic()-self._snapshot_at>2,"last_error":self._last_error}

    def finish(self, state="finished", timeout=2.0, wait_for="local"):
        if state not in ("finished","failed","stopped") or wait_for not in ("local","remote","none") or type(timeout) not in (int,float) or not math.isfinite(timeout) or timeout<0:
            raise ValidationError("finish requires a terminal state, finite timeout and local/remote/none checkpoint")
        deadline = time.monotonic()+timeout
        # A finite lock acquisition is permitted only in explicit finish().
        if not self._lock.acquire(timeout=max(0,deadline-time.monotonic())):
            return FinishReceipt(False,wait_for,self._accepted,self._durable,self._dropped,"finish queue busy")
        try:
            self._wait_remote = wait_for=="remote"
            if not self._finished:
                self._finished = True
                self._finish_frame = frame({"kind":"finish","id":"finish","run_id":self.id,"state":state,"dropped":self._dropped})
        finally:
            self._lock.release()
        self._wake.set()
        if wait_for != "none":
            self._finish_ack.wait(max(0,deadline-time.monotonic()))
            if wait_for == "remote" and self.sync == "background" and self._finish_ack.is_set():
                # A previous local finish may already have stopped the sender.
                # Explicit finish may use IPC; log() remains memory-only. Restore
                # this run's credentials if the detached collector has restarted.
                register = True
                while time.monotonic()<deadline and not self._snapshot.get("remote_complete",False):
                    try:
                        message = self._register if register else {"kind":"status","run_id":self.id}
                        reply = command(self.spool_dir,message,timeout=min(.5,max(.001,deadline-time.monotonic())))
                        self._snapshot = {**self._snapshot,**reply.get("status",{})}
                        self._snapshot_at = time.monotonic()
                        register = False
                    except (OSError,ValueError,RuntimeError) as exc:
                        self._last_error = f"collector checkpoint: {type(exc).__name__}"
                    time.sleep(min(0.02,max(0,deadline-time.monotonic())))
        complete = self._finish_ack.is_set() and self._durable==self._accepted and self._rejected==0
        reason = None if complete else self._last_error or "timeout; remaining events may only exist in memory"
        if wait_for == "remote" and (self.sync=="off" or not self._snapshot.get("remote_complete",False)):
            complete,reason = False,"sync is off" if self.sync=="off" else "; ".join(self._snapshot.get("sync_errors",[])) or "remote checkpoint timeout; local records retained"
        self._wait_remote = False
        receipt = FinishReceipt(complete,wait_for,self._accepted,self._durable,self._dropped,reason)
        if self.mode=="strict" and not complete and wait_for!="none":
            raise RuntimeError(reason)
        return receipt

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            try:
                self.finish(state="failed",timeout=0.2)
            except Exception:
                pass
        else:
            self.finish()
        return False

    def _sender(self):
        inflight = OrderedDict()
        sock = None
        output, incoming = bytearray(),bytearray()
        registered = False
        next_status = 0.0
        finishing_at = None
        try:
            while True:
                if sock is None:
                    try:
                        sock = connect(self.spool_dir,timeout=3)
                        output = bytearray(self._register_frame)
                        output.extend(b"".join(inflight.values()))
                        incoming = bytearray()
                        registered = False
                    except Exception as exc:
                        self._last_error = f"collector startup: {type(exc).__name__}"
                        self._wake.wait(0.2)
                        self._wake.clear()
                        continue
                if registered and len(output)<256*1024 and len(inflight)<128 and sum(map(len,inflight.values()))<768*1024:
                    if self._lock.acquire(blocking=False):
                        try:
                            while self._queue and len(output)<256*1024 and len(inflight)<128 and sum(map(len,inflight.values()))<768*1024:
                                key,data = self._queue.popleft()
                                self._bytes -= len(data)
                                inflight[key] = data
                                output.extend(data)
                            if self._finished and not self._queue and self._finish_frame and "finish" not in inflight and not self._finish_ack.is_set():
                                inflight["finish"] = self._finish_frame
                                output.extend(self._finish_frame)
                        finally:
                            self._lock.release()
                if registered and time.monotonic()>=next_status and not output:
                    output.extend(frame({"kind":"status","id":"status","run_id":self.id}))
                    next_status = time.monotonic()+0.25
                try:
                    readable,writable,_ = select.select([sock],[sock] if output else [],[],0.03)
                    if writable:
                        sent = sock.send(output)
                        del output[:sent]
                    if readable:
                        chunk = sock.recv(65536)
                        if not chunk:
                            raise OSError("collector disconnected")
                        incoming.extend(chunk)
                        for reply in extract(incoming):
                            key = reply.get("id")
                            if not reply.get("ok"):
                                self._last_error = reply.get("error","collector rejected message")
                                if key in inflight:
                                    inflight.pop(key)
                                    self._rejected += 1
                                if key=="register":
                                    # A manifest conflict cannot be retried by changing event IDs.
                                    self._rejected += len(inflight)
                                    return
                                if key=="finish":
                                    self._finish_ack.set()
                                continue
                            if key=="register":
                                registered = True
                                self._snapshot["collector_ready"] = True
                                self._snapshot["session_ordinal"] = reply.get("session_ordinal")
                            if key in inflight:
                                inflight.pop(key)
                                if key.startswith("event-"):
                                    self._durable += 1
                            if "status" in reply:
                                self._snapshot = {**self._snapshot,**reply["status"]}
                                self._snapshot_at = time.monotonic()
                            if key=="finish":
                                self._finish_ack.set()
                                finishing_at = time.monotonic()
                    if self._finish_ack.is_set() and not inflight and not self._queue:
                        if not self._wait_remote or self._snapshot.get("remote_complete",False) or (finishing_at and time.monotonic()-finishing_at>30):
                            return
                except (OSError,ValueError) as exc:
                    self._last_error = f"IPC disconnected: {type(exc).__name__}"
                    sock.close()
                    sock = None
        finally:
            if sock:
                sock.close()


def init(project, run_path, **kwargs):
    run = Run(project,run_path,**kwargs)
    run._resumed = kwargs.get("run_id") is not None
    return run
