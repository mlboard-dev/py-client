"""Bounded local queries over collector-maintained typed indexes."""
from __future__ import annotations

from collections import defaultdict
import datetime as dt
import hashlib
import json
import math
import re
import sqlite3
import threading
import time
import uuid

from .query_limits import QUERY_LIMITS, MAX_ACTIVE_QUERIES, SNAPSHOT_TTL_SECONDS, MIN_DOWNSAMPLE_POINTS
from .store import readonly
from .validation import MAX_INT, dumps, context_key


class QueryError(ValueError):
    def __init__(self, code, message, status=400):
        super().__init__(message)
        self.code,self.status = code,status


def project_id(name):
    return "local-" + hashlib.sha256(name.encode()).hexdigest()[:20]


def project_name(db, pid):
    for row in db.execute("SELECT DISTINCT project FROM runs"):
        if pid in (row[0],project_id(row[0])):
            return row[0]
    raise QueryError("NOT_FOUND","project not found",404)


def run_dict(db, row):
    item = dict(row)
    for name in ("config","tags"):
        item[name] = json.loads(item[name])
    item["project_id"] = project_id(item["project"])
    item["run_version"] = str(item["manifest_revision"])
    item["data_revision"] = str(db.execute("SELECT COALESCE(MAX(revision),0) FROM events WHERE run_id=?",(item["id"],)).fetchone()[0])
    item["summary"] = {}
    # Summary deliberately uses the same primary/latest-logical policy as curves.
    rows = db.execute("""WITH logical AS (
      SELECT s.*, ROW_NUMBER() OVER(PARTITION BY s.name,s.context,COALESCE(s.step,-s.event_revision)
        ORDER BY s.ordinal DESC,s.seq DESC,s.event_revision DESC) AS rn
      FROM scalars s JOIN sessions t ON t.id=s.session_id
      WHERE s.run_id=? AND s.producer_id=t.primary_producer),
      canonical AS (SELECT * FROM logical WHERE rn=1), last_values AS (
        SELECT *,ROW_NUMBER() OVER(PARTITION BY name,context
          ORDER BY ordinal DESC,seq DESC,event_revision DESC) last_rank FROM canonical)
      SELECT name,context,MIN(CASE WHEN value_state='finite' THEN value END) minimum,
        MAX(CASE WHEN value_state='finite' THEN value END) maximum,COUNT(*) n,
        MAX(CASE WHEN last_rank=1 THEN value END) last,
        MAX(CASE WHEN last_rank=1 THEN value_state END) last_state,
        COUNT(CASE WHEN value_state='finite' THEN 1 END) finite_count
      FROM last_values GROUP BY name,context LIMIT 1000""",(item["id"],)).fetchall()
    for metric in rows:
        key=dumps([metric["name"],json.loads(metric["context"])])
        item["summary"][key] = {"name":metric["name"],"min":metric["minimum"],"max":metric["maximum"],"count":metric["n"],"last":metric["last"],"value_state":metric["last_state"],"context":json.loads(metric["context"]),"finite_count":metric["finite_count"]}
    item.pop("initial_body",None)
    return item


def wall_coordinate(value):
    """Parse RFC3339 into exact nanoseconds, including non-UTC offsets."""
    if type(value) is not str:
        raise QueryError("INVALID_QUERY","wall_time requires RFC3339 strings")
    match = re.fullmatch(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})",value)
    if not match:
        raise QueryError("INVALID_QUERY","wall_time requires RFC3339 strings")
    try:
        stamp = dt.datetime.fromisoformat(match[1]+("+00:00" if match[3]=="Z" else match[3]))
        delta = stamp.astimezone(dt.timezone.utc)-dt.datetime(1970,1,1,tzinfo=dt.timezone.utc)
    except (ValueError,OverflowError):
        raise QueryError("INVALID_QUERY","invalid wall_time") from None
    return (delta.days*86400+delta.seconds)*1_000_000_000+int((match[2] or "").ljust(9,"0"))


def coordinate(value, axis):
    if axis=="event_time":
        return wall_coordinate(value)
    if axis in ("step","processed_samples"):
        if type(value) is str and re.fullmatch(r"[0-9]{1,19}",value):
            value=int(value)
        if type(value) is not int or not 0<=value<=MAX_INT:
            raise QueryError("INVALID_QUERY",f"{axis} requires exact nonnegative int63 values")
        return value
    if type(value) is str:
        try:
            value=float(value)
        except ValueError:
            raise QueryError("INVALID_QUERY","numeric x bounds must be finite") from None
    if type(value) not in (int,float) or not math.isfinite(value):
        raise QueryError("INVALID_QUERY","numeric x bounds must be finite")
    return value


def fields_only(value, allowed, label):
    if type(value) is not dict:
        raise QueryError("INVALID_QUERY",f"{label} must be an object")
    unknown=set(value)-set(allowed)
    if unknown:
        raise QueryError("UNSUPPORTED_OPERATION",f"unsupported {label} fields: "+", ".join(sorted(unknown)),422)
    return value


def validate_spec(spec):
    fields_only(spec,("schema_version","runs","source","x","read","transform","page","budget"),"query")
    if spec.get("schema_version","mlboard.query/v1")!="mlboard.query/v1":
        raise QueryError("INVALID_QUERY","unsupported query schema")
    fields_only(spec.get("page",{}),("limit","cursor"),"page")
    selector=fields_only(spec.get("runs",{}),("run_ids","path_prefixes","search","filters","include_ungrouped"),"runs")
    for key in ("run_ids","path_prefixes"):
        if key in selector and (type(selector[key]) is not list or len(selector[key])>1000 or any(type(v) is not str or not v for v in selector[key])):
            raise QueryError("INVALID_QUERY",f"{key} must be a bounded string list")
    if type(selector.get("search","")) is not str or type(selector.get("include_ungrouped",False)) is not bool or type(selector.get("filters",[])) is not list:
        raise QueryError("INVALID_QUERY","invalid run selection")
    for item in selector.get("filters",[]):
        fields_only(item,("field","op","value"),"run filter")
        if item.get("field")!="state" or item.get("op")!="in" or type(item.get("value")) is not list or any(type(v) is not str for v in item["value"]):
            raise QueryError("UNSUPPORTED_OPERATION","only state/in local run filters supported",422)
    source=fields_only(spec.get("source",{}),("kind","metrics"),"source")
    if source.get("kind","scalar")!="scalar":
        raise QueryError("UNSUPPORTED_OPERATION","use /runs/{id}/records for typed local records",422)
    metrics=source.get("metrics",[])
    if type(metrics) is not list or not 1<=len(metrics)<=QUERY_LIMITS["max_series"]:
        raise QueryError("BUDGET_EXCEEDED",f'select 1..{QUERY_LIMITS["max_series"]} metrics',422)
    identities=set()
    for metric in metrics:
        fields_only(metric,("name","context"),"metric")
        if type(metric.get("name")) is not str or not metric["name"] or len(metric["name"].encode())>256 or type(metric.get("context",{})) is not dict:
            raise QueryError("INVALID_QUERY","metrics require name and object context")
        if any(type(v) not in (str,int,float,bool,type(None)) or (type(v) is float and not math.isfinite(v)) for v in metric.get("context",{}).values()):
            raise QueryError("INVALID_QUERY","metric context values must be finite primitive JSON")
        identity=context_key([metric["name"],metric.get("context",{})])
        if identity in identities:
            raise QueryError("INVALID_QUERY","duplicate metric selection")
        identities.add(identity)
    xspec=fields_only(spec.get("x",{}),("field","range"),"x")
    axis=xspec.get("field","step")
    if axis not in ("step","epoch","elapsed_seconds","session_elapsed_seconds","processed_samples","wall_time"):
        raise QueryError("UNSUPPORTED_OPERATION","unsupported local x axis",422)
    axis="event_time" if axis=="wall_time" else axis
    bounds=fields_only(xspec.get("range",{}),("gte","lte"),"x range")
    bounds={k:coordinate(v,axis) for k,v in bounds.items()}
    if "gte" in bounds and "lte" in bounds and bounds["gte"]>bounds["lte"]:
        raise QueryError("INVALID_QUERY","x range is reversed")
    read=fields_only(spec.get("read",{}),("producers","sessions","duplicate_steps"),"read")
    for key,default,allowed in (("producers","primary",("primary","ids")),("sessions","all",("all","latest","ids"))):
        policy=fields_only(read.get(key,{}),("mode","ids"),key)
        mode=policy.get("mode",default)
        if mode not in allowed:
            raise QueryError("UNSUPPORTED_OPERATION",f"unsupported {key} policy",422)
        ids=policy.get("ids")
        if mode=="ids":
            if type(ids) is not list or not 1<=len(ids)<=64 or any(type(v) is not str or not v for v in ids) or len(set(ids))!=len(ids):
                raise QueryError("INVALID_QUERY",f"explicit {key} ids are required")
        elif ids is not None:
            raise QueryError("INVALID_QUERY",f"{key} ids require ids mode")
    if read.get("duplicate_steps","latest_logical") not in ("latest_logical","all"):
        raise QueryError("UNSUPPORTED_OPERATION","unsupported duplicate step policy",422)
    transform=fields_only(spec.get("transform",{}),("kind","algorithm","algorithm_version","points_per_series"),"transform")
    kind=transform.get("kind","raw")
    if kind not in ("none","raw","downsample"):
        raise QueryError("UNSUPPORTED_OPERATION","only raw/min_max transforms are supported",422)
    if kind=="downsample":
        if transform.get("algorithm","min_max")!="min_max" or transform.get("algorithm_version","1")!="1":
            raise QueryError("UNSUPPORTED_OPERATION","only min_max algorithm version 1 is supported",422)
        points=transform.get("points_per_series",QUERY_LIMITS["max_points_per_series"])
        if type(points) is not int or not MIN_DOWNSAMPLE_POINTS<=points<=QUERY_LIMITS["max_points_per_series"]:
            raise QueryError("BUDGET_EXCEEDED",f'points_per_series must be {MIN_DOWNSAMPLE_POINTS}..{QUERY_LIMITS["max_points_per_series"]}',422)
    elif set(transform)-{"kind"}:
        raise QueryError("UNSUPPORTED_OPERATION","raw transform has no algorithm or sampling options",422)
    budget=fields_only(spec.get("budget",{}),("max_runs","max_series","max_points","max_scanned_points","max_bytes","timeout_ms","on_exceed","max_points_per_series"),"budget")
    for key,value in budget.items():
        if key=="on_exceed":
            if value!="error":
                raise QueryError("UNSUPPORTED_OPERATION","partial budget responses are not implemented",422)
        elif type(value) is not int or value<1:
            raise QueryError("INVALID_QUERY","budgets must be positive integers")
    return axis,bounds


def min_max(rows, limit):
    if len(rows)<=limit:
        return rows
    # v1: actual X buckets, first/min/max/last, gap boundaries and finite neighbors.
    mandatory = {0,len(rows)-1}
    finite=lambda i: rows[i]["value_state"]=="finite" and rows[i]["value"] is not None
    for i in range(len(rows)):
        if finite(i):
            continue
        if i==0 or finite(i-1):
            mandatory.add(i)
            if i>0:
                mandatory.add(i-1)
        if i==len(rows)-1 or finite(i+1):
            mandatory.add(i)
            if i+1<len(rows):
                mandatory.add(i+1)
    if len(mandatory)>limit:
        raise QueryError("BUDGET_EXCEEDED","gap boundaries exceed points budget; narrow x range",422)
    buckets=(limit-len(mandatory))//4
    if buckets<1:
        raise QueryError("BUDGET_EXCEEDED","min_max extrema cannot fit points budget",422)
    grouped=defaultdict(list)
    xmin,xmax=rows[0]["_x"],rows[-1]["_x"]
    for i,row in enumerate(rows):
        index=min(buckets-1,int((row["_x"]-xmin)*buckets//(xmax-xmin))) if xmax>xmin else 0
        grouped[index].append(i)
    selected=set(mandatory)
    for indices in grouped.values():
        selected.update((indices[0],indices[-1]))
        numeric=[i for i in indices if finite(i)]
        if numeric:
            selected.add(min(numeric,key=lambda i:rows[i]["value"]))
            selected.add(max(numeric,key=lambda i:rows[i]["value"]))
    return [rows[i] for i in sorted(selected)]


class LocalQuery:
    def __init__(self, root):
        self.root = root
        self.snapshots = {}
        self.lock = threading.Lock()
        self.slots = threading.BoundedSemaphore(MAX_ACTIVE_QUERIES)

    def _page(self, snapshot, offset, limit):
        full = snapshot["result"]
        page = json.loads(dumps(full))
        frames, skipped, returned = [],0,0
        for original in full["frames"]:
            count = original["row_count"]
            if skipped+count<=offset:
                skipped += count
                continue
            begin = max(0,offset-skipped)
            take = min(count-begin,limit-returned)
            if take>0:
                item = {**original,"row_count":take,"columns":{k:v[begin:begin+take] for k,v in original["columns"].items()}}
                frames.append(item)
                returned += take
            skipped += count
            if returned>=limit:
                break
        total = sum(f["row_count"] for f in full["frames"])
        page["frames"] = frames
        page["page"] = {"next_cursor":f'{snapshot["id"]}:{offset+returned}' if offset+returned<total else None,"returned_rows":returned}
        return page

    def execute(self, pid, spec):
        axis,bounds = validate_spec(spec)
        fingerprint=hashlib.sha256(dumps({k:v for k,v in spec.items() if k!="page"}).encode()).hexdigest()
        page_spec = spec.get("page",{})
        limit = page_spec.get("limit",QUERY_LIMITS["max_points_per_series"])
        if type(limit) is not int or not 1<=limit<=QUERY_LIMITS["max_points"]:
            raise QueryError("INVALID_QUERY",f'page limit must be 1..{QUERY_LIMITS["max_points"]}')
        cursor = page_spec.get("cursor")
        if cursor:
            if type(cursor) is not str:
                raise QueryError("INVALID_QUERY","cursor must be a string")
            try:
                sid,offset = cursor.split(":")
                offset = int(offset)
                if offset<0:
                    raise ValueError()
                with self.lock:
                    snapshot = self.snapshots[sid]
                if snapshot["expires"]<time.monotonic() or snapshot["project"]!=pid or snapshot["fingerprint"]!=fingerprint:
                    raise KeyError()
                with readonly(self.root) as db:
                    store = db.execute("SELECT value FROM meta WHERE key='store_id'").fetchone()[0]
                    ids=[item[0] for item in snapshot["generation"]]
                    generation = [(r[0],r[1]) for r in db.execute("SELECT id,deletion_generation FROM runs WHERE id IN ("+','.join('?' for _ in ids)+") ORDER BY id",ids)]
                if store!=snapshot["store"] or generation!=snapshot["generation"]:
                    raise KeyError()
                return self._page(snapshot,offset,limit)
            except (ValueError,KeyError):
                raise QueryError("SNAPSHOT_EXPIRED","query snapshot expired or source changed",410) from None
        if not self.slots.acquire(blocking=False):
            raise QueryError("QUERY_BUSY","two local queries already running",429)
        db = None
        budget = spec.get("budget",{})
        deadline = time.monotonic()+min(QUERY_LIMITS["timeout_ms"],budget.get("timeout_ms",QUERY_LIMITS["timeout_ms"]))/1000
        try:
            db = readonly(self.root)
            db.set_progress_handler(lambda: int(time.monotonic()>deadline),1000)
            db.execute("BEGIN")
            project = project_name(db,pid)
            source = spec.get("source",{})
            metrics = source.get("metrics",[])
            read = spec.get("read",{})
            producer = {"mode":"primary",**read.get("producers",{})}
            session = {"mode":"all",**read.get("sessions",{})}
            duplicate = read.get("duplicate_steps","latest_logical")
            transform = {"kind":"raw",**spec.get("transform",{})}
            if transform["kind"]=="none":
                transform["kind"]="raw"
            if transform["kind"]=="downsample":
                transform={"algorithm":"min_max","algorithm_version":"1",**transform}
            points = min(QUERY_LIMITS["max_points_per_series"],transform.get("points_per_series",QUERY_LIMITS["max_points_per_series"]),budget.get("max_points_per_series",QUERY_LIMITS["max_points_per_series"]))
            max_scanned = min(QUERY_LIMITS["max_scanned_points"],budget.get("max_scanned_points",QUERY_LIMITS["max_scanned_points"]))
            max_total = min(QUERY_LIMITS["max_points"],budget.get("max_points",QUERY_LIMITS["max_points"]))
            selector = spec.get("runs",{})
            ids = selector.get("run_ids")
            sql="SELECT * FROM runs WHERE project=?"
            params=[project]
            if ids is not None:
                sql+=" AND id IN ("+','.join('?' for _ in ids)+")"
                params.extend(ids)
            selected = [dict(r) for r in db.execute(sql+" ORDER BY run_path,id LIMIT 10001",params)]
            if len(selected)>10000:
                raise QueryError("BUDGET_EXCEEDED","run directory exceeds 10000; select explicit run_ids",422)
            prefixes = selector.get("path_prefixes",[])
            if "path_prefixes" in selector or "include_ungrouped" in selector:
                selected = [r for r in selected if any(r["run_path"].startswith(p+"/") for p in prefixes) or (selector.get("include_ungrouped",False) and '/' not in r["run_path"])]
            search = selector.get("search","")
            if search:
                selected = [r for r in selected if search.casefold() in r["run_path"].casefold()]
            for f in selector.get("filters",[]):
                selected = [r for r in selected if r["state"] in f["value"]]
            if len(selected)>min(QUERY_LIMITS["max_runs"],budget.get("max_runs",QUERY_LIMITS["max_runs"])):
                raise QueryError("BUDGET_EXCEEDED",f'local query allows at most {QUERY_LIMITS["max_runs"]} runs',422)
            series = defaultdict(list)
            scanned,merged,missing = 0,0,[]
            resolved,producers,ordinals = [],[],{}
            for run in selected:
                rid = run["id"]
                revision = db.execute("SELECT COALESCE(MAX(revision),0) FROM events WHERE run_id=?",(rid,)).fetchone()[0]
                resolved.append({"run_id":rid,"run_path":run["run_path"],"run_version":str(run["manifest_revision"]),"data_revision":str(revision),"summary_revision":str(revision)})
                sessions = [dict(s) for s in db.execute("SELECT * FROM sessions WHERE run_id=? ORDER BY ordinal",(rid,))]
                if session["mode"]=="latest":
                    sessions=sessions[-1:]
                elif session["mode"]=="ids":
                    sessions=[s for s in sessions if s["id"] in session["ids"]]
                ordinals[rid] = [str(s["ordinal"]) for s in sessions]
                for s in sessions:
                    for p in db.execute("SELECT id FROM producers WHERE run_id=? AND session_id=?",(rid,s["id"])):
                        if producer["mode"]=="primary" and p[0]!=s["primary_producer"]:
                            continue
                        if producer["mode"]=="ids" and p[0] not in producer["ids"]:
                            continue
                        producers.append({"run_id":rid,"session_id":s["id"],"producer_id":p[0],"logical_stream":"primary" if producer["mode"]=="primary" else p[0]})
                for metric in metrics:
                    identity_column = "mlboard_context_key(s.context)" if db.legacy_context_index else "s.context_key"
                    sql = f"SELECT s.* FROM scalars s JOIN sessions t ON t.id=s.session_id WHERE s.run_id=? AND s.name=? AND {identity_column}=? AND s.event_revision<=?"
                    params = [rid,metric["name"],context_key(metric.get("context",{})),revision]
                    if producer["mode"]=="primary":
                        sql += " AND s.producer_id=t.primary_producer"
                    else:
                        pids = producer.get("ids",[])
                        sql += " AND s.producer_id IN ("+','.join('?' for _ in pids)+")"
                        params.extend(pids)
                    if session["mode"]=="latest":
                        sql += " AND s.ordinal=(SELECT MAX(ordinal) FROM sessions WHERE run_id=?)"
                        params.append(rid)
                    elif session["mode"]=="ids":
                        sids = session.get("ids",[])
                        sql += " AND s.session_id IN ("+','.join('?' for _ in sids)+")"
                        params.extend(sids)
                    sql += " ORDER BY s.ordinal,s.seq,s.event_revision LIMIT ?"
                    params.append(max_scanned-scanned+1)
                    rows = [dict(r) for r in db.execute(sql,params)]
                    scanned += len(rows)
                    if scanned>max_scanned:
                        raise QueryError("BUDGET_EXCEEDED","source scan budget exceeded; narrow run/metric selection",422)
                    if duplicate=="latest_logical":
                        logical = {}
                        for r in rows:
                            stream = "primary" if producer["mode"]=="primary" else r["producer_id"]
                            key = (stream,r["step"]) if r["step"] is not None else (stream,"event",r["event_revision"])
                            logical[key] = r
                        merged += len(rows)-len(logical)
                        rows = list(logical.values())
                    for row in rows:
                        x = row[axis]
                        if x is None:
                            missing.append({"run_id":rid,"metric":metric["name"],"reason":"missing_x"})
                            continue
                        lower,upper = bounds.get("gte"),bounds.get("lte")
                        sort_x=coordinate(x,axis)
                        if (lower is not None and sort_x<lower) or (upper is not None and sort_x>upper):
                            continue
                        row["x"] = str(x) if axis in ("step","processed_samples") else x
                        row["_x"] = sort_x
                        stream = "primary" if producer["mode"]=="primary" else row["producer_id"]
                        series[(rid,metric["name"],context_key(metric.get("context",{})),stream)].append(row)
            if len(series)>min(QUERY_LIMITS["max_series"],budget.get("max_series",QUERY_LIMITS["max_series"])):
                raise QueryError("BUDGET_EXCEEDED",f'local query allows at most {QUERY_LIMITS["max_series"]} series',422)
            frames,returned,input_points = [],0,0
            for (rid,name,context,stream),rows in series.items():
                rows.sort(key=lambda r:(r["_x"],r["ordinal"],r["seq"]))
                input_points += len(rows)
                if transform.get("kind")=="downsample":
                    rows = min_max(rows,points)
                elif len(rows)>points:
                    raise QueryError("BUDGET_EXCEEDED","series exceeds points budget; request min_max",422)
                returned += len(rows)
                if returned>max_total:
                    raise QueryError("BUDGET_EXCEEDED","returned point budget exceeded",422)
                columns = {"run_id":[rid]*len(rows),"step":[str(r["step"]) if r["step"] is not None else None for r in rows],"x":[r["x"] for r in rows],"value":[r["value"] for r in rows],"value_state":[r["value_state"] for r in rows],"event_ref":[{"run_id":rid,"session_id":r["session_id"],"session_ordinal":str(r["ordinal"]),"producer_id":r["producer_id"],"seq":str(r["seq"])} for r in rows]}
                xtype="datetime" if axis=="event_time" else "int64" if axis in ("step","processed_samples") else "float64"
                fields = [{"name":k,"type":typ,"nullable":k in ("step","value")} for k,typ in (("run_id","string"),("step","int64"),("x",xtype),("value","float64"),("value_state","enum"),("event_ref","event_ref"))]
                frames.append({"frame_id":uuid.uuid4().hex,"kind":"scalar","row_count":len(rows),"fields":fields,"columns":columns,"meta":{"run_id":rid,"metric":name,"context":json.loads(context),"logical_stream":stream,"derived":False}})
            sid = uuid.uuid4().hex
            store_id = db.execute("SELECT value FROM meta WHERE key='store_id'").fetchone()[0]
            generation = sorted((r["id"],r["deletion_generation"]) for r in selected)
            result = {"schema_version":"mlboard.query-result/v1","query_id":uuid.uuid4().hex,"result_etag":'"'+sid+'"',"complete":True,"consistency":{"snapshot_id":sid,"expires_at":(dt.datetime.now(dt.timezone.utc)+dt.timedelta(seconds=SNAPSHOT_TTL_SECONDS)).isoformat(),"resolved_runs":resolved},"applied":{"read":{"producers":producers,"session_ordinals":ordinals,"duplicate_steps":duplicate,"policy_version":"1"},"transform":{**transform,"input_points":input_points,"returned_points":returned,"downsampled":input_points>returned},"storage":{"storage_instance_id":store_id,"source_revision":str(max((int(r["data_revision"]) for r in resolved),default=0)),"indexed_revision":str(max((int(r["data_revision"]) for r in resolved),default=0)),"coverage":"retained_history"},"budget":{"max_runs":min(QUERY_LIMITS["max_runs"],budget.get("max_runs",QUERY_LIMITS["max_runs"])),"max_series":min(QUERY_LIMITS["max_series"],budget.get("max_series",QUERY_LIMITS["max_series"])),"max_points":max_total,"max_scanned_points":max_scanned}},"frames":frames,"diagnostics":{"truncated_by":[],"missing":missing[:100],"stale_summaries":[],"scanned_points":scanned,"merged_points":merged,"dropped_source_events":str(sum(r["dropped"] for r in selected)),"warnings":[]},"page":{"next_cursor":None,"returned_rows":returned}}
            if len(dumps(result).encode())>min(QUERY_LIMITS["max_bytes"],budget.get("max_bytes",QUERY_LIMITS["max_bytes"])):
                raise QueryError("BUDGET_EXCEEDED","response exceeds byte budget; reduce points",422)
            snapshot = {"id":sid,"expires":time.monotonic()+SNAPSHOT_TTL_SECONDS,"result":result,"store":store_id,"generation":generation,"project":pid,"fingerprint":fingerprint}
            if returned>limit:
                with self.lock:
                    self.snapshots = {key:value for key,value in self.snapshots.items() if value["expires"]>time.monotonic()}
                    if len(self.snapshots)>=QUERY_LIMITS["paged_snapshots"]:
                        raise QueryError("SNAPSHOT_LIMIT",f'{QUERY_LIMITS["paged_snapshots"]} paged queries are active; retry after expiry',429)
                    self.snapshots[sid] = snapshot
            if time.monotonic()>deadline:
                raise QueryError("QUERY_TIMEOUT","local query exceeded its time budget",422)
            return self._page(snapshot,0,limit)
        except sqlite3.OperationalError as exc:
            if "interrupt" in str(exc):
                raise QueryError("QUERY_TIMEOUT","local query exceeded its time budget",422) from None
            raise
        except (TypeError,ValueError) as exc:
            if isinstance(exc,QueryError):
                raise
            raise QueryError("INVALID_QUERY","invalid local query values") from None
        finally:
            if db is not None:
                db.close()
            self.slots.release()
