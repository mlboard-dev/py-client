"""Authenticated loopback local viewer; source RunStore remains read-only."""
from __future__ import annotations

import contextlib
import base64
import binascii
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import mimetypes
from pathlib import Path
import re
import secrets
import sqlite3
import time
import urllib.parse
import webbrowser

from .query import LocalQuery, QueryError, project_id, project_name, run_dict
from .store import readonly
from .validation import MAX_INT, dumps, now


def bounded_int(params, name, default, maximum):
    try:
        n = int(params.get(name,[str(default)])[0])
        if not 1<=n<=maximum:
            raise ValueError()
        return n
    except ValueError:
        raise QueryError("INVALID_ARGUMENT",f"{name} must be 1..{maximum}") from None


def record_cursor(params, kind):
    value=params.get("cursor",[None])[0]
    default=[0,0] if kind=="logs" else [0,"","",""]
    if value is None:
        return default
    try:
        if len(value)>8192:
            raise ValueError()
        parsed=json.loads(base64.b64decode(value.encode("ascii"),altchars=b"-_",validate=True))
        if type(parsed) is not list or len(parsed)!=len(default) or any(type(a) is not type(b) for a,b in zip(parsed,default)) or not 0<=parsed[0]<=MAX_INT or (kind=="logs" and not 0<=parsed[1]<=MAX_INT):
            raise ValueError()
        return parsed
    except (ValueError,UnicodeError,binascii.Error):
        raise QueryError("INVALID_CURSOR","invalid record cursor") from None


def encode_cursor(parts):
    return base64.urlsafe_b64encode(dumps(parts).encode()).decode()


class ViewerServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = False
    def __init__(self, root, address=("127.0.0.1",8081), *, token=None):
        try:
            loopback = address[0]=="localhost" or ipaddress.ip_address(address[0]).is_loopback
        except ValueError:
            loopback = False
        if not loopback and not token:
            raise ValueError("non-loopback host requires explicit --token authentication")
        self.root = Path(root).resolve()
        with readonly(self.root) as db:
            self.store_id = db.execute("SELECT value FROM meta WHERE key='store_id'").fetchone()[0]
        self.access_token = token or secrets.token_urlsafe(32)
        self.cookie_token = secrets.token_urlsafe(32)
        self.queries = LocalQuery(self.root)
        super().__init__(address,Handler)
        host,port = self.server_address[:2]
        self.allowed_hosts = {f"{host}:{port}",f"localhost:{port}",f"127.0.0.1:{port}"} if loopback else {f"{host}:{port}"}


class Handler(BaseHTTPRequestHandler):
    server_version = "MLBoardLocal/0.1"
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        # URLs, cookies and bearer keys must never appear in access logs.
        pass

    def _headers(self, status, *, mime="application/json; charset=utf-8", length=0, extra=None):
        self.send_response(status)
        self.send_header("Content-Type",mime)
        self.send_header("Content-Length",str(length))
        self.send_header("Cache-Control","no-store")
        self.send_header("X-Content-Type-Options","nosniff")
        self.send_header("Referrer-Policy","no-referrer")
        self.send_header("Content-Security-Policy","default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; object-src 'none'; base-uri 'none'")
        if extra:
            for key,value in extra.items():
                self.send_header(key,value)
        self.end_headers()

    def _json(self, value, status=200, extra=None):
        body = dumps(value).encode()
        self._headers(status,length=len(body),extra=extra)
        self.wfile.write(body)

    def _auth(self):
        if self.headers.get("Host") not in self.server.allowed_hosts:
            raise QueryError("INVALID_HOST","untrusted Host",403)
        origin = self.headers.get("Origin")
        if origin and origin not in {"http://"+h for h in self.server.allowed_hosts}:
            raise QueryError("INVALID_ORIGIN","cross-origin access is disabled",403)
        auth = self.headers.get("Authorization","")
        bearer = auth.startswith("Bearer ") and secrets.compare_digest(auth[7:],self.server.access_token)
        cookie = SimpleCookie()
        try:
            cookie.load(self.headers.get("Cookie",""))
            session = cookie.get("mlboard_local")
        except Exception:
            session = None
        logged = session is not None and secrets.compare_digest(session.value,self.server.cookie_token)
        if not bearer and not logged:
            raise QueryError("UNAUTHORIZED","open the session URL printed by mlboard serve",401)
        return bearer

    def _body(self):
        if self.headers.get("Transfer-Encoding"):
            raise QueryError("INVALID_REQUEST","chunked requests are unsupported")
        try:
            size = int(self.headers.get("Content-Length","0"))
        except ValueError:
            raise QueryError("INVALID_REQUEST","invalid Content-Length") from None
        if not 0<size<=256*1024:
            raise QueryError("BODY_TOO_LARGE","request must be 1..262144 bytes",413)
        try:
            value = json.loads(self.rfile.read(size))
        except (ValueError,RecursionError):
            raise QueryError("INVALID_JSON","invalid JSON body") from None
        if type(value) is not dict:
            raise QueryError("INVALID_JSON","body must be an object")
        return value

    def _route(self, method):
        parsed = urllib.parse.urlsplit(self.path)
        path = urllib.parse.unquote(parsed.path)
        params = urllib.parse.parse_qs(parsed.query,keep_blank_values=True)
        if self.headers.get("Host") not in self.server.allowed_hosts:
            raise QueryError("INVALID_HOST","untrusted Host",403)
        if method=="GET" and path=="/":
            body = (Path(__file__).parent/"static"/"index.html").read_bytes()
            self._headers(200,mime="text/html; charset=utf-8",length=len(body))
            self.wfile.write(body)
            return
        assets = {"/assets/local-preferences.js":("local-preferences.js","application/javascript; charset=utf-8"),"/assets/viewer.css":("viewer.css","text/css; charset=utf-8")}
        if method=="GET" and path in assets:
            filename,mime = assets[path]
            body = (Path(__file__).parent/"static"/filename).read_bytes()
            self._headers(200,mime=mime,length=len(body))
            self.wfile.write(body)
            return
        bearer = self._auth()
        if method=="POST" and path=="/api/local/session":
            if not bearer:
                raise QueryError("UNAUTHORIZED","bearer bootstrap required",401)
            self._body()
            return self._json({"authenticated":True},extra={"Set-Cookie":f"mlboard_local={self.server.cookie_token}; HttpOnly; SameSite=Strict; Path=/"})
        prefix = "/api/v1"
        if not path.startswith(prefix+"/"):
            raise QueryError("NOT_FOUND","route not found",404)
        route = path[len(prefix):]
        if method=="GET" and route=="/capabilities":
            return self._json({"profile":"local-viewer","version":"0.1.0","store_id":self.server.store_id,"read_only":True,"event_schema":"mlboard.event/v1","query_schema":"mlboard.query/v1","kinds":["scalar","histogram","confusion_matrix","curve","table","text","media"],"query_kinds":["scalar"],"transforms":["raw","none","min_max"],"unsupported_query_fields":["consistency","reuse","viewport","read.at_revision"],"axes":["step","epoch","wall_time","elapsed_seconds","session_elapsed_seconds","processed_samples"],"limits":{"max_runs":10,"max_series":20,"max_points_per_series":2000,"max_points":20000,"max_scanned_points":250000,"max_bytes":2097152,"timeout_ms":2000,"paged_snapshots":8},"layout_storage":"browser-local"})
        if method=="POST":
            match = re.fullmatch(r"/projects/([^/]+)/query",route)
            if match:
                return self._json(self.server.queries.execute(match[1],self._body()))
            raise QueryError("READ_ONLY","source data is read-only; local query is the supported POST operation",405)
        if method!="GET":
            raise QueryError("READ_ONLY","local viewer source is read-only",405)
        db = readonly(self.server.root)
        deadline = time.monotonic()+2
        db.set_progress_handler(lambda: int(time.monotonic()>deadline),1000)
        try:
            if route=="/projects":
                rows = db.execute("SELECT project,COUNT(*) n FROM runs GROUP BY project ORDER BY project LIMIT 1000").fetchall()
                return self._json({"projects":[{"id":project_id(r["project"]),"slug":r["project"],"name":r["project"],"run_count":r["n"],"role":"viewer"} for r in rows]})
            if route=="/local/status":
                counts = dict(db.execute("SELECT history_events,pending_events,acked_events,history_bytes,media_bytes FROM store_stats WHERE singleton=1").fetchone())
                runs = [dict(r) for r in db.execute("SELECT id,run_path,retention,pending_mode,last_error,dropped,deletion_generation FROM runs LIMIT 1000")]
                return self._json({"store_id":self.server.store_id,"source":"MLBoard RunStore",**counts,"runs":runs,"updated_at":now()})
            match = re.fullmatch(r"/projects/([^/]+)/(runs|categories)",route)
            if match:
                project = project_name(db,match[1])
                if match[2]=="categories":
                    groups = {}
                    for row in db.execute("SELECT run_path FROM runs WHERE project=?",(project,)):
                        pieces = row[0].split('/')[:-1]
                        for i in range(1,len(pieces)+1):
                            group = '/'.join(pieces[:i])
                            groups[group] = groups.get(group,0)+1
                    return self._json({"categories":[{"path":k,"count":v} for k,v in sorted(groups.items())]})
                limit = bounded_int(params,"limit",100,200)
                offset = int(params.get("cursor",["0"])[0])
                search = params.get("search",[""])[0]
                prefix = params.get("path_prefix",[None])[0]
                state = params.get("state",[None])[0]
                pattern=search.replace('\\','\\\\').replace('%','\\%').replace('_','\\_')
                rows = db.execute("SELECT * FROM runs WHERE project=? AND run_path LIKE ? ESCAPE '\\' ORDER BY run_path,id LIMIT 10001",(project,'%'+pattern+'%')).fetchall()
                if len(rows)>10000:
                    raise QueryError("BUDGET_EXCEEDED","run directory exceeds 10000; narrow search",422)
                rows = [r for r in rows if (prefix is None or (prefix=="" and '/' not in r["run_path"]) or (bool(prefix) and r["run_path"].startswith(prefix+'/'))) and (not state or r["state"]==state)]
                return self._json({"runs":[run_dict(db,r) for r in rows[offset:offset+limit]],"next_cursor":str(offset+limit) if len(rows)>offset+limit else None,"total":len(rows)})
            match = re.fullmatch(r"/runs/([^/]+)(?:/(series|events|logs|records|artifacts|sessions))?",route)
            if match:
                rid,kind = match.groups()
                run = db.execute("SELECT * FROM runs WHERE id=?",(rid,)).fetchone()
                if not run:
                    raise QueryError("NOT_FOUND","run not found",404)
                if kind is None:
                    return self._json(run_dict(db,run))
                if kind=="series":
                    offset = max(0,int(params.get("cursor",["0"])[0]))
                    limit = bounded_int(params,"limit",200,1000)
                    rows = db.execute("SELECT name,'scalar' kind,context,COUNT(*) count FROM scalars WHERE run_id=? GROUP BY name,context UNION ALL SELECT name,kind,context,COUNT(*) count FROM records WHERE run_id=? GROUP BY name,kind,context ORDER BY name LIMIT ? OFFSET ?",(rid,rid,limit+1,offset)).fetchall()
                    return self._json({"series":[{"id":dumps([r["name"],r["kind"],json.loads(r["context"])]),"name":r["name"],"kind":r["kind"],"context":json.loads(r["context"]),"count":r["count"]} for r in rows[:limit]],"next_cursor":str(offset+limit) if len(rows)>limit else None})
                if kind=="artifacts":
                    rows = [dict(r) for r in db.execute("SELECT id,name,kind,size,mime,sha256,metadata FROM media WHERE run_id=? LIMIT 1000",(rid,))]
                    for r in rows:
                        r["metadata"] = json.loads(r["metadata"])
                    return self._json({"artifacts":rows})
                if kind=="sessions":
                    rows = [dict(r) for r in db.execute("SELECT * FROM sessions WHERE run_id=? ORDER BY ordinal",(rid,))]
                    for row in rows:
                        row["ordinal"] = str(row["ordinal"])
                        row["producers"] = [dict(p) for p in db.execute("SELECT id,role FROM producers WHERE run_id=? AND session_id=?",(rid,row["id"]))]
                    return self._json({"sessions":rows})
                limit = bounded_int(params,"limit",100,200)
                if kind=="events":
                    cursor = max(0,int(params.get("cursor",["0"])[0]))
                    rows = db.execute("SELECT revision,body FROM events WHERE run_id=? AND revision>? ORDER BY revision LIMIT ?",(rid,cursor,limit+1)).fetchall()
                    values = [json.loads(r["body"]) for r in rows[:limit]]
                    next_cursor=str(rows[limit-1]["revision"]) if len(rows)>limit else None
                elif kind=="logs":
                    cursor=record_cursor(params,kind)
                    rows = db.execute("SELECT rowid,* FROM logs WHERE run_id=? AND (event_revision,rowid)>(?,?) ORDER BY event_revision,rowid LIMIT ?",(rid,*cursor,limit+1)).fetchall()
                    values = [{"time":r["time"],"level":r["level"],"message":r["message"]} for r in rows[:limit]]
                    next_cursor=encode_cursor([rows[limit-1]["event_revision"],rows[limit-1]["rowid"]]) if len(rows)>limit else None
                else:
                    cursor=record_cursor(params,kind)
                    rows = db.execute("SELECT * FROM records WHERE run_id=? AND (event_revision,name,kind,context)>(?,?,?,?) ORDER BY event_revision,name,kind,context LIMIT ?",(rid,*cursor,limit+1)).fetchall()
                    values = [{"name":r["name"],"kind":r["kind"],"step":str(r["step"]) if r["step"] is not None else None,"context":json.loads(r["context"]),"payload":json.loads(r["payload"])} for r in rows[:limit]]
                    next_cursor=encode_cursor([rows[limit-1][k] for k in ("event_revision","name","kind","context")]) if len(rows)>limit else None
                return self._json({kind:values,"next_cursor":next_cursor})
            match = re.fullmatch(r"/artifacts/([^/]+)/content",route)
            if match:
                media = db.execute("SELECT * FROM media WHERE id=?",(match[1],)).fetchone()
                if not media:
                    raise QueryError("NOT_FOUND","artifact not found",404)
                file_path = (self.server.root/media["path"]).resolve()
                if file_path.parent != self.server.root/"media":
                    raise QueryError("INVALID_ARTIFACT","invalid media reference",500)
                mime = media["mime"]
                inline = mime in ("image/png","image/jpeg","image/gif","image/webp","audio/mpeg","video/mp4")
                # Close source read connection before streaming any file bytes.
                size = file_path.stat().st_size
                db.close()
                self._headers(200,mime=mime if inline else "application/octet-stream",length=size,extra={"Content-Disposition":'inline' if inline else 'attachment; filename="artifact"'})
                with file_path.open("rb") as source:
                    while block := source.read(65536):
                        self.wfile.write(block)
                return
            raise QueryError("NOT_FOUND","route not found",404)
        finally:
            db.close()

    def _handle(self, method):
        self.connection.settimeout(5)
        try:
            self._route(method)
        except QueryError as exc:
            self._json({"error":{"code":exc.code,"message":str(exc),"retryable":exc.status in (429,503)},"request_id":secrets.token_hex(8)},exc.status)
        except (ValueError,KeyError,TypeError):
            self._json({"error":{"code":"INVALID_ARGUMENT","message":"invalid request argument","retryable":False}},400)
        except sqlite3.OperationalError:
            self._json({"error":{"code":"SOURCE_BUSY","message":"source unavailable or query exceeded 2-second read budget","retryable":True}},503)
        except (BrokenPipeError,ConnectionResetError,TimeoutError):
            pass
        except OSError:
            self._json({"error":{"code":"SOURCE_UNAVAILABLE","message":"local source unavailable","retryable":True}},503)

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")

    def do_PUT(self):
        self._handle("PUT")

    def do_DELETE(self):
        self._handle("DELETE")


def serve(root, *, host="127.0.0.1", port=8081, token=None, open_browser=False):
    server = ViewerServer(root,(host,port),token=token)
    url = f"http://{host}:{server.server_port}/#token={server.access_token}"
    print("MLBoard local viewer (read-only source): " + url,flush=True)
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
