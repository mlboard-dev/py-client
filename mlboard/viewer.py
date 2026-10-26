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
import socket
import sqlite3
import threading
import time
import urllib.parse
import webbrowser

from .query import LocalQuery, QueryError, project_id, project_name, run_dict, series_directory
from .query_limits import QUERY_LIMITS
from .store import readonly
from .validation import MAX_INT, dumps, now
from .viewer_limits import BoundedRequestReader, HeaderLimitError, ViewerLimits


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
    request_queue_size = 16
    def __init__(self, root, address=("127.0.0.1",8081), *, token=None, public_url=None, limits=None):
        try:
            loopback = address[0]=="localhost" or ipaddress.ip_address(address[0]).is_loopback
        except ValueError:
            loopback = False
        if not loopback and not token:
            raise ValueError("non-loopback host requires explicit --token authentication")
        public = urllib.parse.urlsplit(public_url) if public_url else None
        if public and (public.scheme not in ("http","https") or not public.hostname or public.username
                       or public.password or public.path not in ("", "/") or public.query or public.fragment):
            raise ValueError("--public-url must be an HTTP(S) origin")
        if address[0] in ("0.0.0.0","::","") and not public:
            raise ValueError("wildcard bind requires --public-url, e.g. http://192.168.1.20:8081")
        self.root = Path(root).resolve()
        with readonly(self.root) as db:
            self.store_id = db.execute("SELECT value FROM meta WHERE key='store_id'").fetchone()[0]
        self.access_token = token or secrets.token_urlsafe(32)
        self.cookie_token = secrets.token_urlsafe(32)
        self.queries = LocalQuery(self.root)
        self.limits = limits or ViewerLimits()
        self._connection_lock = threading.Lock()
        self._connections = set()
        self._closing = False
        super().__init__(address,Handler)
        host,port = self.server_address[:2]
        host = f"[{host}]" if ":" in host else host
        self.allowed_hosts = {f"{host}:{port}",f"localhost:{port}",f"127.0.0.1:{port}"} if loopback else {f"{host}:{port}"}
        if public:
            self.allowed_hosts.add(public.netloc)
        self.allowed_origins = {public_url.rstrip("/")} if public else {"http://"+h for h in self.allowed_hosts}
        self.public_url = public_url.rstrip("/") if public else f"http://{host}:{port}"
        self.secure_cookies = bool(public and public.scheme == "https")

    def process_request(self, request, client_address):
        # Acquire before ThreadingMixIn creates a handler thread. Authentication
        # cannot protect a connection that has not finished its first headers.
        with self._connection_lock:
            admitted = not self._closing and len(self._connections) < self.limits.connections
            if admitted:
                self._connections.add(request)
        if not admitted:
            self.shutdown_request(request)
            return
        try:
            request.settimeout(self.limits.idle_seconds)
            super().process_request(request, client_address)
        except BaseException:
            with self._connection_lock:
                self._connections.discard(request)
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            with self._connection_lock:
                self._connections.discard(request)

    def server_close(self):
        with self._connection_lock:
            self._closing = True
            connections = list(self._connections)
        for connection in connections:
            with contextlib.suppress(OSError):
                connection.shutdown(socket.SHUT_RDWR)
        super().server_close()


class Handler(BaseHTTPRequestHandler):
    server_version = "MLBoardLocal/0.1"
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        self.rfile.close()
        self.rfile = BoundedRequestReader(self.connection, self.server.limits)

    def handle_one_request(self):
        self.rfile.begin_headers()
        try:
            super().handle_one_request()
        except HeaderLimitError:
            self.close_connection = True
            self.request_version = getattr(self, "request_version", "HTTP/1.1")
            self.requestline = getattr(self, "requestline", "")
            self.command = getattr(self, "command", None)
            with contextlib.suppress(OSError):
                self.send_error(431, "Request headers exceed byte limit")
        except OSError:
            self.close_connection = True

    def parse_request(self):
        parsed = super().parse_request()
        self.rfile.finish_read()
        return parsed

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
        if self.close_connection:
            self.send_header("Connection", "close")
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
        if origin and origin not in self.server.allowed_origins:
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
        if self.headers.get_all("Transfer-Encoding"):
            raise QueryError("INVALID_REQUEST","chunked requests are unsupported")
        if len(self.headers.get_all("Content-Length", [])) != 1:
            raise QueryError("INVALID_REQUEST","one Content-Length is required")
        try:
            size = int(self.headers.get("Content-Length","0"))
        except ValueError:
            raise QueryError("INVALID_REQUEST","invalid Content-Length") from None
        if not 0<size<=256*1024:
            raise QueryError("BODY_TOO_LARGE","request must be 1..262144 bytes",413)
        try:
            self.rfile.begin_body()
            try:
                body = self.rfile.read(size)
            finally:
                self.rfile.finish_read()
            value = json.loads(body)
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
            secure = "; Secure" if self.server.secure_cookies else ""
            return self._json({"authenticated":True},extra={"Set-Cookie":f"mlboard_local={self.server.cookie_token}; HttpOnly; SameSite=Strict; Path=/{secure}"})
        prefix = "/api/v1"
        if not path.startswith(prefix+"/"):
            raise QueryError("NOT_FOUND","route not found",404)
        route = path[len(prefix):]
        if method=="GET" and route=="/capabilities":
            return self._json({"profile":"local-viewer","version":"0.1.0","store_id":self.server.store_id,"read_only":True,"event_schema":"mlboard.event/v1","query_schema":"mlboard.query/v1","kinds":["scalar","histogram","confusion_matrix","curve","table","text","media"],"query_kinds":["scalar"],"transforms":["raw","none","min_max"],"unsupported_query_fields":["consistency","reuse","viewport","read.at_revision"],"axes":["step","epoch","wall_time","elapsed_seconds","session_elapsed_seconds","processed_samples"],"limits":dict(QUERY_LIMITS),"layout_storage":"browser-local"})
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
                    rows = series_directory(db,rid,limit,offset)
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
        self.connection.settimeout(self.server.limits.idle_seconds)
        try:
            self._route(method)
        except QueryError as exc:
            # Do not parse an unread/invalid body as a new keep-alive request.
            self.close_connection = True
            self._json({"error":{"code":exc.code,"message":str(exc),"retryable":exc.status in (429,503)},"request_id":secrets.token_hex(8)},exc.status)
        except (ValueError,KeyError,TypeError):
            self.close_connection = True
            self._json({"error":{"code":"INVALID_ARGUMENT","message":"invalid request argument","retryable":False}},400)
        except sqlite3.OperationalError:
            self._json({"error":{"code":"SOURCE_BUSY","message":"source unavailable or query exceeded 2-second read budget","retryable":True}},503)
        except (BrokenPipeError,ConnectionResetError,TimeoutError):
            self.close_connection = True
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


def serve(root, *, host="127.0.0.1", port=8081, token=None, open_browser=False, public_url=None):
    server = ViewerServer(root,(host,port),token=token,public_url=public_url)
    url = f"{server.public_url}/#token={server.access_token}"
    print("MLBoard local viewer (read-only source): " + url,flush=True)
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
