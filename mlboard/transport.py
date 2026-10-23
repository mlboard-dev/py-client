"""Explicit bounded HTTP synchronization. This module is never used by log()."""
from __future__ import annotations

import http.client
import json
import re
from pathlib import Path
import urllib.error
import urllib.parse
import urllib.request
import uuid

from .store import readonly
from .validation import dumps, normalize_server


class SyncError(RuntimeError):
    def __init__(self, message, *, status=None, code=None):
        super().__init__(message)
        self.status,self.code = status,code


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise SyncError("HTTP redirect refused; configure the final server URL")


class HTTP:
    def __init__(self, server, api_key=None, timeout=4):
        self.base = normalize_server(server) + "/api/v1"
        self.key = api_key
        self.timeout = timeout
        self.opener = urllib.request.build_opener(NoRedirect())

    def request(self, method, path, body=None, headers=None):
        hdr = {"Accept":"application/json"}
        if self.key:
            hdr["Authorization"] = "Bearer " + self.key
        if headers:
            hdr.update(headers)
        data = None
        if body is not None:
            data = dumps(body).encode()
            hdr["Content-Type"] = "application/json"
        request = urllib.request.Request(self.base + path,data=data,headers=hdr,method=method)
        try:
            with self.opener.open(request,timeout=self.timeout) as response:
                raw = response.read(2 * 1024**2 + 1)
                if len(raw) > 2 * 1024**2:
                    raise SyncError("remote response exceeds 2 MiB")
                result = json.loads(raw) if raw else {}
                if type(result) is not dict:
                    raise SyncError("remote response is not a JSON object")
                return result
        except urllib.error.HTTPError as exc:
            code = "HTTP_ERROR"
            try:
                candidate = json.loads(exc.read(65536)).get("error",{}).get("code",code)
                if type(candidate) is str and re.fullmatch(r"[A-Za-z0-9_:-]{1,80}",candidate):
                    code = candidate
            except (ValueError,AttributeError):
                pass
            raise SyncError(f"HTTP {exc.code}: {code}",status=exc.code,code=code) from None
        except (OSError,ValueError) as exc:
            # Deliberately omit URL, request, token and untrusted remote body.
            raise SyncError(f"remote unavailable ({type(exc).__name__})") from None

    def upload(self, run_id, item, root):
        # Stream multipart; do not load checkpoints into memory.
        boundary = "mlboard-" + uuid.uuid4().hex
        parts = []
        fields = {"artifact_id":item["id"],"name":item["name"],"kind":item["kind"],"metadata":item["metadata"]}
        for key,value in fields.items():
            parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode())
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="artifact"\r\nContent-Type: {item["mime"]}\r\n\r\n'.encode())
        prefix, suffix = b"".join(parts), f"\r\n--{boundary}--\r\n".encode()
        url = urllib.parse.urlsplit(self.base)
        cls = http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
        conn = cls(url.hostname,url.port,timeout=self.timeout)
        headers = {"Content-Type":"multipart/form-data; boundary="+boundary,"Content-Length":str(len(prefix)+item["size"]+len(suffix))}
        if self.key:
            headers["Authorization"] = "Bearer " + self.key
        try:
            conn.putrequest("POST",url.path+f"/runs/{run_id}/artifacts")
            for k,v in headers.items():
                conn.putheader(k,v)
            conn.endheaders()
            conn.send(prefix)
            with (Path(root)/item["path"]).open("rb") as source:
                while chunk := source.read(65536):
                    conn.send(chunk)
            conn.send(suffix)
            response = conn.getresponse()
            raw = response.read(65536)
            if not 200 <= response.status < 300:
                raise SyncError(f"artifact upload HTTP {response.status}")
            result = json.loads(raw)
            if result.get("id") != item["id"]:
                raise SyncError("artifact ID mismatch; event body was not changed")
        finally:
            conn.close()


def publish_state(http, rid, state):
    """Publish against a fresh revision, with bounded retries for concurrent edits."""
    for attempt in range(3):
        remote = http.request("GET",f"/runs/{rid}")
        revision = remote.get("run_version")
        if type(revision) not in (str,int) or not re.fullmatch(r"[0-9]{1,19}",str(revision)):
            raise SyncError("remote run has no valid run_version")
        if remote.get("state") == state:
            return
        try:
            patched = http.request("PATCH",f"/runs/{rid}",{"state":state},headers={"If-Match":str(revision)})
        except SyncError as exc:
            if exc.status==412 and attempt<2:
                continue
            raise
        if patched.get("state") != state:
            raise SyncError("remote state was not confirmed")
        return


def sync_pass(root, config):
    """Read a bounded work unit; caller applies returned ACKs on its sole writer."""
    db = readonly(root)
    try:
        # Background collectors supply an explicit credential target for each run.
        # Never borrow a new run's key for retained runs discovered after restart.
        configured = None
        if "runs" in config:
            configured={rid:{**c,"server":normalize_server(c["server"])} for rid,c in config["runs"].items()}
            if not configured:
                return []
            selection="id IN ("+','.join('?' for _ in configured)+")"
            args=tuple(configured)
        else:
            config={**config,"server":normalize_server(config["server"])}
            selection="remote_server=?"
            args=(config["server"],)
        runs=[dict(r) for r in db.execute("SELECT * FROM runs WHERE pending_mode=1 AND (pending_events>0 OR remote_manifest_revision<manifest_revision) AND "+selection+" ORDER BY updated_at LIMIT 128",args)]
        results = []
        for run in runs:
            rid = run["id"]
            run_config = configured[rid] if configured is not None else config
            server = normalize_server(run["remote_server"] or run_config["server"])
            result = {"run_id":rid,"server":server,"accepted":[],"media":[]}
            try:
                if server!=run_config["server"]:
                    # A key supplied for a new collector target must never be sent
                    # to an old run's persisted remote URL.
                    raise SyncError("REMOTE_TARGET_MISMATCH: credentials do not match this run's bound server")
                http = HTTP(server,run_config.get("api_key"),run_config.get("timeout",4))
                projects = http.request("GET","/projects").get("projects",[])
                project = next((p for p in projects if p.get("id") == run["project"] or p.get("slug") == run["project"]),None)
                if project is None and run_config.get("create_project"):
                    project = http.request("POST","/projects",{"slug":run["project"],"name":run["project"]})
                if project is None:
                    raise SyncError("PROJECT_NOT_FOUND: create project explicitly or use a permitted project ID")
                pid = project["id"]
                if run["remote_project"] and run["remote_project"] != pid:
                    raise SyncError("remote project mapping conflict")
                result["project_id"] = pid
                http.request("POST",f"/projects/{pid}/runs",json.loads(run["initial_body"]))
                sessions = [dict(s) for s in db.execute("SELECT * FROM sessions WHERE run_id=? ORDER BY ordinal",(rid,))]
                for session in sessions:
                    sid = session["id"]
                    http.request("POST",f"/runs/{rid}/sessions",{"session_id":sid,"ordinal":str(session["ordinal"]),"primary_producer":session["primary_producer"]})
                    producers = [dict(p) for p in db.execute("SELECT * FROM producers WHERE run_id=? AND session_id=?",(rid,sid))]
                    for producer in producers:
                        http.request("POST",f"/runs/{rid}/sessions/{sid}/producers",{"producer_id":producer["id"],"role":producer["role"]})
                rows = [dict(r) for r in db.execute("SELECT e.body FROM events e WHERE e.run_id=? AND e.pending=1 AND NOT EXISTS(SELECT 1 FROM media_refs ref JOIN media m ON m.id=ref.artifact_id WHERE ref.event_revision=e.revision AND m.uploaded=0) ORDER BY e.revision LIMIT 512",(rid,))]
                batch, length = [], 0
                for row in rows:
                    length += len(row["body"].encode())
                    if length > 900 * 1024 and batch:
                        break
                    batch.append(json.loads(row["body"]))
                if batch:
                    response = http.request("POST",f"/runs/{rid}/events",{"schema_version":"mlboard.batch/v1","events":batch})
                    expected = {(e["session_id"],e["producer_id"],str(e["seq"])) for e in batch}
                    accepted = {(e["session_id"],e["producer_id"],str(e["seq"])) for e in response.get("accepted",[])}
                    if not accepted <= expected:
                        raise SyncError("remote ACK includes an unsubmitted event")
                    result["accepted"] = list(accepted)
                    if accepted != expected:
                        raise SyncError("remote ACK is incomplete; unacknowledged records retained")
                # Scalars are ACKed before the independent media channel's one-file
                # budget. A slow artifact never sits in front of a scalar batch.
                media = [dict(m) for m in db.execute("SELECT * FROM media WHERE run_id=? AND uploaded=0 LIMIT 1",(rid,))]
                for item in media:
                    http.upload(rid,item,root)
                    result["media"].append(item["id"])
                pending = db.execute("SELECT pending_events FROM runs WHERE id=?",(rid,)).fetchone()[0]
                if pending <= len(result["accepted"]):
                    publish_state(http,rid,run["state"])
                    result["manifest_revision"] = run["manifest_revision"]
            except (SyncError,OSError,ValueError,KeyError) as exc:
                result["error"] = str(exc)[:512] if isinstance(exc,SyncError) else type(exc).__name__
            results.append(result)
        return results
    finally:
        db.close()
