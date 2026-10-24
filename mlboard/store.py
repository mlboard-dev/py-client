"""Collector-owned durable RunStore. Viewers only use readonly()."""
from __future__ import annotations

import contextlib
import hashlib
import json
import mimetypes
import os
from pathlib import Path
import shutil
import sqlite3
import time
import uuid

from .validation import dumps, digest, now, normalize_server, context_key

SCHEMA = 4
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS runs(
 id TEXT PRIMARY KEY, project TEXT NOT NULL, run_path TEXT NOT NULL, name TEXT NOT NULL,
 config TEXT NOT NULL, tags TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'running', retention TEXT NOT NULL,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, remote_server TEXT, remote_project TEXT,
 organization TEXT, remote_organization TEXT,
 pending_mode INTEGER NOT NULL DEFAULT 0, last_error TEXT, dropped INTEGER NOT NULL DEFAULT 0,
 deletion_generation INTEGER NOT NULL DEFAULT 0, initial_body TEXT NOT NULL,
 history_events INTEGER NOT NULL DEFAULT 0, pending_events INTEGER NOT NULL DEFAULT 0,
 acked_events INTEGER NOT NULL DEFAULT 0, history_bytes INTEGER NOT NULL DEFAULT 0,
 media_bytes INTEGER NOT NULL DEFAULT 0, manifest_revision INTEGER NOT NULL DEFAULT 0,
 remote_manifest_revision INTEGER NOT NULL DEFAULT -1);
CREATE TABLE IF NOT EXISTS sessions(
 id TEXT PRIMARY KEY, run_id TEXT NOT NULL, ordinal INTEGER NOT NULL, primary_producer TEXT NOT NULL,
 UNIQUE(run_id, ordinal));
CREATE TABLE IF NOT EXISTS producers(
 run_id TEXT NOT NULL, session_id TEXT NOT NULL, id TEXT NOT NULL, token TEXT NOT NULL,
 role TEXT NOT NULL, pending_mode INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(run_id,session_id,id));
CREATE TABLE IF NOT EXISTS events(
 revision INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, session_id TEXT NOT NULL,
 producer_id TEXT NOT NULL, seq INTEGER NOT NULL, body TEXT NOT NULL, hash TEXT NOT NULL,
 pending INTEGER NOT NULL DEFAULT 0, acked INTEGER NOT NULL DEFAULT 0,
 UNIQUE(run_id,session_id,producer_id,seq));
CREATE INDEX IF NOT EXISTS events_pending ON events(pending,run_id,revision);
CREATE INDEX IF NOT EXISTS events_sync_state ON events(run_id,pending,acked,revision);
CREATE INDEX IF NOT EXISTS events_run_revision ON events(run_id,revision);
CREATE TABLE IF NOT EXISTS scalars(
 event_revision INTEGER NOT NULL, run_id TEXT NOT NULL, name TEXT NOT NULL, context TEXT NOT NULL,
 step INTEGER, epoch REAL, elapsed_seconds REAL, session_elapsed_seconds REAL, event_time TEXT NOT NULL,
 value REAL, value_state TEXT NOT NULL, session_id TEXT NOT NULL, producer_id TEXT NOT NULL, seq INTEGER NOT NULL,
 ordinal INTEGER NOT NULL, processed_samples INTEGER, PRIMARY KEY(event_revision,name,context));
CREATE INDEX IF NOT EXISTS scalars_query ON scalars(run_id,name,context,step,event_revision);
CREATE TABLE IF NOT EXISTS records(
 event_revision INTEGER NOT NULL, run_id TEXT NOT NULL, name TEXT NOT NULL, kind TEXT NOT NULL,
 context TEXT NOT NULL, payload TEXT NOT NULL, step INTEGER,
 PRIMARY KEY(event_revision,name,kind,context));
CREATE INDEX IF NOT EXISTS records_query ON records(run_id,name,kind,event_revision);
CREATE INDEX IF NOT EXISTS records_page ON records(run_id,event_revision,name,kind,context);
CREATE TABLE IF NOT EXISTS logs(
 event_revision INTEGER NOT NULL, run_id TEXT NOT NULL, time TEXT NOT NULL, level TEXT NOT NULL, message TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS logs_run ON logs(run_id,event_revision);
CREATE TABLE IF NOT EXISTS media(
 id TEXT PRIMARY KEY, run_id TEXT NOT NULL, name TEXT NOT NULL, kind TEXT NOT NULL, path TEXT NOT NULL,
 size INTEGER NOT NULL, mime TEXT NOT NULL, sha256 TEXT NOT NULL, metadata TEXT NOT NULL,
 uploaded INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS media_run ON media(run_id,uploaded);
CREATE TABLE IF NOT EXISTS store_stats(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), history_events INTEGER NOT NULL DEFAULT 0,
 pending_events INTEGER NOT NULL DEFAULT 0, acked_events INTEGER NOT NULL DEFAULT 0,
 history_bytes INTEGER NOT NULL DEFAULT 0, media_bytes INTEGER NOT NULL DEFAULT 0);
INSERT OR IGNORE INTO store_stats(singleton) VALUES(1);
CREATE TABLE IF NOT EXISTS media_refs(artifact_id TEXT NOT NULL,event_revision INTEGER NOT NULL,
 PRIMARY KEY(artifact_id,event_revision));
CREATE INDEX IF NOT EXISTS media_refs_event ON media_refs(event_revision);
CREATE TABLE IF NOT EXISTS gc_files(path TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS import_offsets(source TEXT PRIMARY KEY, identity TEXT NOT NULL, offset INTEGER NOT NULL);
"""


class ReadOnlyConnection(sqlite3.Connection):
    def __exit__(self, exc_type, exc, tb):
        try:
            return super().__exit__(exc_type,exc,tb)
        finally:
            self.close()


def readonly(root):
    path = Path(root).resolve() / "spool.sqlite3"
    db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.05,factory=ReadOnlyConnection)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    db.execute("PRAGMA busy_timeout=50")
    schema = db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    if not schema or int(schema[0]) not in (3, SCHEMA):
        db.close()
        raise ValueError("unsupported MLBoard RunStore schema")
    db.legacy_context_index = int(schema[0]) < 4
    if db.legacy_context_index:
        db.create_function("mlboard_context_key",1,lambda raw: context_key(json.loads(raw)),deterministic=True)
    return db


class RunStore:
    def __init__(self, root, *, max_bytes=10 * 1024**3, max_run_bytes=2 * 1024**3):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "media").mkdir(exist_ok=True)
        (self.root / "incoming").mkdir(exist_ok=True)
        self.max_bytes = max_bytes
        self.max_run_bytes = max_run_bytes
        self._next_space_check = 0
        self._physical_size = 0
        self._free_space = 0
        self.db = sqlite3.connect(self.root / "spool.sqlite3", timeout=0.2)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=200")
        self.db.execute("PRAGMA wal_autocheckpoint=256")
        self.db.execute(f"PRAGMA max_page_count={max(128, max_bytes // 4096)}")
        self.db.executescript(SCHEMA_SQL)
        columns = {r[1] for r in self.db.execute("PRAGMA table_info(runs)")}
        migrated = False
        with self.db:
            for column in ("history_events","pending_events","acked_events","history_bytes","media_bytes"):
                if column not in columns:
                    self.db.execute(f"ALTER TABLE runs ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0")
                    migrated = True
            for column,default in (("manifest_revision",0),("remote_manifest_revision",-1)):
                if column not in columns:
                    self.db.execute(f"ALTER TABLE runs ADD COLUMN {column} INTEGER NOT NULL DEFAULT {default}")
            for column in ("organization","remote_organization"):
                if column not in columns:
                    self.db.execute(f"ALTER TABLE runs ADD COLUMN {column} TEXT")
            if "pending_mode" not in {r[1] for r in self.db.execute("PRAGMA table_info(producers)")}:
                self.db.execute("ALTER TABLE producers ADD COLUMN pending_mode INTEGER NOT NULL DEFAULT 0")
                self.db.execute("UPDATE producers SET pending_mode=(SELECT pending_mode FROM runs WHERE id=producers.run_id)")
            if "processed_samples" not in {r[1] for r in self.db.execute("PRAGMA table_info(scalars)")}:
                self.db.execute("ALTER TABLE scalars ADD COLUMN processed_samples INTEGER")
            if migrated:
                # One migration scan; append paths use only indexed row/counter work.
                self.db.execute("UPDATE runs SET history_events=(SELECT COUNT(*) FROM events WHERE run_id=runs.id),pending_events=(SELECT COALESCE(SUM(pending),0) FROM events WHERE run_id=runs.id),acked_events=(SELECT COALESCE(SUM(acked),0) FROM events WHERE run_id=runs.id),history_bytes=(SELECT COALESCE(SUM(length(CAST(body AS BLOB))),0) FROM events WHERE run_id=runs.id),media_bytes=(SELECT COALESCE(SUM(size),0) FROM media WHERE run_id=runs.id)")
                self.db.execute("UPDATE store_stats SET history_events=(SELECT COALESCE(SUM(history_events),0) FROM runs),pending_events=(SELECT COALESCE(SUM(pending_events),0) FROM runs),acked_events=(SELECT COALESCE(SUM(acked_events),0) FROM runs),history_bytes=(SELECT COALESCE(SUM(history_bytes),0) FROM runs),media_bytes=(SELECT COALESCE(SUM(media_bytes),0) FROM runs) WHERE singleton=1")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO meta VALUES('store_id',?)", (str(uuid.uuid4()),))
        with self.db:
            self.db.execute("UPDATE producers SET role='worker' WHERE role='auxiliary'")
            if "sync_attempt" not in {r[1] for r in self.db.execute("PRAGMA table_info(runs)")}:
                self.db.execute("ALTER TABLE runs ADD COLUMN sync_attempt INTEGER NOT NULL DEFAULT 0")
            self.db.execute("INSERT OR IGNORE INTO meta VALUES('sync_clock','0')")
            for table in ("scalars", "records"):
                if "context_key" not in {r[1] for r in self.db.execute(f"PRAGMA table_info({table})")}:
                    self.db.execute(f"ALTER TABLE {table} ADD COLUMN context_key TEXT NOT NULL DEFAULT '{{}}'")
                    rows = self.db.execute(f"SELECT rowid,context FROM {table}")
                    while batch := rows.fetchmany(512):
                        self.db.executemany(f"UPDATE {table} SET context_key=? WHERE rowid=?",
                                            [(context_key(json.loads(row[1])),row[0]) for row in batch])
                self.db.execute(f"CREATE INDEX IF NOT EXISTS {table}_context_identity ON {table}(run_id,name,context_key,event_revision)")
            self.db.execute("INSERT INTO meta VALUES('schema_version',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(SCHEMA),))
        self._orphan_scan = None
        self.store_id = self.db.execute("SELECT value FROM meta WHERE key='store_id'").fetchone()[0]

    def close(self):
        with contextlib.suppress(sqlite3.Error):
            self.db.execute("PRAGMA wal_checkpoint(PASSIVE)")
        if self._orphan_scan is not None:
            self._orphan_scan.close()
        self.db.close()

    def register(self, m):
        rid = m["run_id"]
        body = dumps({"run_id": rid, "run_path": m["run_path"], "name": m["name"], "config": m["config"], "tags": m["tags"]})
        old = self.db.execute("SELECT * FROM runs WHERE id=?", (rid,)).fetchone()
        if old and (old["project"] != m["project"] or old["run_path"] != m["run_path"]):
            raise ValueError("run_id already belongs to another project/path")
        if not old:
            t = now()
            self.db.execute("INSERT INTO runs(id,project,run_path,name,config,tags,retention,created_at,updated_at,pending_mode,initial_body,organization) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                            (rid,m["project"],m["run_path"],m["name"],dumps(m["config"]),dumps(m["tags"]),m["retention"],t,t,int(m["sync"] == "background"),body,m.get("organization")))
        elif old["retention"] != m["retention"]:
            raise ValueError("cannot change existing run retention")
        # Once resolved, preserve the selector that established the stable binding.
        if old and old["organization"] is None and old["remote_organization"] is None and m.get("organization") is not None:
            self.db.execute("UPDATE runs SET organization=? WHERE id=?",(m["organization"],rid))
        session = self.db.execute("SELECT * FROM sessions WHERE id=?", (m["session_id"],)).fetchone()
        if not session:
            maximum = self.db.execute("SELECT COALESCE(MAX(ordinal),-1) FROM sessions WHERE run_id=?", (rid,)).fetchone()[0]
            ordinal = m.get("session_ordinal")
            ordinal = maximum + 1 if ordinal is None else int(ordinal)
            self.db.execute("INSERT INTO sessions VALUES(?,?,?,?)", (m["session_id"],rid,ordinal,m["primary_producer"]))
        elif session["run_id"] != rid or session["primary_producer"] != m["primary_producer"] or (m.get("session_ordinal") is not None and session["ordinal"]!=int(m["session_ordinal"])):
            raise ValueError("session manifest conflict")
        producer = self.db.execute("SELECT token FROM producers WHERE run_id=? AND session_id=? AND id=?", (rid,m["session_id"],m["producer_id"])).fetchone()
        if producer and producer[0] != m["token"]:
            raise ValueError("producer already exists: start a new session or producer incarnation")
        self.db.execute("INSERT OR IGNORE INTO producers VALUES(?,?,?,?,?,?)", (rid,m["session_id"],m["producer_id"],m["token"],"primary" if m["producer_id"] == m["primary_producer"] else "worker",int(m["sync"]=="background")))
        if not session or not producer:
            self.db.execute("UPDATE runs SET state='running',updated_at=?,manifest_revision=manifest_revision+1 WHERE id=?", (now(),rid))
        return self.db.execute("SELECT ordinal FROM sessions WHERE id=?", (m["session_id"],)).fetchone()[0]

    def space(self, rid, extra):
        usage = self.db.execute("SELECT history_bytes,media_bytes FROM runs WHERE id=?",(rid,)).fetchone()
        if usage[0] + usage[1] + extra > self.max_run_bytes:
            raise OSError("run quota exceeded; existing history retained")
        if time.monotonic()>=self._next_space_check:
            sizes = {p.name:p.stat().st_size for p in self.root.glob("spool.sqlite3*")}
            if sizes.get("spool.sqlite3-wal",0)>64*1024**2:
                # Checkpoint cannot run inside an event transaction. Reject further
                # events until the collector's between-batch checkpoint succeeds.
                raise OSError("WAL budget exceeded; checkpoint required")
            self._physical_size = sum(sizes.values())
            self._free_space = shutil.disk_usage(self.root).free
            self._next_space_check = time.monotonic()+.25
        media_bytes = self.db.execute("SELECT media_bytes FROM store_stats WHERE singleton=1").fetchone()[0]
        if self._physical_size + media_bytes + extra + 1024*1024 > self.max_bytes:
            raise OSError("RunStore quota exceeded; existing history retained")
        if self._free_space<max(16*1024**2,extra*2):
            raise OSError("insufficient free space")

    def append(self, event):
        body = dumps(event)
        identity = (event["run_id"], event["session_id"], event["producer_id"], int(event["seq"]))
        old = self.db.execute("SELECT hash,revision FROM events WHERE run_id=? AND session_id=? AND producer_id=? AND seq=?", identity).fetchone()
        payload_hash = digest(body)
        if old:
            if old["hash"] != payload_hash:
                raise ValueError("EVENT_IDENTITY_CONFLICT: same event identity has different payload")
            return old["revision"], True
        run = self.db.execute("SELECT pending_mode FROM runs WHERE id=?", (event["run_id"],)).fetchone()
        if not run:
            raise ValueError("run not registered")
        session = self.db.execute("SELECT ordinal FROM sessions WHERE id=? AND run_id=?", (event["session_id"],event["run_id"])).fetchone()
        if not session:
            raise ValueError("session not registered")
        producer = self.db.execute("SELECT pending_mode FROM producers WHERE run_id=? AND session_id=? AND id=?", identity[:3]).fetchone()
        if not producer:
            raise ValueError("producer not registered")
        self.space(event["run_id"], len(body.encode()))
        cur = self.db.execute("INSERT INTO events(run_id,session_id,producer_id,seq,body,hash,pending) VALUES(?,?,?,?,?,?,?)", (*identity,body,payload_hash,producer[0]))
        rev = cur.lastrowid
        step = int(event["step"]) if "step" in event else None
        for metric in (f for f in event.get("fields", []) if f["kind"] == "scalar"):
            self.db.execute("INSERT INTO scalars VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (rev,identity[0],metric["name"],dumps(metric.get("context",{})),step,event.get("epoch"),event.get("elapsed_seconds"),event.get("session_elapsed_seconds"),event["event_time"],metric["value"],metric.get("value_state","finite"),identity[1],identity[2],identity[3],session[0],int(event["processed_samples"]) if "processed_samples" in event else None,context_key(metric.get("context",{}))))
        for record in (f for f in event.get("fields", []) if f["kind"] != "scalar"):
            if record["kind"]=="media":
                aid = record["payload"].get("artifact_id")
                if not self.db.execute("SELECT 1 FROM media WHERE id=? AND run_id=?",(aid,identity[0])).fetchone():
                    raise ValueError("media event references an unavailable artifact")
                self.db.execute("INSERT INTO media_refs VALUES(?,?)",(aid,rev))
            self.db.execute("INSERT INTO records VALUES(?,?,?,?,?,?,?,?)", (rev,identity[0],record["name"],record["kind"],dumps(record.get("context",{})),dumps(record["payload"]),step,context_key(record.get("context",{}))))
        for log in event.get("logs", []):
            self.db.execute("INSERT INTO logs VALUES(?,?,?,?,?)", (rev,identity[0],event["event_time"],log["level"],log["message"]))
        body_bytes = len(body.encode())
        self.db.execute("UPDATE runs SET updated_at=?,history_events=history_events+1,pending_events=pending_events+?,history_bytes=history_bytes+? WHERE id=?",(now(),producer[0],body_bytes,identity[0]))
        self.db.execute("UPDATE store_stats SET history_events=history_events+1,pending_events=pending_events+?,history_bytes=history_bytes+? WHERE singleton=1",(producer[0],body_bytes))
        return rev, False

    def publish_media(self, rid, item):
        old = self.db.execute("SELECT * FROM media WHERE id=?",(item["id"],)).fetchone()
        if old:
            if old["run_id"]!=rid or old["sha256"]!=item["sha256"] or old["metadata"]!=item["metadata"]:
                raise ValueError("media identity conflict")
            return dict(old)
        self.space(rid,item["size"])
        self.db.execute("INSERT INTO media(id,run_id,name,kind,path,size,mime,sha256,metadata) VALUES(?,?,?,?,?,?,?,?,?)",(item["id"],rid,item["name"],item["kind"],item["path"],item["size"],item["mime"],item["sha256"],item["metadata"]))
        self.db.execute("UPDATE runs SET media_bytes=media_bytes+? WHERE id=?",(item["size"],rid))
        self.db.execute("UPDATE store_stats SET media_bytes=media_bytes+? WHERE singleton=1",(item["size"],))
        return item

    def copy_media(self, rid, item):
        # Explicit synchronous store helper; collector uses the independent copy worker.
        return self.publish_media(rid,prepare_media_file(self.root,item))

    def finish(self, rid, state, dropped):
        self.db.execute("UPDATE runs SET manifest_revision=manifest_revision+CASE WHEN state<>? THEN 1 ELSE 0 END,state=?,dropped=MAX(dropped,?),updated_at=? WHERE id=?", (state,state,dropped,now(),rid))

    def mark_sync(self, *, server, project=None, run_id=None, force_remote_check=False):
        server=normalize_server(server)
        clause,args = (" WHERE id=?",(run_id,)) if run_id else (" WHERE project=?",(project,)) if project else ("",())
        rows = self.db.execute("SELECT id,remote_server,history_events,acked_events,pending_events FROM runs"+clause,args).fetchall()
        for r in rows:
            if r["remote_server"] and normalize_server(r["remote_server"]) != server:
                raise ValueError("run is already bound to another server; export/migrate explicitly")
        for r in rows:
            count = r["history_events"]-r["acked_events"]-r["pending_events"]
            self.db.execute("UPDATE events SET pending=1 WHERE run_id=? AND acked=0 AND pending=0", (r["id"],))
            self.db.execute("UPDATE runs SET pending_events=pending_events+? WHERE id=?",(count,r["id"]))
            self.db.execute("UPDATE store_stats SET pending_events=pending_events+? WHERE singleton=1",(count,))
            self.db.execute("UPDATE runs SET pending_mode=1,remote_server=? WHERE id=?", (server,r["id"]))
            if force_remote_check:
                # Older in-flight results must not satisfy this explicit check.
                self.db.execute("UPDATE runs SET manifest_revision=manifest_revision+1 WHERE id=?",(r["id"],))
        return len(rows)

    def apply_sync_result(self, result):
        rid = result["run_id"]
        self.db.execute("UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='sync_clock'")
        self.db.execute("UPDATE runs SET sync_attempt=(SELECT CAST(value AS INTEGER) FROM meta WHERE key='sync_clock') WHERE id=?",(rid,))
        if result.get("error"):
            self.db.execute("UPDATE runs SET last_error=? WHERE id=?",(result["error"][:1024],rid))
        else:
            self.db.execute("UPDATE runs SET last_error=NULL WHERE id=?",(rid,))
        if result.get("project_id"):
            self.db.execute("UPDATE runs SET remote_server=?,remote_project=? WHERE id=?",(result["server"],result["project_id"],rid))
        if result.get("organization_id"):
            self.db.execute("UPDATE runs SET remote_organization=? WHERE id=?",(result["organization_id"],rid))
        if not result.get("error") and "manifest_revision" in result:
            self.db.execute("UPDATE runs SET remote_manifest_revision=MAX(remote_manifest_revision,?) WHERE id=?",(result["manifest_revision"],rid))
        for aid in result.get("media",[]):
            self.db.execute("UPDATE media SET uploaded=1 WHERE id=? AND run_id=?", (aid,rid))
        newly_acked = pending_cleared = 0
        for sid,pid,seq in result.get("accepted",[]):
            old = self.db.execute("SELECT pending,acked FROM events WHERE run_id=? AND session_id=? AND producer_id=? AND seq=?",(rid,sid,pid,int(seq))).fetchone()
            if old:
                newly_acked += int(not old["acked"])
                pending_cleared += old["pending"]
                self.db.execute("UPDATE events SET pending=0,acked=1 WHERE run_id=? AND session_id=? AND producer_id=? AND seq=?",(rid,sid,pid,int(seq)))
        self.db.execute("UPDATE runs SET pending_events=pending_events-?,acked_events=acked_events+? WHERE id=?",(pending_cleared,newly_acked,rid))
        self.db.execute("UPDATE store_stats SET pending_events=pending_events-?,acked_events=acked_events+? WHERE singleton=1",(pending_cleared,newly_acked))
        retention = self.db.execute("SELECT retention FROM runs WHERE id=?",(rid,)).fetchone()[0]
        if retention == "cache":
            deleted = self.db.execute("SELECT revision,length(CAST(body AS BLOB)) body_bytes FROM events WHERE run_id=? AND acked=1 AND pending=0 LIMIT 512",(rid,)).fetchall()
            revisions = [r["revision"] for r in deleted]
            if revisions:
                marks = ','.join('?' for _ in revisions)
                for table, column in (("scalars","event_revision"),("records","event_revision"),("logs","event_revision"),("media_refs","event_revision"),("events","revision")):
                    self.db.execute(f"DELETE FROM {table} WHERE {column} IN ({marks})",revisions)
                removed_bytes = sum(r["body_bytes"] for r in deleted)
                self.db.execute("UPDATE runs SET deletion_generation=deletion_generation+1,history_events=history_events-?,acked_events=acked_events-?,history_bytes=history_bytes-? WHERE id=?",(len(revisions),len(revisions),removed_bytes,rid))
                self.db.execute("UPDATE store_stats SET history_events=history_events-?,acked_events=acked_events-?,history_bytes=history_bytes-? WHERE singleton=1",(len(revisions),len(revisions),removed_bytes))
                unused = self.db.execute("SELECT id,path,size FROM media WHERE run_id=? AND uploaded=1 AND NOT EXISTS(SELECT 1 FROM media_refs WHERE artifact_id=media.id) LIMIT 16",(rid,)).fetchall()
                for item in unused:
                    self.db.execute("INSERT OR IGNORE INTO gc_files VALUES(?)",(item["path"],))
                    self.db.execute("DELETE FROM media WHERE id=?",(item["id"],))
                    self.db.execute("UPDATE runs SET media_bytes=media_bytes-? WHERE id=?",(item["size"],rid))
                    self.db.execute("UPDATE store_stats SET media_bytes=media_bytes-? WHERE singleton=1",(item["size"],))

    def discard_unreferenced_media(self, item):
        # Called after rollback, on the sole database writer. Protect retries
        # whose artifact was already committed by a previous request.
        with self.db:
            if not self.db.execute("SELECT 1 FROM media WHERE path=?",(item["path"],)).fetchone():
                self.db.execute("INSERT OR IGNORE INTO gc_files VALUES(?)",(item["path"],))

    def maintenance(self):
        """Bounded work only between write batches; never inside training."""
        with contextlib.suppress(sqlite3.Error):
            self.db.execute("PRAGMA wal_checkpoint(PASSIVE)")
        try:
            self._maintain_files()
        except (sqlite3.Error, OSError):
            # Disk-full/read-only/transient IO must not kill the collector. The
            # next tick restarts reconciliation after storage becomes writable.
            if self._orphan_scan is not None:
                self._orphan_scan.close()
                self._orphan_scan = None

    def _maintain_files(self):
        # Reconcile crash leftovers incrementally; fresh files may still be in
        # the media worker's completion queue and must not be collected.
        if self._orphan_scan is None:
            self._orphan_scan = os.scandir(self.root/"media")
        for _ in range(32):
            entry = next(self._orphan_scan, None)
            if entry is None:
                self._orphan_scan.close()
                self._orphan_scan = None
                break
            if entry.is_file(follow_symlinks=False) and time.time()-entry.stat().st_mtime>3600:
                self.discard_unreferenced_media({"path":str(Path("media")/entry.name)})
        files = self.db.execute("SELECT path FROM gc_files LIMIT 16").fetchall()
        for row in files:
            path = (self.root/row[0]).resolve()
            if path.parent==self.root/"media" and not self.db.execute("SELECT 1 FROM media WHERE path=?",(row[0],)).fetchone():
                with contextlib.suppress(FileNotFoundError):
                    path.unlink()
            with self.db:
                self.db.execute("DELETE FROM gc_files WHERE path=?",(row[0],))

    def status(self, rid=None, *, project=None):
        clause,args = (" AND id=?",(rid,)) if rid else (" AND project=?",(project,)) if project else ("",())
        if project and not rid:
            row = self.db.execute("SELECT SUM(history_events),SUM(pending_events),SUM(acked_events),SUM(history_bytes),SUM(media_bytes) FROM runs WHERE 1=1"+clause,args).fetchone()
            row = tuple(value or 0 for value in row)
        else:
            row = self.db.execute("SELECT history_events,pending_events,acked_events,history_bytes,media_bytes FROM " + ("runs WHERE id=?" if rid else "store_stats WHERE singleton=1"),(rid,) if rid else ()).fetchone()
        if not row:
            row = (0,0,0,0,0)
        errors = [r[0] for r in self.db.execute("SELECT last_error FROM runs WHERE last_error IS NOT NULL"+clause+" LIMIT 128",args)]
        # Metadata/session/terminal-state publication is a separate checkpoint
        # from event ACKs, including for runs that recorded zero events.
        incomplete = self.db.execute("SELECT COUNT(*) FROM runs WHERE pending_mode=1 AND remote_manifest_revision<manifest_revision"+clause,args).fetchone()[0]
        return {"store_id":self.store_id,"history_events":row[0],"pending_events":row[1],"acked_events":row[2],"history_bytes":row[3],"media_bytes":row[4],"sync_errors":errors,"remote_incomplete_runs":incomplete,"remote_complete":incomplete==0 and row[1]==0 and not errors,"updated_at":now()}


def prepare_media_file(root, item, *, max_bytes=256*1024**2):
    """Stream and fsync one immutable media copy without touching SQLite."""
    root,source = Path(root),Path(item["path"])
    aid = str(uuid.UUID(item["artifact_id"]))
    final,temporary = root/"media"/aid,root/"incoming"/aid
    with source.open("rb") as src:
        info = os.fstat(src.fileno())
        if not __import__('stat').S_ISREG(info.st_mode) or info.st_size>max_bytes:
            raise ValueError("media must be a regular file within quota (max256 MiB)")
        if shutil.disk_usage(root).free<max(16*1024**2,info.st_size*2):
            raise OSError("insufficient media copy space")
        hasher,total = hashlib.sha256(),0
        try:
            with temporary.open("wb") as dst:
                while block:=src.read(1024*1024):
                    total+=len(block)
                    if total>info.st_size or total>max_bytes:
                        raise ValueError("media grew during copy")
                    dst.write(block)
                    hasher.update(block)
                dst.flush()
                os.fsync(dst.fileno())
            after=os.fstat(src.fileno())
            if total!=info.st_size or after.st_mtime_ns!=info.st_mtime_ns:
                raise ValueError("media changed during copy")
            if final.exists():
                # Reconnect can resend a durable media message. Compare the new copy
                # before replacing, preserving the immutable UUID content invariant.
                previous=hashlib.sha256()
                with final.open("rb") as old:
                    while block:=old.read(1024*1024):
                        previous.update(block)
                if previous.digest()!=hasher.digest():
                    raise ValueError("media identity conflict")
                temporary.unlink()
            else:
                os.replace(temporary,final)
            fd=os.open(final.parent,os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()
    return {"id":aid,"name":item["name"],"kind":item.get("kind","file"),"path":str(Path("media")/aid),"size":total,"mime":mimetypes.guess_type(str(source))[0] or "application/octet-stream","sha256":hasher.hexdigest(),"metadata":dumps(item.get("metadata",{}))}
