from __future__ import annotations

import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import signal
import threading
import unittest
import urllib.parse
import urllib.request

import mlboard
from mlboard.cli import sync
from mlboard.ipc import command
from mlboard.query import LocalQuery, QueryError, min_max, project_id, run_dict
from mlboard.sdk import Run
from mlboard.store import RunStore, readonly
from mlboard.transport import sync_pass
from mlboard.validation import ValidationError, dumps
from mlboard.viewer import ViewerServer
from test_client import TempCase, StoreFixture, event, manifest


class Remote:
    """Real HTTP peer with observable auth, durable event identities and revisions."""
    def __init__(self, *, project="vision", patch_failure=None):
        self.project=project
        self.patch_failure=patch_failure
        self.requests=[]
        self.events={}
        self.states={}
        self.versions={}
        self.patches=[]
        peer=self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def reply(self, value, status=200):
                raw=dumps(value).encode()
                self.send_response(status)
                self.send_header("Content-Length",str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            def remember(self):
                peer.requests.append((self.command,self.path,self.headers.get("Authorization")))
            def body(self):
                return json.loads(self.rfile.read(int(self.headers.get("Content-Length",0))))
            def do_GET(self):
                self.remember()
                if self.path=="/api/v1/projects":
                    return self.reply({"projects":[{"id":"project-"+peer.project,"slug":peer.project}]})
                parts=urllib.parse.urlsplit(self.path)
                rid=parts.path.rsplit("/",1)[-1]
                if urllib.parse.parse_qs(parts.query).get("include_summary") != ["false"]:
                    return self.reply({"error":{"code":"summary_not_requested"}},422)
                self.reply({"id":rid,"state":peer.states.get(rid,"running"),"run_version":str(peer.versions.get(rid,1))})
            def do_POST(self):
                self.remember()
                body=self.body()
                if self.path.endswith("/events"):
                    accepted=[]
                    for e in body["events"]:
                        identity=(e["run_id"],e["session_id"],e["producer_id"],e["seq"])
                        old=peer.events.setdefault(identity,e)
                        if old!=e:
                            return self.reply({"error":{"code":"conflict"}},409)
                        accepted.append({"session_id":e["session_id"],"producer_id":e["producer_id"],"seq":e["seq"]})
                    return self.reply({"accepted":accepted})
                self.reply({})
            def do_PATCH(self):
                self.remember()
                body=self.body()
                rid=self.path.rsplit("/",1)[-1]
                peer.patches.append(self.headers.get("If-Match"))
                if peer.patch_failure=="always":
                    return self.reply({"error":{"code":"state_write_failed","message":"untrusted response text"}},500)
                if peer.patch_failure=="conflict_once" and len(peer.patches)==1:
                    peer.versions[rid]=2
                    return self.reply({"error":{"code":"revision_conflict"}},412)
                if self.headers.get("If-Match")!=str(peer.versions.get(rid,1)):
                    return self.reply({"error":{"code":"revision_conflict"}},412)
                peer.states[rid]=body["state"]
                peer.versions[rid]=peer.versions.get(rid,1)+1
                self.reply({"id":rid,"state":body["state"],"run_version":str(peer.versions[rid])})

        self.server=ThreadingHTTPServer(("127.0.0.1",0),Handler)
        self.url=f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever,daemon=True).start()
    def __enter__(self): return self
    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()


class RemoteCheckpointTests(TempCase):
    def test_zero_event_manifest_is_pending_until_terminal_state_confirmed(self):
        with Remote() as remote:
            store=RunStore(self.root)
            try:
                m=manifest(sync="background")
                with store.db:
                    store.register(m)
                    store.mark_sync(server=remote.url)
                    store.finish(m["run_id"],"finished",0)
                self.assertFalse(store.status()["remote_complete"])
                self.assertEqual(store.status()["pending_events"],0)
                with store.db:
                    for result in sync_pass(self.root,{"server":remote.url}):
                        store.apply_sync_result(result)
                self.assertTrue(store.status()["remote_complete"])
                self.assertEqual(remote.states[m["run_id"]],"finished")
                reads=[path for method,path,_ in remote.requests if method=="GET" and "/runs/" in path]
                self.assertEqual(reads,[f"/api/v1/runs/{m['run_id']}?include_summary=false"])
                row=store.db.execute("SELECT manifest_revision,remote_manifest_revision FROM runs").fetchone()
                self.assertEqual(row[0],row[1])
            finally:
                store.close()

    def test_acked_events_do_not_hide_failed_terminal_patch(self):
        with Remote(patch_failure="always") as remote:
            run=mlboard.init("vision","remote/failing-terminal",spool_dir=self.root,sync="background",server=remote.url)
            self.assertTrue(run.log({"loss":1},step=0).accepted)
            receipt=run.finish(timeout=1.2,wait_for="remote")
            self.assertFalse(receipt.complete)
            self.assertEqual(receipt.durable,1)
            self.assertIn("state_write_failed",receipt.reason)
            self.assertNotIn("untrusted response text",receipt.reason)
            with readonly(self.root) as db:
                row=db.execute("SELECT pending_events,manifest_revision,remote_manifest_revision FROM runs").fetchone()
                self.assertEqual(row[0],0)
                self.assertGreater(row[1],row[2])
            with self.assertRaisesRegex(RuntimeError,"state_write_failed"):
                sync(self.root,server=remote.url,timeout=.4)

    def test_zero_event_sdk_finish_checks_remote_metadata(self):
        with Remote(patch_failure="always") as remote:
            run=mlboard.init("vision","remote/empty",spool_dir=self.root,sync="background",server=remote.url)
            receipt=run.finish(timeout=.7,wait_for="remote")
            self.assertFalse(receipt.complete)
            self.assertEqual(receipt.accepted,0)
            self.assertFalse(run.status()["remote_complete"])
        with Remote() as remote:
            run=mlboard.init("vision","remote/empty-success",spool_dir=self.root,sync="background",server=remote.url)
            self.assertTrue(run.finish(timeout=3,wait_for="remote").complete)
            self.assertEqual(remote.states[run.id],"finished")

    def test_revision_conflict_reloads_before_retry(self):
        with Remote(patch_failure="conflict_once") as remote:
            run=mlboard.init("vision","remote/revision",spool_dir=self.root,sync="background",server=remote.url)
            self.assertTrue(run.finish(timeout=3,wait_for="remote").complete)
            self.assertEqual(remote.patches,["1","2"])

    def test_remote_finish_after_local_finish_refreshes_its_checkpoint(self):
        with Remote() as remote:
            run=mlboard.init("vision","remote/repeated-finish",spool_dir=self.root,sync="background",server=remote.url)
            self.assertTrue(run.finish(timeout=3,wait_for="local").complete)
            self.assertTrue(sync(self.root,server=remote.url,timeout=3)["remote_complete"])
            self.assertTrue(run.finish(timeout=.5,wait_for="remote").complete)

    def test_remote_finish_after_collector_restart_restores_credentials(self):
        with Remote(patch_failure="always") as remote:
            run=mlboard.init("vision","remote/restarted-finish",spool_dir=self.root,sync="background",server=remote.url,api_key="key-original")
            self.assertTrue(run.finish(timeout=3,wait_for="local").complete)
            pid=json.loads((self.root/"collector.json").read_text())["pid"]
            os.kill(pid,signal.SIGTERM)
            with contextlib.suppress(ChildProcessError): os.waitpid(pid,0)
            remote.patch_failure=None
            self.assertTrue(run.finish(timeout=3,wait_for="remote").complete)
            self.assertEqual(remote.states[run.id],"finished")
            self.assertTrue(all(key=="Bearer key-original" for _,_,key in remote.requests))

    def test_collector_restart_does_not_borrow_new_run_key(self):
        with Remote(project="first") as first, Remote(project="second") as second:
            store=RunStore(self.root)
            a,b=manifest(sync="background"),manifest(sync="background")
            a["project"],b["project"]="first","second"
            with store.db:
                for m,remote in ((a,first),(b,second)):
                    store.register(m)
                    store.append(event(m,0))
                    store.finish(m["run_id"],"finished",0)
                    store.mark_sync(server=remote.url,run_id=m["run_id"])
            store.close()
            # A fresh process knows only the new registration's second-server key.
            run=mlboard.init("second","group/test",run_id=b["run_id"],spool_dir=self.root,sync="background",server=second.url,api_key="key-second")
            self.assertTrue(run.finish(timeout=3,wait_for="remote").complete)
            self.assertEqual(first.requests,[])
            self.assertTrue(second.requests)
            self.assertTrue(all(key=="Bearer key-second" for _,_,key in second.requests))
            self.assertFalse(any(a["run_id"] in path for _,path,_ in second.requests))
            with readonly(self.root) as db:
                self.assertEqual(db.execute("SELECT pending_events FROM runs WHERE id=?",(a["run_id"],)).fetchone()[0],1)
            # Explicit mismatches are rejected before HTTP; same-origin work units
            # never discover the unrelated server's retained run.
            mismatch=sync_pass(self.root,{"runs":{a["run_id"]:{"server":second.url,"api_key":"key-second"}}})
            self.assertIn("REMOTE_TARGET_MISMATCH",mismatch[0]["error"])
            self.assertEqual(sync_pass(self.root,{"server":second.url,"api_key":"key-second"}),[])
            with self.assertRaisesRegex(RuntimeError,"another server"):
                command(self.root,{"kind":"sync","project":"first","server":second.url,"api_key":"key-second"})
            # A project-scoped CLI checkpoint excludes another project's backlog.
            status=sync(self.root,server=second.url,api_key="key-second",project="second",timeout=2)
            self.assertTrue(status["remote_complete"])
            self.assertEqual(first.requests,[])


class QueryRegressionTests(StoreFixture):
    def spec(self, **extra):
        return {"runs":{"run_ids":[self.m["run_id"]]},"source":{"kind":"scalar","metrics":[{"name":"train/loss","context":{}}]},**extra}

    def test_exact_int63_x_bounds_and_typed_columns(self):
        big=2**63-3
        with self.store.db:
            for seq in range(3):
                e=event(self.m,seq,seq,step=big+seq)
                e["processed_samples"]=str(big+seq)
                self.store.append(e)
        query=LocalQuery(self.root)
        for axis in ("step","processed_samples"):
            result=query.execute(project_id("vision"),self.spec(x={"field":axis,"range":{"gte":str(big+1),"lte":str(big+1)}},transform={"kind":"raw"}))
            frame=result["frames"][0]
            self.assertEqual(frame["columns"]["x"],[str(big+1)])
            self.assertEqual(frame["columns"]["value"],[1])
            self.assertEqual(next(f["type"] for f in frame["fields"] if f["name"]=="x"),"int64")
        with self.assertRaises(QueryError):
            query.execute(project_id("vision"),self.spec(x={"field":"step","range":{"gte":float(big)}}))

    def test_wall_time_offsets_sort_and_nanosecond_bounds(self):
        times=["2026-10-23T12:00:00+08:00","2026-10-23T05:00:00.000000001Z","2026-10-23T05:00:00.000000002Z"]
        with self.store.db:
            for seq,stamp in enumerate(times):
                e=event(self.m,seq,seq)
                e["event_time"]=stamp
                self.store.append(e)
        result=LocalQuery(self.root).execute(project_id("vision"),self.spec(x={"field":"wall_time","range":{"gte":"2026-10-23T13:00:00.000000001+08:00","lte":"2026-10-23T05:00:00.000000001Z"}}))
        frame=result["frames"][0]
        self.assertEqual(frame["columns"]["value"],[1])
        self.assertEqual(frame["columns"]["x"],[times[1]])
        self.assertEqual(next(f["type"] for f in frame["fields"] if f["name"]=="x"),"datetime")

    def test_cursor_is_bound_to_semantics_and_survives_single_page_refresh(self):
        with self.store.db:
            for seq in range(4): self.store.append(event(self.m,seq,seq))
        query=LocalQuery(self.root)
        spec=self.spec(page={"limit":1})
        first=query.execute(project_id("vision"),spec)
        cursor=first["page"]["next_cursor"]
        changed=self.spec(page={"limit":1,"cursor":cursor},source={"kind":"scalar","metrics":[{"name":"other","context":{}}]})
        with self.assertRaises(QueryError) as exc: query.execute(project_id("vision"),changed)
        self.assertEqual(exc.exception.status,410)
        for _ in range(16): query.execute(project_id("vision"),self.spec())
        self.assertEqual(len(query.snapshots),1)
        spec["page"]["cursor"]=cursor
        second=query.execute(project_id("vision"),spec)
        self.assertEqual(second["frames"][0]["columns"]["value"],[1])
        self.assertEqual(first["consistency"],second["consistency"])

    def test_unimplemented_fields_are_rejected_explicitly(self):
        query=LocalQuery(self.root)
        for extra in ({"consistency":{"summary":"exact"}},{"reuse":{"snapshot_id":"x"}},{"viewport":{"lod":"auto"}},{"read":{"at_revision":{self.m["run_id"]:"1"}}},{"budget":{"on_exceed":"partial"}},{"transform":{"kind":"downsample","algorithm_version":"2"}},{"x":{"field":"step","invented":1}}):
            with self.subTest(extra=extra), self.assertRaises(QueryError) as exc:
                query.execute(project_id("vision"),self.spec(**extra))
            self.assertEqual(exc.exception.code,"UNSUPPORTED_OPERATION")

    def test_category_prefix_excludes_same_named_leaf(self):
        leaf=manifest();leaf["run_path"]="group"
        other=manifest();other["run_path"]="groupish/test"
        with self.store.db:
            self.store.append(event(self.m,0))
            for m in (leaf,other):
                self.store.register(m);self.store.append(event(m,0))
        query=LocalQuery(self.root)
        result=query.execute(project_id("vision"),self.spec(runs={"path_prefixes":["group"]}))
        self.assertEqual([f["meta"]["run_id"] for f in result["frames"]],[self.m["run_id"]])
        result=query.execute(project_id("vision"),self.spec(runs={"path_prefixes":["group"],"include_ungrouped":True}))
        self.assertEqual({f["meta"]["run_id"] for f in result["frames"]},{self.m["run_id"],leaf["run_id"]})
        for selection in ({"path_prefixes":[]},{"path_prefixes":[],"include_ungrouped":False},{"include_ungrouped":False}):
            self.assertEqual(query.execute(project_id("vision"),self.spec(runs=selection))["frames"],[])
        for selection in ({"path_prefixes":[],"include_ungrouped":True},{"include_ungrouped":True}):
            result=query.execute(project_id("vision"),self.spec(runs=selection))
            self.assertEqual([f["meta"]["run_id"] for f in result["frames"]],[leaf["run_id"]])
        result=query.execute(project_id("vision"),self.spec(runs={}))
        self.assertEqual(len(result["frames"]),3)

    def test_minmax_uses_actual_x_extrema_and_gap_neighbors(self):
        xs=list(range(97))+[1000,1001,2000]
        rows=[{"_x":x,"value":-60 if i==60 else 50 if i==50 else 0,"value_state":"finite","seq":i} for i,x in enumerate(xs)]
        sampled=min_max(rows,14)
        self.assertEqual({r["seq"] for r in sampled},{0,50,60,96,97,98,99})
        for i in (2,3,4): rows[i].update(value=None,value_state="nan")
        sampled=min_max(rows,14)
        self.assertTrue({1,2,4,5}<={r["seq"] for r in sampled})
        self.assertIn(-60,[r["value"] for r in sampled])
        self.assertIn(50,[r["value"] for r in sampled])
        with self.assertRaisesRegex(QueryError,"cannot fit"):
            min_max(rows,8)

    def test_summary_keeps_context_identity_and_last_logical_order(self):
        a={"split":"val","fold":1}
        b={"split":"val","fold":2}
        collision="train/loss "+dumps(a)
        with self.store.db:
            first=event(self.m,0,9,step=100)
            first["fields"][0]["context"]=a
            self.store.append(first)
            second=event(self.m,1,8,step=0)
            second["fields"]=[{"name":"train/loss","kind":"scalar","context":a,"value":8,"value_state":"finite"},{"name":"train/loss","kind":"scalar","context":b,"value":5,"value_state":"finite"},{"name":collision,"kind":"scalar","context":{},"value":7,"value_state":"finite"}]
            self.store.append(second)
            resumed=manifest(rid=self.m["run_id"])
            self.store.register(resumed)
            last=event(resumed,0,1,step=0)
            last["fields"][0]["context"]=a
            self.store.append(last)
        summary=run_dict(self.store.db,self.store.db.execute("SELECT * FROM runs").fetchone())["summary"]
        self.assertEqual(len(summary),3)
        self.assertEqual(summary[dumps(["train/loss",a])]["last"],1)
        self.assertEqual(summary[dumps(["train/loss",a])]["count"],2)
        self.assertEqual(summary[dumps(["train/loss",a])]["max"],9)
        self.assertEqual(summary[dumps(["train/loss",b])]["last"],5)
        self.assertEqual(summary[dumps([collision,{}])]["last"],7)
        self.assertTrue(all("name" in item and "context" in item for item in summary.values()))


class SDKValidationRegressions(unittest.TestCase):
    def test_count_based_curves_reject_macro_and_weighted_labels(self):
        counts={"thresholds":[float("-inf"),float("inf")],"tp":[1,0],"fp":[1,0],"fn":[0,1],"tn":[0,1],"step":0}
        for mode in ("safe","strict"):
            run=Run("vision","native/averaging",mode=mode,_start_sender=False)
            for averaging in ("binary","micro","per_class"):
                self.assertTrue(run.log_threshold_curve("val/curve",averaging=averaging,**counts).accepted)
            for averaging in ("macro","weighted"):
                if mode=="strict":
                    with self.assertRaisesRegex(ValidationError,"count-based"):
                        run.log_threshold_curve("val/curve",averaging=averaging,**counts)
                else:
                    self.assertFalse(run.log_threshold_curve("val/curve",averaging=averaging,**counts).accepted)

    def test_native_invalid_steps_follow_safe_and_strict_semantics(self):
        for mode in ("safe","strict"):
            run=Run("vision","native/validation",mode=mode,_start_sender=False)
            for bad in (-1,"0",True,2**63):
                for helper in (lambda:run.log_histogram("weights",bin_edges=[0,1],counts=[1],step=bad),lambda:run.log_confusion_matrix("confusion",[[1]],step=bad)):
                    if mode=="strict":
                        with self.assertRaises(ValidationError): helper()
                    else:
                        self.assertFalse(helper().accepted)
        with self.assertRaises(ValidationError):
            Run("vision","native/name",name="a"*201,_start_sender=False)


class ViewerPagingRegressions(TempCase):
    def test_same_event_records_and_logs_paginate_without_loss(self):
        store=RunStore(self.root)
        m=manifest()
        body=event(m,0)
        body["fields"]=[{"name":"details","kind":"text","context":{"fold":i},"payload":{"schema_version":"mlboard.text/v1","text":f"记录 {i}"}} for i in range(7)]
        body["logs"]=[{"level":"info","message":f"line {i}"} for i in range(5)]
        with store.db:
            store.register(m);store.append(body)
        store.close()
        server=ViewerServer(self.root,("127.0.0.1",0))
        threading.Thread(target=server.serve_forever,daemon=True).start()
        base=f"http://127.0.0.1:{server.server_port}/api/v1"
        def get(path):
            with urllib.request.urlopen(urllib.request.Request(base+path,headers={"Authorization":"Bearer "+server.access_token})) as response:
                return json.load(response)
        try:
            for kind,expected in (("records",7),("logs",5)):
                cursor=None;values=[]
                while True:
                    params={"limit":2}
                    if cursor: params["cursor"]=cursor
                    page=get("/runs/"+m["run_id"]+"/"+kind+"?"+urllib.parse.urlencode(params))
                    values.extend(page[kind]);cursor=page["next_cursor"]
                    if not cursor: break
                self.assertEqual(len(values),expected)
                self.assertEqual(len({dumps(item) for item in values}),expected)
            self.assertEqual(get("/local/status")["history_bytes"],len(dumps(body).encode()))
        finally:
            server.shutdown();server.server_close()

    def test_run_directory_empty_prefix_and_literal_search(self):
        store=RunStore(self.root)
        manifests=[]
        with store.db:
            for path in ("group","group/test","groupish/test","special%_value","specialXYvalue"):
                m=manifest();m["run_path"]=path;manifests.append(m);store.register(m)
        store.close()
        server=ViewerServer(self.root,("127.0.0.1",0))
        threading.Thread(target=server.serve_forever,daemon=True).start()
        base=f"http://127.0.0.1:{server.server_port}/api/v1/projects/"+project_id("vision")+"/runs?"
        def paths(params):
            request=urllib.request.Request(base+urllib.parse.urlencode(params),headers={"Authorization":"Bearer "+server.access_token})
            with urllib.request.urlopen(request) as response:
                return {r["run_path"] for r in json.load(response)["runs"]}
        try:
            self.assertEqual(paths({"path_prefix":""}),{"group","special%_value","specialXYvalue"})
            self.assertEqual(paths({"path_prefix":"group"}),{"group/test"})
            self.assertEqual(paths({"search":"%_"}),{"special%_value"})
        finally:
            server.shutdown();server.server_close()


if __name__=="__main__": unittest.main()
