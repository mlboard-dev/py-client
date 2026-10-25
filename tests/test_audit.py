import json
import os
import uuid
from unittest.mock import patch
from test_client import TempCase, manifest, event
from mlboard.store import RunStore, prepare_media_file, readonly
from mlboard.query import LocalQuery, QueryError, project_id, run_dict, series_directory
from mlboard.query_limits import QUERY_LIMITS
from mlboard.transport import sync_pass
from mlboard.viewer import ViewerServer

class AuditRegressionTests(TempCase):
    def test_summary_and_directory_merge_numeric_context_before_paging(self):
        store=RunStore(self.root)
        m=manifest()
        with store.db:
            store.register(m)
            for seq,(fold,value,step) in enumerate(((1.0,9,0),(1,8,0),(1.0,3,1),(True,7,0),("1",6,0))):
                body=event(m,seq,value,step=step)
                body["fields"][0]["context"]={"fold":fold}
                body["fields"].append({"name":"details","kind":"text","context":{"fold":fold},"payload":{"schema_version":"mlboard.text/v1","text":str(seq)}})
                store.append(body)
        originals=[tuple(r) for r in store.db.execute("SELECT body,hash FROM events ORDER BY revision")]
        for schema in (4,3):
            with self.subTest(schema=schema):
                if schema==3:
                    with store.db:
                        for table in ("scalars","records"):
                            store.db.execute(f"DROP INDEX {table}_context_identity")
                            store.db.execute(f"ALTER TABLE {table} DROP COLUMN context_key")
                        store.db.execute("UPDATE meta SET value='3' WHERE key='schema_version'")
                with readonly(self.root) as db:
                    summary=run_dict(db,db.execute("SELECT * FROM runs").fetchone())["summary"]
                    self.assertEqual(len(summary),3) # true, 1 and "1" remain distinct
                    numeric=next(x for x in summary.values() if type(x["context"]["fold"]) is int)
                    self.assertEqual((numeric["count"],numeric["min"],numeric["max"],numeric["last"]),(2,3,8,3))
                    # Request one directory entry per page, never split aliases.
                    entries=[dict(series_directory(db,m["run_id"],1,i)[0]) for i in range(6)]
                    self.assertEqual(len({(r["name"],r["kind"],r["context"]) for r in entries}),6)
                    self.assertEqual([r["count"] for r in entries if r["context"]=='{"fold":1}'],[3,3])
                    self.assertEqual(series_directory(db,m["run_id"],1,6),[])
                    self.assertEqual(db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0],str(schema))
                q={"runs":{"run_ids":[m["run_id"]]},"source":{"metrics":[{"name":"train/loss","context":{"fold":1}}]},"x":{"field":"step"},"transform":{"kind":"raw"}}
                result=LocalQuery(self.root).execute(project_id("vision"),q)
                self.assertEqual(result["frames"][0]["row_count"],numeric["count"])
                self.assertEqual([tuple(r) for r in store.db.execute("SELECT body,hash FROM events ORDER BY revision")],originals)
        store.close()

    def test_numeric_context_indexes_preserve_event_identity_and_migrate(self):
        store=RunStore(self.root)
        m=manifest(); e=event(m,0); e["fields"][0]["context"]={"fold":1.0,"zero":-0.0}
        with store.db:
            store.register(m); store.append(e)
        original=tuple(store.db.execute("SELECT body,hash FROM events").fetchone())
        q={"source":{"kind":"scalar","metrics":[{"name":"train/loss","context":{"fold":1,"zero":0}}]},"x":{"field":"step"},"transform":{"kind":"raw"}}
        self.assertEqual(LocalQuery(self.root).execute(project_id("vision"),q)["frames"][0]["row_count"],1)
        with store.db:
            for table in ("scalars","records"):
                store.db.execute(f"DROP INDEX {table}_context_identity")
                store.db.execute(f"ALTER TABLE {table} DROP COLUMN context_key")
            store.db.execute("UPDATE meta SET value='3' WHERE key='schema_version'")
        store.close(); store=RunStore(self.root)
        try:
            self.assertEqual(LocalQuery(self.root).execute(project_id("vision"),q)["frames"][0]["row_count"],1)
            self.assertEqual(tuple(store.db.execute("SELECT body,hash FROM events").fetchone()),original)
        finally: store.close()

    def test_legacy_secondary_producer_and_fair_queue(self):
        store=RunStore(self.root)
        config={"server":"http://127.0.0.1:9999"}
        with store.db:
            for i in range(129):
                m=manifest(sync="background"); store.register(m)
            store.mark_sync(server=config["server"])
        with patch("mlboard.transport.HTTP.request",side_effect=OSError("offline")):
            first=sync_pass(self.root,config)
            with store.db:
                for outcome in first: store.apply_sync_result(outcome)
            second=sync_pass(self.root,config)
        self.assertEqual(len(first),128)
        self.assertTrue({r["run_id"] for r in second}-{r["run_id"] for r in first})
        with store.db: store.db.execute("UPDATE producers SET role='auxiliary'")
        store.close(); store=RunStore(self.root)
        self.assertEqual(store.db.execute("SELECT DISTINCT role FROM producers").fetchone()[0],"worker")
        store.close()

    def test_rollback_orphan_and_committed_retry_are_distinguished(self):
        store=RunStore(self.root); m=manifest()
        with store.db: store.register(m)
        source=self.root/"sample.txt"; source.write_text("abc")
        item=prepare_media_file(self.root,{"artifact_id":str(uuid.uuid4()),"path":str(source),"name":"sample"})
        store.max_run_bytes=4
        try:
            with store.db:
                store.publish_media(m["run_id"],item); store.append(event(m,0))
        except OSError: store.discard_unreferenced_media(item)
        else: self.fail("expected combined file/event quota failure")
        store.maintenance()
        self.assertFalse((self.root/item["path"]).exists())
        store.max_run_bytes=10000
        item=prepare_media_file(self.root,{"artifact_id":str(uuid.uuid4()),"path":str(source),"name":"sample"})
        with store.db: store.publish_media(m["run_id"],item)
        store.discard_unreferenced_media(item); store.maintenance()
        self.assertTrue((self.root/item["path"]).exists())
        orphan=self.root/"media"/str(uuid.uuid4()); orphan.write_text("orphan"); os.utime(orphan,(0,0))
        store.maintenance(); store.maintenance()
        self.assertFalse(orphan.exists()); store.close()

    def test_wildcard_requires_explicit_origin(self):
        store=RunStore(self.root);store.close()
        with self.assertRaisesRegex(ValueError,"public-url"):
            ViewerServer(self.root,("0.0.0.0",0),token="test")
        viewer=ViewerServer(self.root,("0.0.0.0",0),token="test",public_url="http://192.168.1.42:8081")
        try:
            self.assertIn("192.168.1.42:8081",viewer.allowed_hosts)
            self.assertNotIn("evil.invalid:8081",viewer.allowed_hosts)
        finally: viewer.server_close()

    def test_orphan_maintenance_recovers_from_unwritable_database(self):
        store=RunStore(self.root); m=manifest()
        with store.db: store.register(m)
        source=self.root/"sample.txt"; source.write_text("retained")
        item=prepare_media_file(self.root,{"artifact_id":str(uuid.uuid4()),"path":str(source),"name":"sample"})
        with store.db: store.publish_media(m["run_id"],item)
        retained=self.root/item["path"]; os.utime(retained,(0,0))
        orphan=self.root/"media"/str(uuid.uuid4()); orphan.write_text("orphan"); os.utime(orphan,(0,0))
        store.db.execute("PRAGMA query_only=ON")
        try:
            store.maintenance() # Registration of the aged orphan fails; collector keeps running.
            self.assertTrue(orphan.exists())
            self.assertTrue(retained.exists())
        finally:
            store.db.execute("PRAGMA query_only=OFF")
        store.maintenance()
        self.assertFalse(orphan.exists())
        self.assertTrue(retained.exists())
        self.assertEqual(store.db.execute("SELECT COUNT(*) FROM media").fetchone()[0],1)
        store.close()

    def test_query_execution_uses_advertised_point_ceiling(self):
        store=RunStore(self.root); m=manifest()
        with store.db:
            store.register(m)
            for seq in range(3): store.append(event(m,seq))
        query={"source":{"kind":"scalar","metrics":[{"name":"train/loss"}]},"page":{"limit":2}}
        with patch.dict(QUERY_LIMITS,{"max_points":2}):
            with self.assertRaises(QueryError) as result:
                LocalQuery(self.root).execute(project_id("vision"),query)
            self.assertEqual(result.exception.code,"BUDGET_EXCEEDED")
        self.assertEqual(LocalQuery(self.root).execute(project_id("vision"),query)["page"]["returned_rows"],2)
        store.close()
