import json
import os
import uuid
from unittest.mock import patch
from test_client import TempCase, manifest, event
from mlboard.store import RunStore, prepare_media_file
from mlboard.query import LocalQuery, QueryError, project_id
from mlboard.query_limits import QUERY_LIMITS
from mlboard.transport import sync_pass
from mlboard.viewer import ViewerServer

class AuditRegressionTests(TempCase):
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
