from __future__ import annotations

import contextlib
from dataclasses import asdict
import hashlib
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import importlib.util
import json
import math
import os
from pathlib import Path
import signal
import socket
import sqlite3
import struct
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request
import uuid

import mlboard
from mlboard.ipc import command
from mlboard.query import LocalQuery, QueryError, project_id
from mlboard.sdk import Run
from mlboard.store import RunStore, readonly
from mlboard.tensorboard import crc32c,masked_crc,records
from mlboard.transport import HTTP, SyncError,sync_pass
from mlboard.validation import ValidationError,dumps,json_value,run_path
from mlboard.viewer import ViewerServer

ROOT=Path(__file__).resolve().parents[3]
TMP=ROOT/'outputs'/'tmp'
TMP.mkdir(parents=True,exist_ok=True)


def manifest(*,sync='off',retention='record',rid=None,sid=None,token=None):
    return {'project':'vision','run_path':'group/test','name':'test','run_id':rid or str(uuid.uuid4()),'session_id':sid or str(uuid.uuid4()),'session_ordinal':None,'producer_id':'rank0','primary_producer':'rank0','token':token or uuid.uuid4().hex,'config':{'lr':.1},'tags':[],'sync':sync,'retention':retention}


def event(m,seq,value=1.,step=None):
    return {'schema_version':'mlboard.event/v1','run_id':m['run_id'],'session_id':m['session_id'],'producer_id':m['producer_id'],'seq':str(seq),'event_time':'2026-10-23T08:00:00.000000Z','step':str(seq if step is None else step),'fields':[{'name':'train/loss','kind':'scalar','context':{},'value':value,'value_state':'finite'}]}


class TempCase(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='mlboard-test-',dir=TMP)
        self.root=Path(self.temp.name)
    def tearDown(self):
        endpoint=self.root/'collector.json'
        if endpoint.exists():
            with contextlib.suppress(OSError,ValueError):
                pid=json.loads(endpoint.read_text())['pid']
                os.kill(pid,signal.SIGTERM)
                with contextlib.suppress(ChildProcessError):
                    os.waitpid(pid,0)
        self.temp.cleanup()


class SDKTests(TempCase):
    def test_scalar_tags_keep_their_own_steps_and_module_wrapper(self):
        run=mlboard.init('vision','axes/per-metric',spool_dir=self.root)
        receipts=[run.log_scalar('train/loss',.8,step=100,epoch=.5),mlboard.log_scalar('val/loss',.9,step=1,epoch=1),run.log_scalar('train/loss',.6,step=200,epoch=1),mlboard.log_scalar('val/loss',.7,step=2,epoch=2)]
        self.assertTrue(all(receipt.accepted for receipt in receipts))
        self.assertTrue(run.finish(timeout=5).complete)
        query={'source':{'kind':'scalar','metrics':[{'name':'train/loss','context':{}},{'name':'val/loss','context':{}}]},'x':{'field':'step'},'transform':{'kind':'raw'}}
        response=LocalQuery(self.root).execute(project_id('vision'),query)
        frames={frame['meta']['metric']:frame['columns'] for frame in response['frames']}
        self.assertEqual(frames['train/loss']['x'],['100','200'])
        self.assertEqual(frames['val/loss']['x'],['1','2'])
        self.assertEqual([ref['seq'] for ref in frames['train/loss']['event_ref']],['0','2'])
        self.assertEqual([ref['seq'] for ref in frames['val/loss']['event_ref']],['1','3'])

    def test_training_log_never_uses_disk_network_or_tensor_conversion(self):
        class GPU:
            def item(self): raise AssertionError('item called')
            def cpu(self): raise AssertionError('cpu called')
            def numpy(self): raise AssertionError('numpy called')
            def __float__(self): raise AssertionError('float called')
        run=Run('vision','local/run',spool_dir=self.root,_start_sender=False)
        with patch('builtins.open',side_effect=AssertionError('disk access')),patch('socket.create_connection',side_effect=AssertionError('network access')):
            self.assertTrue(run.log({'loss':.4},step=0).accepted)
            self.assertFalse(run.log({'loss':GPU()},step=1).accepted)
            run.status()
        self.assertFalse((self.root/'spool.sqlite3').exists())

    def test_drop_newest_and_finite_finish(self):
        run=Run('vision','local/run',spool_dir=self.root,queue_events=2,queue_bytes=4096,_start_sender=False)
        self.assertTrue(run.log({'x':1}).accepted)
        self.assertTrue(run.log({'x':2}).accepted)
        self.assertFalse(run.log({'x':3}).accepted)
        start=time.monotonic()
        receipt=run.finish(timeout=.03)
        self.assertLess(time.monotonic()-start,.3)
        self.assertFalse(receipt.complete)
        self.assertEqual((receipt.accepted,receipt.dropped),(2,1))

    def test_offline_process_durable_nonfinite_and_module_api(self):
        run=mlboard.init('vision','offline/run',spool_dir=self.root,server='http://127.0.0.1:1')
        for step,value in enumerate((1.,math.nan,math.inf,-math.inf)):
            self.assertTrue(mlboard.log({'loss':value},step=step).accepted)
        receipt=mlboard.finish(timeout=5)
        self.assertTrue(receipt.complete,receipt)
        with readonly(self.root) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0],4)
            self.assertEqual(db.execute('SELECT SUM(pending) FROM events').fetchone()[0],0)
            self.assertEqual([r[0] for r in db.execute('SELECT value_state FROM scalars ORDER BY seq')],['finite','nan','pos_inf','neg_inf'])
        self.assertEqual(mlboard.status()['sync_errors'],[])

    def test_background_offline_preserves_local_and_remote_timeout(self):
        run=mlboard.init('vision','offline/background',spool_dir=self.root,sync='background',server='http://127.0.0.1:1')
        run.log({'loss':.5},step=0)
        receipt=run.finish(timeout=.6,wait_for='remote')
        self.assertFalse(receipt.complete)
        self.assertEqual(receipt.durable,1)
        with readonly(self.root) as db:
            self.assertEqual(db.execute('SELECT pending FROM events').fetchone()[0],1)

    def test_context_manager_keeps_original_training_exception(self):
        class TrainingError(Exception): pass
        with self.assertRaisesRegex(TrainingError,'training failed'):
            with mlboard.init('vision','exceptions/run',spool_dir=self.root,mode='strict') as run:
                run.log({'loss':1},step=0)
                raise TrainingError('training failed')
        run.finish(timeout=5)

    def test_real_collector_crash_and_resend(self):
        run=mlboard.init('vision','crash/run',spool_dir=self.root)
        for seq in range(30): run.log({'loss':seq},step=seq)
        end=time.monotonic()+5
        while run.status()['durable']<30 and time.monotonic()<end: time.sleep(.01)
        self.assertEqual(run.status()['durable'],30)
        pid=json.loads((self.root/'collector.json').read_text())['pid']
        os.kill(pid,signal.SIGKILL)
        with contextlib.suppress(ChildProcessError): os.waitpid(pid,0)
        for seq in range(30,60): run.log({'loss':seq},step=seq)
        receipt=run.finish(timeout=8)
        self.assertTrue(receipt.complete,receipt)
        with readonly(self.root) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0],60)
            self.assertEqual(db.execute('SELECT COUNT(DISTINCT seq) FROM events').fetchone()[0],60)

    def test_resume_allocates_new_session_without_reusing_sequences(self):
        first=mlboard.init('vision','resume/run',spool_dir=self.root)
        first.log({'loss':2},step=0)
        self.assertTrue(first.finish(timeout=5).complete)
        second=mlboard.init('vision','resume/run',spool_dir=self.root,run_id=first.id)
        second.log({'loss':1},step=0)
        self.assertTrue(second.finish(timeout=5).complete)
        with readonly(self.root) as db:
            self.assertEqual([r[0] for r in db.execute('SELECT ordinal FROM sessions ORDER BY ordinal')],[0,1])
            self.assertNotIn('elapsed_seconds',json.loads(db.execute('SELECT body FROM events ORDER BY revision DESC LIMIT 1').fetchone()[0]))

    def test_media_burst_retries_backpressure_before_finishing(self):
        source=self.root/'media.bin';source.write_bytes(b'x'*65536)
        runs=[mlboard.init('vision',f'media/burst-{i}',spool_dir=self.root,mode='strict') for i in range(2)]
        started=time.monotonic()
        for run in runs:
            for step in range(10):
                self.assertTrue(run.log_media(f'files/{step}',source,step=step).accepted)
                self.assertTrue(run.log({'loss':float(step)},step=step).accepted)
        self.assertLess(time.monotonic()-started,1.0,'log_media must not wait for file copies')
        for run in runs:
            receipt=run.finish(timeout=12)
            self.assertTrue(receipt.complete,receipt)
            self.assertEqual(receipt.dropped,0)
            self.assertEqual(run.status()['collector_rejected'],0)
        with readonly(self.root) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM media').fetchone()[0],20)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0],40)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM runs WHERE state='finished'").fetchone()[0],2)

    def test_media_is_snapshot_and_missing_file_is_explicit_failure(self):
        source=self.root/'input.txt';source.write_text('initial')
        run=mlboard.init('vision','media/run',spool_dir=self.root)
        run.log_media('files/test',source,step=0)
        self.assertTrue(run.finish(timeout=5).complete)
        source.write_text('modified')
        with readonly(self.root) as db:
            row=db.execute('SELECT * FROM media').fetchone()
            self.assertEqual((self.root/row['path']).read_text(),'initial')
        other=mlboard.init('vision','media/missing',spool_dir=self.root)
        other.log_media('files/nope',self.root/'missing',step=0)
        self.assertFalse(other.finish(timeout=5).complete)


class StoreFixture(TempCase):
    def setUp(self):
        super().setUp();self.store=RunStore(self.root);self.m=manifest()
        with self.store.db: self.store.register(self.m)
    def tearDown(self):
        self.store.close();super().tearDown()


class StoreTests(StoreFixture):
    def test_duplicate_and_hash_conflict(self):
        e=event(self.m,0)
        with self.store.db:
            rev,dup=self.store.append(e);rev2,dup2=self.store.append(e)
        self.assertEqual(rev,rev2);self.assertFalse(dup);self.assertTrue(dup2)
        with self.assertRaisesRegex(ValueError,'CONFLICT'):
            with self.store.db: self.store.append(event(self.m,0,2))
        self.assertEqual(self.store.status()['history_events'],1)

    def test_record_ack_retains_history_and_ack_idempotent(self):
        with self.store.db:
            self.store.append(event(self.m,0))
            self.store.mark_sync(server='http://127.0.0.1:8080')
        result={'run_id':self.m['run_id'],'server':'http://127.0.0.1:8080','project_id':'p','accepted':[(self.m['session_id'],'rank0','0')]}
        with self.store.db:
            self.store.apply_sync_result(result);self.store.apply_sync_result(result)
        self.assertEqual(self.store.status()['history_events'],1)
        self.assertEqual(self.store.status()['pending_events'],0)
        self.assertEqual(self.store.status()['acked_events'],1)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM scalars').fetchone()[0],1)

    def test_cache_ack_reclaims_only_acked_records(self):
        m=manifest(sync='background',retention='cache')
        with self.store.db:
            self.store.register(m);self.store.append(event(m,0));self.store.append(event(m,1))
            self.store.apply_sync_result({'run_id':m['run_id'],'server':'http://127.0.0.1:8080','project_id':'p','accepted':[(m['session_id'],'rank0','0')]})
        self.assertEqual(self.store.status(m['run_id'])['history_events'],1)
        self.assertEqual(self.store.status(m['run_id'])['pending_events'],1)
        self.assertEqual(self.store.db.execute('SELECT seq FROM events WHERE run_id=?',(m['run_id'],)).fetchone()[0],1)

    def test_quota_failure_preserves_existing_events(self):
        self.store.max_run_bytes=500
        with self.store.db: self.store.append(event(self.m,0))
        with self.assertRaises(OSError):
            with self.store.db: self.store.append(event(self.m,1))
        self.assertEqual(self.store.status()['history_events'],1)

    def test_append_work_does_not_scan_existing_history(self):
        # Count SQLite VM work for append before and after a substantial history.
        def work(seq):
            ticks=[0]
            self.store.db.set_progress_handler(lambda: ticks.__setitem__(0,ticks[0]+1) or 0,1)
            with self.store.db: self.store.append(event(self.m,seq))
            self.store.db.set_progress_handler(None,0)
            return ticks[0]
        early=work(0)
        for begin in range(1,5001,128):
            with self.store.db:
                for seq in range(begin,min(begin+128,5001)): self.store.append(event(self.m,seq))
        later=work(5001)
        self.assertLess(later,early*2+100,(early,later))
        stats=self.store.status()
        self.assertEqual(stats['history_events'],5002)
        actual=self.store.db.execute('SELECT SUM(length(CAST(body AS BLOB))) FROM events').fetchone()[0]
        self.assertEqual(stats['history_bytes'],actual)


class QueryTests(StoreFixture):
    def test_latest_logical_before_x_range_and_snapshot_pagination(self):
        with self.store.db:
            self.store.append(event(self.m,0,5,step=0));self.store.append(event(self.m,1,99,step=0))
            for seq in range(2,8): self.store.append(event(self.m,seq,float(seq)))
        query=LocalQuery(self.root)
        spec={'runs':{'run_ids':[self.m['run_id']]},'source':{'kind':'scalar','metrics':[{'name':'train/loss','context':{}}]},'x':{'field':'step'},'page':{'limit':2}}
        first=query.execute(project_id('vision'),spec)
        self.assertEqual(first['frames'][0]['columns']['value'],[99,2])
        self.assertEqual(first['diagnostics']['merged_points'],1)
        with self.store.db: self.store.append(event(self.m,8,8))
        spec['page']['cursor']=first['page']['next_cursor']
        second=query.execute(project_id('vision'),spec)
        self.assertEqual(second['consistency'],first['consistency'])
        self.assertEqual(second['frames'][0]['columns']['value'],[3,4])
        with self.store.db: self.store.db.execute('UPDATE runs SET deletion_generation=1')
        with self.assertRaises(QueryError) as exc: query.execute(project_id('vision'),spec)
        self.assertEqual(exc.exception.status,410)

    def test_minmax_preserves_spike_nan_and_missing_step(self):
        with self.store.db:
            for seq in range(100):
                e=event(self.m,seq,100 if seq==55 else 0)
                if seq==23: e['fields'][0].update(value=None,value_state='nan')
                self.store.append(e)
        response=LocalQuery(self.root).execute(project_id('vision'),{'source':{'metrics':[{'name':'train/loss','context':{}}]},'transform':{'kind':'downsample','algorithm':'min_max','points_per_series':20}})
        frame=response['frames'][0]
        self.assertIn(100,frame['columns']['value']);self.assertIn('nan',frame['columns']['value_state']);self.assertLessEqual(frame['row_count'],20)

    def test_unsupported_policies_are_errors(self):
        query=LocalQuery(self.root)
        for extra in ({'x':{'field':'invented'}},{'read':{'duplicate_steps':'mean'}},{'transform':{'kind':'average'}},{'source':{'kind':'histogram','metrics':[{'name':'x'}]}}):
            spec={'source':{'kind':'scalar','metrics':[{'name':'train/loss','context':{}}]},**extra}
            with self.assertRaises(QueryError): query.execute(project_id('vision'),spec)


class ProtocolTests(unittest.TestCase):
    def test_validation_bounds(self):
        for bad in ('a/../b','a//b','a\\b','a/ b','a\x00b','a\nb'):
            with self.assertRaises(ValidationError): run_path(bad)
        with self.assertRaises(ValidationError): json_value({'a'*500:1,'b'*500:2},budget=600)
        with self.assertRaises(ValidationError): mlboard.init('v','run',retention='cache')
        r=Run('v','run',_start_sender=False)
        self.assertFalse(r.log({'x'*257:1}).accepted)

    def test_crc_partial_tail_and_corruption(self):
        self.assertEqual(crc32c(b'123456789'),0xe3069283)
        with tempfile.TemporaryDirectory(dir=TMP) as directory:
            path=Path(directory)/'event'
            payload=b'example';length=struct.pack('<Q',len(payload));whole=length+struct.pack('<I',masked_crc(length))+payload+struct.pack('<I',masked_crc(payload))
            path.write_bytes(whole+whole[:14])
            self.assertEqual(len(list(records(path))),1)
            path.write_bytes(whole[:-1]+b'x')
            with self.assertRaisesRegex(ValueError,'CRC'): list(records(path))

    def test_redirect_does_not_leak_key(self):
        seen=[]
        class Target(BaseHTTPRequestHandler):
            def do_GET(self):
                seen.append(self.headers.get('Authorization'));self.send_response(200);self.end_headers();self.wfile.write(b'{}')
            def log_message(self,*_): pass
        target=ThreadingHTTPServer(('127.0.0.1',0),Target)
        class Redirect(Target):
            def do_GET(self):
                self.send_response(302);self.send_header('Location',f'http://127.0.0.1:{target.server_port}/steal');self.end_headers()
        source=ThreadingHTTPServer(('127.0.0.1',0),Redirect)
        for srv in (source,target): threading.Thread(target=srv.serve_forever,daemon=True).start()
        try:
            with self.assertRaises(SyncError): HTTP(f'http://127.0.0.1:{source.server_port}','private-test-key').request('GET','/projects')
            self.assertEqual(seen,[])
        finally:
            for srv in (source,target): srv.shutdown();srv.server_close()


class SyncTests(TempCase):
    def test_remote_committed_but_ack_lost_resends_same_body(self):
        store=RunStore(self.root);m=manifest()
        with store.db: store.register(m);store.append(event(m,0));store.finish(m['run_id'],'finished',0)
        seen,bodies={},[]
        class Remote(BaseHTTPRequestHandler):
            def log_message(self,*_): pass
            def reply(self,v):
                b=dumps(v).encode();self.send_response(200);self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
            def do_GET(self):
                if self.path.endswith('/projects'): self.reply({'projects':[{'id':'p','slug':'vision'}]})
                else: self.reply({'id':m['run_id'],'run_version':'1','state':'running'})
            def do_PATCH(self):
                self.rfile.read(int(self.headers['Content-Length']));self.reply({'state':'finished'})
            def do_POST(self):
                raw=self.rfile.read(int(self.headers['Content-Length']));body=json.loads(raw)
                if not self.path.endswith('/events'): self.reply({});return
                bodies.append(raw)
                accepted=[]
                for e in body['events']:
                    identity=(e['session_id'],e['producer_id'],e['seq']);seen.setdefault(identity,e)
                    accepted.append({'session_id':identity[0],'producer_id':identity[1],'seq':identity[2]})
                if len(bodies)==1:
                    self.connection.shutdown(socket.SHUT_RDWR);self.connection.close();return
                self.reply({'accepted':accepted,'duplicate_count':len(accepted)})
        server=ThreadingHTTPServer(('127.0.0.1',0),Remote)
        threading.Thread(target=server.serve_forever,daemon=True).start()
        remote=f'http://127.0.0.1:{server.server_port}'
        try:
            with store.db: store.mark_sync(server=remote)
            first=sync_pass(self.root,{'server':remote})
            with store.db:
                for r in first: store.apply_sync_result(r)
            self.assertEqual(store.status()['pending_events'],1)
            second=sync_pass(self.root,{'server':remote})
            with store.db:
                for r in second: store.apply_sync_result(r)
            self.assertEqual(len(seen),1);self.assertEqual(bodies[0],bodies[1])
            self.assertEqual(store.status()['pending_events'],0);self.assertEqual(store.status()['history_events'],1)
        finally:
            store.close();server.shutdown();server.server_close()


class ViewerTests(TempCase):
    def test_auth_host_origin_queries_and_readonly(self):
        store=RunStore(self.root);m=manifest()
        with store.db: store.register(m);store.append(event(m,0))
        store.close()
        server=ViewerServer(self.root,('127.0.0.1',0))
        threading.Thread(target=server.serve_forever,daemon=True).start()
        base=f'http://127.0.0.1:{server.server_port}'
        auth={'Authorization':'Bearer '+server.access_token}
        try:
            for path,mime in (('/assets/local-preferences.js','application/javascript'),('/assets/viewer.css','text/css')):
                with urllib.request.urlopen(base+path) as asset:
                    self.assertEqual(asset.headers.get_content_type(),mime)
                    self.assertEqual(asset.headers['X-Content-Type-Options'],'nosniff')
                    self.assertGreater(len(asset.read()),100)
                with self.assertRaises(urllib.error.HTTPError) as exc:
                    urllib.request.urlopen(urllib.request.Request(base+path,headers={'Host':'evil.example'}))
                self.assertEqual(exc.exception.code,403);exc.exception.close()
            with self.assertRaises(urllib.error.HTTPError) as exc:
                urllib.request.urlopen(urllib.request.Request(base+'/assets/..%2fsdk.py',headers=auth))
            self.assertEqual(exc.exception.code,404);exc.exception.close()
            with self.assertRaises(urllib.error.HTTPError) as exc: urllib.request.urlopen(base+'/api/v1/projects')
            self.assertEqual(exc.exception.code,401);exc.exception.close()
            for headers in ({**auth,'Host':'evil.example'},{**auth,'Origin':'https://evil.example'}):
                with self.assertRaises(urllib.error.HTTPError) as exc: urllib.request.urlopen(urllib.request.Request(base+'/api/v1/projects',headers=headers))
                self.assertEqual(exc.exception.code,403);exc.exception.close()
            response=json.load(urllib.request.urlopen(urllib.request.Request(base+'/api/v1/projects',headers=auth)))
            capabilities=json.load(urllib.request.urlopen(urllib.request.Request(base+'/api/v1/capabilities',headers=auth)))
            from mlboard.query_limits import QUERY_LIMITS
            self.assertEqual(capabilities['limits'],QUERY_LIMITS)
            pid=response['projects'][0]['id']
            spec={'source':{'kind':'scalar','metrics':[{'name':'train/loss','context':{}}]}}
            request=urllib.request.Request(base+'/api/v1/projects/'+pid+'/query',data=dumps(spec).encode(),headers=auth,method='POST')
            result=json.load(urllib.request.urlopen(request))
            self.assertEqual(result['frames'][0]['columns']['value'],[1])
            with readonly(self.root) as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0],1)
                self.assertEqual(db.execute('PRAGMA query_only').fetchone()[0],1)
        finally:
            server.shutdown();server.server_close()


@unittest.skipUnless(importlib.util.find_spec('tensorboard'),'optional tensorboard dependency absent')
class TensorBoardTests(TempCase):
    def test_real_proto_incremental_offset_tail_and_unsupported(self):
        from tensorboard.compat.proto.event_pb2 import Event
        from tensorboard.compat.proto.summary_pb2 import Summary
        from mlboard.tensorboard import import_directory
        source=self.root/'source';source.mkdir()
        dest=self.root/'store';file=source/'events.out.tfevents.1.test'
        def encode(step,tag):
            e=Event(wall_time=1700000000+step,step=step,summary=Summary(value=[Summary.Value(tag=tag,simple_value=1/(step+1))]))
            payload=e.SerializeToString();length=struct.pack('<Q',len(payload))
            return length+struct.pack('<I',masked_crc(length))+payload+struct.pack('<I',masked_crc(payload))
        first,second,third,fourth=encode(100,'train/loss'),encode(1,'val/loss'),encode(200,'train/loss'),encode(2,'val/loss')
        file.write_bytes(first+second[:15])
        self.assertEqual(import_directory(source,dest)['imported_events'],1)
        self.assertEqual(import_directory(source,dest)['imported_events'],0)
        with file.open('ab') as out: out.write(second[15:]+third+fourth)
        self.assertEqual(import_directory(source,dest)['imported_events'],3)
        with readonly(dest) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0],4)
            self.assertEqual(db.execute('SELECT offset FROM import_offsets').fetchone()[0],sum(map(len,(first,second,third,fourth))))
            self.assertEqual([row[0] for row in db.execute("SELECT step FROM scalars WHERE name='train/loss' ORDER BY seq")],[100,200])
            self.assertEqual([row[0] for row in db.execute("SELECT step FROM scalars WHERE name='val/loss' ORDER BY seq")],[1,2])
        # Stop just this test's nested collector before deleting its store.
        pid=json.loads((dest/'collector.json').read_text())['pid'];os.kill(pid,signal.SIGTERM)
        with contextlib.suppress(ChildProcessError): os.waitpid(pid,0)


if __name__=='__main__': unittest.main()
