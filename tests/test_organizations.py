from __future__ import annotations

import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import signal
import threading
import time
import urllib.parse
from unittest.mock import patch

import mlboard
from mlboard.cli import sync
from mlboard.ipc import command
from mlboard.store import RunStore, readonly
from mlboard.transport import sync_pass
from mlboard.validation import dumps
from test_client import TempCase, event, manifest


class OrganizationRemote:
    """An HTTP peer requiring an explicit organization when creation is ambiguous."""
    def __init__(self, *, organizations=None, projects=None, discover=True):
        self.organizations=organizations if organizations is not None else [{'id':'org-a','slug':'alpha','name':'Same name','can_manage':False},{'id':'org-b','slug':'beta','name':'Same name','can_manage':False}]
        self.projects=projects or []
        self.requests=[];self.created=[];self.events={};self.states={}
        peer=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_): pass
            def reply(self,value,status=200):
                raw=dumps(value).encode();self.send_response(status);self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
            def do_GET(self):
                peer.requests.append(('GET',self.path,None))
                if self.path=='/api/v1/projects': return self.reply({'projects':peer.projects})
                if self.path=='/api/v1/organizations':
                    return self.reply({'organizations':peer.organizations}) if discover else self.reply({'error':{'code':'FORBIDDEN'}},403)
                rid=urllib.parse.urlsplit(self.path).path.rsplit('/',1)[-1];self.reply({'id':rid,'run_version':'1','state':peer.states.get(rid,'running')})
            def do_POST(self):
                body=json.loads(self.rfile.read(int(self.headers['Content-Length'])));peer.requests.append(('POST',self.path,body))
                if self.path=='/api/v1/projects':
                    oid=body.get('organization_id')
                    if oid is None:
                        if len(peer.organizations)!=1: return self.reply({'error':{'code':'ORGANIZATION_REQUIRED'}},422)
                        oid=peer.organizations[0]['id']
                    if oid not in [org['id'] for org in peer.organizations]: return self.reply({'error':{'code':'FORBIDDEN'}},403)
                    project={'id':'created-project','slug':body['slug'],'name':body['name'],'organization_id':oid}
                    peer.created.append(body);peer.projects.append(project);return self.reply(project)
                if self.path.endswith('/events'):
                    accepted=[]
                    for item in body['events']:
                        identity=(item['run_id'],item['session_id'],item['producer_id'],item['seq']);old=peer.events.setdefault(identity,item)
                        if old!=item: return self.reply({'error':{'code':'EVENT_IDENTITY_CONFLICT'}},409)
                        accepted.append({key:item[key] for key in ('session_id','producer_id','seq')})
                    return self.reply({'accepted':accepted})
                self.reply({})
            def do_PATCH(self):
                body=json.loads(self.rfile.read(int(self.headers['Content-Length'])));peer.requests.append(('PATCH',self.path,body))
                rid=self.path.rsplit('/',1)[-1];peer.states[rid]=body['state'];self.reply({'state':body['state']})
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler);self.url=f'http://127.0.0.1:{self.server.server_port}'
        threading.Thread(target=self.server.serve_forever,daemon=True).start()
    def __enter__(self): return self
    def __exit__(self,*_): self.server.shutdown();self.server.server_close()


class OrganizationTests(TempCase):
    def test_completed_background_registration_rechecks_an_explicit_organization(self):
        projects=[{'id':'target','slug':'vision','organization_id':'org-b'},{'id':'other','slug':'other','organization_id':'org-a'}]
        with OrganizationRemote(projects=projects,discover=False) as remote:
            run=mlboard.init('target','org/reconnect',spool_dir=self.root,sync='background',server=remote.url,organization='org-b')
            self.assertTrue(run.finish(timeout=4,wait_for='remote').complete)
            posts=len([request for request in remote.requests if request[0]=='POST'])
            # Same token/session/producer: register is otherwise an idempotent retry.
            reply=command(self.root,{**run._register,'organization':'org-a'})
            self.assertFalse(reply['status']['remote_complete'])
            deadline=time.monotonic()+4
            while time.monotonic()<deadline:
                status=command(self.root,{'kind':'status','run_id':run.id})['status']
                if any('REMOTE_ORGANIZATION_MISMATCH' in error for error in status['sync_errors']): break
                time.sleep(.03)
            self.assertTrue(any('REMOTE_ORGANIZATION_MISMATCH' in error for error in status['sync_errors']))
            self.assertFalse(status['remote_complete'])
            self.assertEqual(len([request for request in remote.requests if request[0]=='POST']),posts)

    def test_an_old_sync_result_cannot_complete_a_new_organization_check(self):
        projects=[{'id':'target','slug':'vision','organization_id':'org-b'}]
        with OrganizationRemote(projects=projects) as remote:
            store=RunStore(self.root);m=manifest(sync='background')
            try:
                with store.db: store.register(m);store.mark_sync(server=remote.url);store.finish(m['run_id'],'finished',0)
                old=sync_pass(self.root,{'server':remote.url})[0]
                self.assertNotIn('error',old)
                with store.db:
                    store.mark_sync(server=remote.url,force_remote_check=True)
                    store.apply_sync_result(old)
                self.assertFalse(store.status()['remote_complete'])
                with store.db:
                    for result in sync_pass(self.root,{'server':remote.url,'organization':'org-b'}): store.apply_sync_result(result)
                self.assertTrue(store.status()['remote_complete'])
            finally: store.close()

    def test_offline_organization_survives_collector_restart_and_manual_sync(self):
        with patch.dict(os.environ,{'MLBOARD_ORGANIZATION':'beta'}):
            run=mlboard.init('vision','org/offline',spool_dir=self.root,create_project=True)
            self.assertTrue(run.log_scalar('train/loss',.5,step=100).accepted)
            self.assertTrue(run.finish(timeout=5).complete)
        with readonly(self.root) as db:
            self.assertEqual(db.execute('SELECT organization FROM runs').fetchone()[0],'beta')
            original=db.execute('SELECT body FROM events').fetchone()[0]
        pid=json.loads((self.root/'collector.json').read_text())['pid'];os.kill(pid,signal.SIGTERM)
        with contextlib.suppress(ChildProcessError): os.waitpid(pid,0)
        with OrganizationRemote() as remote:
            self.assertTrue(sync(self.root,server=remote.url,api_key='test-global-key',create_project=True,timeout=4)['remote_complete'])
            self.assertEqual(remote.created,[{'slug':'vision','name':'vision','organization_id':'org-b'}])
            with readonly(self.root) as db:
                row=db.execute('SELECT organization,remote_organization,remote_project FROM runs').fetchone()
                self.assertEqual(tuple(row),('beta','org-b','created-project'))
                self.assertEqual(db.execute('SELECT body FROM events').fetchone()[0],original)
            posts=len([request for request in remote.requests if request[0]=='POST'])
            with self.assertRaisesRegex(RuntimeError,'REMOTE_ORGANIZATION_MISMATCH'):
                sync(self.root,server=remote.url,organization='alpha',timeout=1)
            self.assertEqual(len([request for request in remote.requests if request[0]=='POST']),posts)
            # A stable binding survives both organization and project slug changes.
            remote.organizations[1]['slug']='renamed-org';remote.projects[0]['slug']='renamed-project'
            before=len(remote.requests)
            self.assertTrue(sync(self.root,server=remote.url,organization='org-b',timeout=4)['remote_complete'])
            self.assertFalse(any(path.endswith('/organizations') for _,path,_ in remote.requests[before:]))
            self.assertEqual(len(remote.created),1);self.assertEqual(len(remote.events),1)

    def test_project_id_wins_over_a_matching_slug_without_org_discovery(self):
        projects=[{'id':'other','slug':'target-id','name':'Same name','organization_id':'org-a'},{'id':'target-id','slug':'vision','name':'Same name','organization_id':'org-b'}]
        with OrganizationRemote(projects=projects,discover=False) as remote:
            run=mlboard.init('target-id','org/project-key',spool_dir=self.root,sync='background',server=remote.url,organization='org-b',api_key='test-project-key')
            self.assertTrue(run.finish(timeout=4,wait_for='remote').complete)
            with readonly(self.root) as db:
                self.assertEqual(tuple(db.execute('SELECT remote_project,remote_organization FROM runs').fetchone()),('target-id','org-b'))
            paths=[path for _,path,_ in remote.requests]
            self.assertIn('/api/v1/projects/target-id/runs',paths)
            self.assertNotIn('/api/v1/projects/other/runs',paths)
            self.assertNotIn('/api/v1/organizations',paths)

    def test_omitted_organization_uses_only_server_single_org_compatibility(self):
        for count in (1,2):
            root=self.root/str(count);store=RunStore(root);m=manifest(sync='background')
            try:
                with OrganizationRemote(organizations=[{'id':f'org-{index}','slug':f'org-{index}'} for index in range(count)]) as remote:
                    with store.db: store.register(m);store.mark_sync(server=remote.url);store.finish(m['run_id'],'finished',0)
                    results=sync_pass(root,{'server':remote.url,'create_project':True})
                    with store.db:
                        for result in results: store.apply_sync_result(result)
                    self.assertEqual(store.status()['remote_complete'],count==1)
                    project_requests=[body for method,path,body in remote.requests if method=='POST' and path=='/api/v1/projects']
                    self.assertEqual(project_requests,[{'slug':'vision','name':'vision'}])
                    self.assertFalse(any(path.endswith('/organizations') for _,path,_ in remote.requests))
                    if count==2: self.assertIn('ORGANIZATION_REQUIRED',results[0]['error'])
            finally: store.close()

    def test_ambiguous_org_slug_is_rejected_before_project_creation(self):
        organizations=[{'id':'org-a','slug':'duplicate'},{'id':'org-b','slug':'duplicate'}]
        with OrganizationRemote(organizations=organizations) as remote:
            store=RunStore(self.root);m={**manifest(sync='background'),'organization':'duplicate'}
            try:
                with store.db: store.register(m);store.mark_sync(server=remote.url)
                results=sync_pass(self.root,{'server':remote.url,'create_project':True})
                self.assertIn('ORGANIZATION_NOT_FOUND_OR_AMBIGUOUS',results[0]['error']);self.assertEqual(remote.created,[])
            finally: store.close()

    def test_existing_store_adds_optional_org_metadata_without_changing_history(self):
        store=RunStore(self.root);m=manifest()
        with store.db:
            store.register(m);store.append(event(m,0));body=store.db.execute('SELECT body FROM events').fetchone()[0]
            store.db.execute('ALTER TABLE runs DROP COLUMN organization');store.db.execute('ALTER TABLE runs DROP COLUMN remote_organization')
        store.close();store=RunStore(self.root)
        try:
            self.assertEqual(store.db.execute('SELECT body FROM events').fetchone()[0],body)
            self.assertEqual(tuple(store.db.execute('SELECT organization,remote_organization,history_events FROM runs').fetchone()),(None,None,1))
        finally: store.close()
