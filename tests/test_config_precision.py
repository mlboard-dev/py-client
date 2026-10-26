import json
import unittest
from decimal import Decimal
from mlboard.config_json import JSONNumber, loads_config_json, dumps_config_json, config_value
from mlboard.validation import json_value, scalar
from mlboard.ipc import frame, extract

RAW='{"ints":[9007199254740991,9007199254740992,9007199254740993,-9007199254740993,9223372036854775807,-9223372036854775808],"values":[1.00,0.123456789012345678901234567890,1e400,"9007199254740993","1e400"]}'
class ConfigPrecisionTests(unittest.TestCase):
    def test_exact_tokens_and_builtin_ints_roundtrip(self):
        value=loads_config_json(RAW)
        self.assertEqual(dumps_config_json(value),RAW)
        self.assertEqual(value['ints'][2],9007199254740993)
        self.assertIs(type(value['ints'][2]),int)
        self.assertIs(type(value['values'][1]),JSONNumber)
        self.assertIs(type(value['values'][3]),str)
        self.assertRaises(TypeError,json.dumps,value)
        self.assertRaises(TypeError,float,value['values'][1])
    def test_config_and_ipc_boundaries_do_not_change_metrics(self):
        value={'config':loads_config_json(RAW),'metric':0.25}
        result=extract(bytearray(frame(value)))[0]
        self.assertEqual(dumps_config_json(result['config']),RAW)
        self.assertIs(type(result['metric']),float)
        self.assertEqual(result['metric'],0.25)
        config=config_value({'n':9223372036854775807,'decimal':Decimal('0.123456789012345678901234567890')})
        self.assertIn('9223372036854775807',dumps_config_json(config))
        self.assertIn('0.123456789012345678901234567890',dumps_config_json(config))
        self.assertRaises(ValueError,json_value,{'n':9007199254740993})
        self.assertRaises(ValueError,scalar,9007199254740993)
    def test_bounds_and_invalid_numbers(self):
        for raw in ('NaN','Infinity','01','1;null',''):
            self.assertRaises(ValueError,JSONNumber,raw)
        self.assertRaises(ValueError,config_value,{'p':'x'*65536})
        value=0
        for _ in range(21):value={'nested':value}
        self.assertRaises(ValueError,config_value,value)
    def test_long_integer_tokens_do_not_depend_on_python_int_limits(self):
        token='9'*5000
        raw='{"long":'+token+',"string":"'+token+'"}'
        for parsed in (loads_config_json(raw),loads_config_json('{"config":'+raw+'}',fields=True)['config']):
            self.assertIs(type(parsed['long']),JSONNumber)
            self.assertIs(type(parsed['string']),str)
            self.assertEqual(dumps_config_json(config_value(parsed)),raw)
            restored=extract(bytearray(frame({'config':parsed})))[0]['config']
            self.assertEqual(dumps_config_json(restored),raw)
        self.assertRaises(ValueError,extract,bytearray(frame({'metric':JSONNumber(token)})))

import contextlib
import os
from pathlib import Path
import signal
import tempfile
import threading
import urllib.request
import uuid
from mlboard.sdk import Run
from mlboard.store import readonly, RunStore
from mlboard.transport import HTTP, sync_pass
from mlboard.query import run_dict
from mlboard.viewer import ViewerServer

@unittest.skipUnless(os.environ.get('MLBOARD_CONFIG_TEST_FIXTURE'),'isolated config HTTP fixture required')
class ConfigHTTPContractTests(unittest.TestCase):
    def test_sdk_collector_local_http_sync_get_patch_replay(self):
        fixture=loads_config_json(Path(os.environ['MLBOARD_CONFIG_TEST_FIXTURE']).read_text(),fields=True)
        base=Path(os.environ['MLBOARD_ROOT'])/'outputs/correctness-lan-20261026/config'
        with tempfile.TemporaryDirectory(prefix='python-sdk-',dir=base) as directory:
            root=Path(directory);run=None;viewer=None
            try:
                config=loads_config_json(RAW)
                config['long_integer']=JSONNumber('9'*5000)
                run=Run(fixture['project_id'],'sdk/'+uuid.uuid4().hex,config=config,spool_dir=root,sync='off')
                self.assertTrue(run.log({'loss':0.25},step=1).accepted)
                self.assertTrue(run.finish(timeout=5).complete)
                with readonly(root) as db:
                    row=db.execute('SELECT * FROM runs WHERE id=?',(run.id,)).fetchone()
                    local=run_dict(db,row)
                    self.assertEqual(dumps_config_json(local['config']),dumps_config_json(config))
                viewer=ViewerServer(root,('127.0.0.1',0));threading.Thread(target=viewer.serve_forever,daemon=True).start()
                url=f'http://127.0.0.1:{viewer.server_port}/api/v1/runs/{run.id}'
                req=urllib.request.Request(url,headers={'Authorization':'Bearer '+viewer.access_token})
                with urllib.request.urlopen(req,timeout=3) as response:
                    local_http=loads_config_json(response.read(),fields=True)
                self.assertEqual(dumps_config_json(local_http['config']),dumps_config_json(config))
                store=RunStore(root)
                with store.db:store.mark_sync(server=fixture["server"])
                store.close()
                for _ in range(2):
                    results=sync_pass(root,{'server':fixture['server'],'api_key':fixture['key']})
                    self.assertTrue(results)
                    self.assertTrue(all(not item.get('error') for item in results))
                http=HTTP(fixture['server'],fixture['key'])
                remote=http.request('GET','/runs/'+run.id+'?include_summary=false')
                self.assertEqual(dumps_config_json(remote['config']),dumps_config_json(config))
                saved=http.request('PATCH','/runs/'+run.id,{'config':remote['config']},headers={'If-Match':remote['run_version']})
                self.assertEqual(dumps_config_json(saved['config']),dumps_config_json(config))
            finally:
                if viewer:viewer.shutdown();viewer.server_close()
                endpoint=root/'collector.json'
                if endpoint.exists():
                    with contextlib.suppress(OSError,ValueError):os.kill(json.loads(endpoint.read_text())['pid'],signal.SIGTERM)
