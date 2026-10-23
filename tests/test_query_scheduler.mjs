import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';

const html=fs.readFileSync(new URL('../mlboard/static/index.html',import.meta.url),'utf8');
const queueSource=html.slice(html.indexOf('class LatestQueryQueue'),html.indexOf('async function loadChartResponse'));
const responseSource=html.slice(html.indexOf('async function loadChartResponse'),html.indexOf('function metricKey'));
const LatestQueryQueue=vm.runInNewContext('('+queueSource.trim()+')',{setTimeout,clearTimeout});
function deferred(){let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve}}
async function until(predicate){
  const deadline=Date.now()+1000;
  while(!predicate()){
    assert.ok(Date.now()<deadline,'scheduler did not progress');
    await new Promise(resolve=>setTimeout(resolve,2));
  }
}

test('duplicate toggles share one request and only the latest pending target runs',async()=>{
  const gate=deferred(),seen=[];let concurrent=0,maximum=0;
  const queue=new LatestQueryQueue(async job=>{
    concurrent++;maximum=Math.max(maximum,concurrent);seen.push(job.key);
    if(job.key==='initial')await gate.promise;
    concurrent--;
  });
  const initial=queue.submit({key:'initial'});
  for(let i=0;i<100;i++)assert.equal(queue.submit({key:'initial'}),initial);
  await until(()=>queue.active!==null);
  const superseded=[];
  for(let i=0;i<100;i++)superseded.push(queue.submit({key:'intermediate-'+i}));
  const latest=queue.submit({key:'latest'});
  assert.equal(queue.active.key,'initial');
  assert.equal(queue.pending.key,'latest');
  assert.ok((await Promise.all(superseded)).every(value=>value===false));
  gate.resolve();
  assert.equal(await initial,false);
  assert.equal(await latest,true);
  assert.deepEqual(seen,['initial','latest']);
  assert.equal(maximum,1);
});

test('a generation change drops queued work and drains the active request before the next',async()=>{
  const gate=deferred(),seen=[];
  const queue=new LatestQueryQueue(async(job,current)=>{
    seen.push(job.key);
    if(job.key==='old-active'){
      await gate.promise;
      assert.equal(current(),false);
    }
  });
  const active=queue.submit({key:'old-active'});
  await until(()=>queue.active!==null);
  const oldPending=queue.submit({key:'old-pending'});
  queue.cancel();
  assert.equal(await oldPending,false);
  const current=queue.submit({key:'new-generation'});
  assert.equal(queue.active.key,'old-active');
  gate.resolve();
  await active;await current;
  assert.deepEqual(seen,['old-active','new-generation']);
});

test('returning to the active target cancels the obsolete pending target',async()=>{
  const gate=deferred(),seen=[];
  const queue=new LatestQueryQueue(async job=>{seen.push(job.key);await gate.promise});
  const active=queue.submit({key:'step'});
  await until(()=>queue.active!==null);
  const pending=queue.submit({key:'epoch'});
  assert.equal(queue.submit({key:'step'}),active);
  assert.equal(await pending,false);
  gate.resolve();assert.equal(await active,true);
  assert.deepEqual(seen,['step']);
});

test('refresh promotes a queued request and an error does not stop future work',async()=>{
  const seen=[],errors=[];
  const queue=new LatestQueryQueue(async job=>{
    seen.push([job.key,Boolean(job.force)]);
    if(job.key==='failed')throw Error('query failed');
  },e=>errors.push(e.message));
  const request=queue.submit({key:'failed',force:false});
  assert.equal(queue.submit({key:'failed',force:true}),request);
  await request;
  await queue.submit({key:'next'});
  assert.deepEqual(seen,[['failed',true],['next',false]]);
  assert.deepEqual(errors,['query failed']);
});

function responseLoader(api,clock=Date){
  return vm.runInNewContext('('+responseSource.trim()+')',{api,Date:clock,AbortController,DOMException,setTimeout,clearTimeout});
}

test('busy responses retry serially and eventually complete',async()=>{
  let calls=0,active=0,maximum=0;
  const load=responseLoader(async()=>{
    calls++;active++;maximum=Math.max(maximum,active);
    await Promise.resolve();active--;
    if(calls<3)throw Object.assign(Error('busy'),{status:429,code:'QUERY_BUSY'});
    return {complete:true};
  });
  const result=await load({project:'local',spec:{}},()=>true);
  assert.equal(result.complete,true);assert.equal(calls,3);assert.equal(maximum,1);
});

test('an obsolete target does not retry a busy request',async()=>{
  let current=true,calls=0;
  const load=responseLoader(async()=>{
    calls++;current=false;
    throw Object.assign(Error('busy'),{status:429,code:'QUERY_BUSY'});
  });
  await assert.rejects(load({project:'local',spec:{}},()=>current),e=>e.name==='AbortError');
  assert.equal(calls,1);
});

test('a stalled HTTP request ends with an actionable timeout',async()=>{
  let ticks=0;
  const clock={now:()=>ticks++===0?0:4999};
  const load=responseLoader((_path,options)=>new Promise((_resolve,reject)=>{
    options.signal.addEventListener('abort',()=>reject(new DOMException('aborted','AbortError')));
  }),clock);
  await assert.rejects(load({project:'local',spec:{}},()=>true),/Query timed out/);
});
