import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import fs from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';

const require=createRequire(import.meta.url),local=require('../mlboard/static/local-preferences.js');
const html=fs.readFileSync(new URL('../mlboard/static/index.html',import.meta.url),'utf8');
const cohortSource=html.slice(html.indexOf('async function loadChartCohort'),html.indexOf('function chartCard'));
const visibleSource=html.slice(html.indexOf('function drawVisible'),html.indexOf('function svgEl'));
const specSource=html.slice(html.indexOf('function querySpec'),html.indexOf('async function workspace'));
function result(points,scanned=points,series=1){
  return {complete:true,frames:Array.from({length:series},()=>({columns:{value:Array(points/series).fill(1)}})),page:{returned_rows:points,next_cursor:null},diagnostics:{scanned_points:scanned,truncated_by:[]}};
}
function job(){return {project:'local',axisGroups:['step','epoch','wall_time'].map(axis=>({axis,metrics:[{name:axis}],spec:{x:{field:axis},page:{limit:20000},budget:{max_runs:10,max_series:20,max_points:20000,max_scanned_points:250000,max_bytes:2097152}}}))}}
function loader(loadChartResponse,{clock=Date,responseBytes=new WeakMap()}={}){
  return vm.runInNewContext('('+cohortSource.trim()+')',{
    loadChartResponse,Date:clock,responseBytes,TextEncoder,DOMException,
    codedError:code=>Object.assign(Error(code),{code})
  });
}

test('different X-axis groups run serially and spend one shared point, scan, series and byte budget',async()=>{
  const responses=[result(3000,80000,3),result(2000,70000,2),result(1000,50000,1)],bytes=new WeakMap();
  responses.forEach((response,index)=>bytes.set(response,[600000,500000,400000][index]));
  const requests=[];let active=0,maximum=0;
  const load=loader(async request=>{
    requests.push(request);active++;maximum=Math.max(maximum,active);
    await new Promise(resolve=>setTimeout(resolve,3));active--;
    return responses[requests.length-1];
  },{responseBytes:bytes,clock:{now:()=>100}});
  const loaded=await load(job(),()=>true);
  assert.equal(maximum,1);assert.equal(loaded.points,6000);
  assert.deepEqual(requests.map(request=>request.spec.x.field),['step','epoch','wall_time']);
  assert.deepEqual(requests.map(request=>request.spec.budget.max_series),[20,17,15]);
  assert.deepEqual(requests.map(request=>request.spec.budget.max_points),[20000,17000,15000]);
  assert.deepEqual(requests.map(request=>request.spec.page.limit),[20000,17000,15000]);
  assert.deepEqual(requests.map(request=>request.spec.budget.max_scanned_points),[250000,170000,100000]);
  assert.deepEqual(requests.map(request=>request.spec.budget.max_bytes),[2097152,1497152,997152]);
  assert.ok(requests.every(request=>request.deadline===5100));
});

test('a superseded cohort stops before its remaining axis groups',async()=>{
  let current=true,calls=0;
  const load=loader(async()=>{calls++;current=false;return result(1)});
  await assert.rejects(load(job(),()=>current),error=>error.name==='AbortError');
  assert.equal(calls,1);
});

test('an aggregate budget overrun or unexpected paged result cannot be applied as complete',async()=>{
  for(const dimension of ['points','scanned','bytes','pagination']){
    const responses=dimension==='points'?[result(12000),result(9000)]:dimension==='scanned'?[result(1,200000),result(1,60000)]:[result(1),result(1)];
    const bytes=new WeakMap();responses.forEach((response,index)=>bytes.set(response,dimension==='bytes'?[1500000,800000][index]:100));
    if(dimension==='pagination')responses[1].page.next_cursor='more-data';
    let calls=0;
    const load=loader(async()=>responses[calls++],{responseBytes:bytes});
    await assert.rejects(load(job(),()=>true),error=>error.code==='CLIENT_BUDGET_EXCEEDED',dimension);
    assert.equal(calls,2,dimension);
  }
});

test('the five-second deadline is shared across axis groups and checked after a response',async()=>{
  let time=0;const seen=[];
  const load=loader(async request=>{
    seen.push(request);time=seen.length===1?4900:5001;return result(1);
  },{clock:{now:()=>time}});
  await assert.rejects(load(job(),()=>true),error=>error.code==='CLIENT_QUERY_TIMEOUT');
  assert.equal(seen.length,2);assert.deepEqual(seen.map(request=>request.deadline),[5000,5000]);
  assert.deepEqual(seen.map(request=>request.spec.budget.timeout_ms),[2000,100]);
});

test('visible cards use their own project, name and context preference within the total series cap',()=>{
  const metrics=[{name:'train/loss',context:{split:'train'}},{name:'train/loss',context:{split:'val'}},{name:'system/memory',context:{}}];
  const values=new Map(),storage={getItem:key=>values.get(key)??null,setItem:(key,value)=>values.set(key,value)};
  local.writeAxis(storage,'project-a',metrics[1],'epoch');local.writeAxis(storage,'project-b',metrics[0],'wall_time');
  let submitted;
  const context={generation:4,view:'workspace',project:'project-a',groups:new Map([['metrics',metrics]]),selected:new Set(Array.from({length:10},(_,index)=>String(index))),local,preferenceStorage:storage,
    document:{querySelectorAll:()=>[{dataset:{group:'metrics'}}]},prepareChartCards(){},prunePlots(){},chartQueue:{submit(job){submitted=job;return Promise.resolve(true)},cancel(){}}
  };
  vm.createContext(context);vm.runInContext(specSource+'\n'+visibleSource+'\ndrawVisible(4);',context);
  assert.deepEqual(Array.from(submitted.axisGroups,group=>group.axis),['step','epoch']);
  assert.deepEqual(Array.from(submitted.chosen,metric=>metric.context.split),['train','val']);
  assert.ok(submitted.axisGroups.every(group=>group.spec.runs.run_ids.length===10));
  assert.equal(submitted.chosen.length*10,20);
  assert.equal(submitted.axisGroups[1].spec.source.metrics[0].context.split,'val');
});
