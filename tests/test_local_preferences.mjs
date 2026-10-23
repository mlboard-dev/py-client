import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import test from 'node:test';
const require=createRequire(import.meta.url);
const local=require('../mlboard/static/local-preferences.js');
function memoryStorage(initial={}){
  const values=new Map(Object.entries(initial));
  return {values,getItem:key=>values.get(key)??null,setItem:(key,value)=>values.set(key,value)};
}
function mediaQuery(matches){
  const listeners=new Set();
  return {matches,addEventListener:(_event,listener)=>listeners.add(listener),removeEventListener:(_event,listener)=>listeners.delete(listener),set(value){this.matches=value;for(const listener of listeners)listener({matches:value})},get listenerCount(){return listeners.size}};
}
test('English and Chinese catalogues cover the same UI keys and placeholders',()=>{
  assert.deepEqual(Object.keys(local.messages.en).sort(),Object.keys(local.messages['zh-CN']).sort());
  const tokens=value=>[...String(typeof value==='object'?value.other:value).matchAll(/\{(\w+)\}/g)].map(match=>match[1]).sort();
  for(const key of Object.keys(local.messages.en))assert.deepEqual(tokens(local.messages.en[key]),tokens(local.messages['zh-CN'][key]),key);
  for(const key of Object.values(local.errorKeys))assert.ok(local.messages.en[key],key);
});
test('plural and number formatting preserve user strings and protocol identifiers',()=>{
  assert.equal(local.translate('en','chart.metricCount',{count:1}),'1 metric');
  assert.equal(local.translate('en','chart.metricCount',{count:2}),'2 metrics');
  assert.equal(local.translate('zh-CN','chart.metricCount',{count:2}),'2 个指标');
  const value='train/loss <svg> · wall_time';
  assert.equal(local.translate('zh-CN','record.step',{step:value}),'步数 '+value);
  assert.equal(local.number('en',9223372036854775807n),'9,223,372,036,854,775,807');
});
test('system is the default theme and follows system changes only when selected',()=>{
  const storage=memoryStorage(),media=mediaQuery(false),seen=[];
  const prefs=local.createPreferences({storage,language:'zh-TW',media});
  prefs.subscribe(state=>seen.push(state));
  assert.deepEqual(prefs.snapshot(),{language:'zh-CN',theme:'system',resolvedTheme:'light'});
  media.set(true);assert.equal(prefs.snapshot().resolvedTheme,'dark');
  prefs.setTheme('light');media.set(false);media.set(true);
  assert.equal(prefs.snapshot().resolvedTheme,'light');
  assert.equal(seen.length,2);
  prefs.setTheme('system');assert.equal(prefs.snapshot().resolvedTheme,'dark');
  prefs.setLanguage('en');assert.equal(storage.values.get('mlboard-local-language'),'en');
  assert.equal(storage.values.get('mlboard-local-theme'),'system');
  prefs.dispose();assert.equal(media.listenerCount,0);
});
test('corrupt or unavailable browser storage falls back without blocking the viewer',()=>{
  const bad=memoryStorage({'mlboard-local-language':'invalid','mlboard-local-theme':'invalid'});
  const prefs=local.createPreferences({storage:bad,language:'zh-CN',media:mediaQuery(true)});
  assert.equal(prefs.snapshot().language,'zh-CN');assert.equal(prefs.snapshot().theme,'system');
  assert.throws(()=>prefs.setTheme('auto'),RangeError);assert.throws(()=>prefs.setLanguage('fr'),RangeError);
  const blocked={getItem(){throw Error('storage blocked')},setItem(){throw Error('storage blocked')}};
  const session=local.createPreferences({storage:blocked,language:'en',media:mediaQuery(false)});
  session.setTheme('dark');session.setLanguage('zh-CN');
  assert.deepEqual(session.snapshot(),{language:'zh-CN',theme:'dark',resolvedTheme:'dark'});
});
test('axis preferences distinguish project, metric and complete context',()=>{
  const storage=memoryStorage(),metric={name:'train/loss',context:{fold:1,split:'val'}};
  assert.equal(local.readAxis(storage,'project-a',metric),'step');
  local.writeAxis(storage,'project-a',metric,'epoch');
  assert.equal(local.readAxis(storage,'project-a',{name:metric.name,context:{split:'val',fold:1}}),'epoch');
  assert.equal(local.readAxis(storage,'project-b',metric),'step');
  assert.equal(local.readAxis(storage,'project-a',{name:'val/loss',context:metric.context}),'step');
  assert.equal(local.readAxis(storage,'project-a',{name:metric.name,context:{fold:2,split:'val'}}),'step');
  assert.throws(()=>local.writeAxis(storage,'project-a',metric,'metric_join'),RangeError);
  storage.setItem(local.axisPreferenceKey('project-a',metric),'metric_join');
  assert.equal(local.readAxis(storage,'project-a',metric),'step');
});
test('error presentation is localized while raw protocol errors remain unchanged',()=>{
  const source={code:'UNSUPPORTED_OPERATION',message:'unsupported read fields: at_revision'};
  assert.equal(local.errorText('zh-CN',source),'本地查看器不支持请求的查询操作。');
  assert.equal(source.message,'unsupported read fields: at_revision');
  assert.equal(local.errorText('en',{code:'CLIENT_QUERY_TIMEOUT'}),'Query timed out. Refresh to retry.');
  assert.equal(local.errorText('zh-CN',new SyntaxError('bad JSON')),'请输入有效的 JSON。');
});
