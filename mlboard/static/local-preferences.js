(function(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  else root.MLBoardLocal=api;
})(globalThis,function(){
  'use strict';
  const AXES=['step','epoch','wall_time','elapsed_seconds','session_elapsed_seconds','processed_samples'];
  const messages={
    en:{
      'page.title':'MLBoard · Local experiments','settings.language':'Language','settings.theme':'Theme',
      'theme.system':'System','theme.light':'Light','theme.dark':'Dark',
      'nav.workspace':'Workspace','nav.config':'Config & summary','nav.records':'Structured data','nav.files':'Files','nav.logs':'Logs','nav.query':'Query inspector','nav.storage':'Storage & sync',
      'view.workspace':'Experiment workspace','view.config':'Run configuration','view.records':'Structured data','view.files':'Recorded files','view.logs':'Training logs','view.query':'Query inspector','view.storage':'Storage & sync',
      'experiments.heading':'Experiments','experiments.selected':'Experiments · {count} selected','experiments.ungrouped':'Ungrouped','experiments.project':'Project','experiments.search':'Search run paths',
      'action.loadMore':'Load more','action.refresh':'Refresh','action.runQuery':'Run query','action.nextPage':'Next page','action.openMedia':'Open media','action.inspectPayload':'Inspect payload',
      'empty.experiments':'No experiments recorded yet.','empty.selection':'Select an experiment in the list.','empty.scalars':'No scalar observations have been recorded yet.','empty.finite':'No finite observations on this axis. Missing values are not zero.','empty.files':'No files recorded.','empty.records':'No structured data recorded.','empty.logs':'No logs recorded.',
      'chart.axis':'X axis','chart.smoothing':'Smoothing','chart.metrics':'Metrics','chart.metricCount':{one:'{count} metric',other:'{count} metrics'},'chart.loading':'Loading observations…','chart.limit':'Query limit reached. Collapse another metric group or use Query inspector.','chart.updated':'{count} points · {time}','chart.description':'Recorded metric curves',
      'axis.step':'Step','axis.epoch':'Epoch','axis.wall_time':'Wall time','axis.elapsed_seconds':'Elapsed seconds','axis.session_elapsed_seconds':'Session seconds','axis.processed_samples':'Processed samples',
      'state.pending':'Pending','state.running':'Running','state.finished':'Finished','state.failed':'Failed','state.stopped':'Stopped',
      'summary.metric':'Metric','summary.context':'Context','summary.last':'Last','summary.min':'Min','summary.max':'Max','summary.count':'Count','detail.config':'Config','detail.summary':'Summary · primary logical stream','detail.source':'Source & sessions',
      'kind.histogram':'Histogram','kind.confusion_matrix':'Confusion matrix','kind.curve':'Curve','kind.table':'Table','kind.text':'Text','kind.media':'Media','record.step':'step {step}','record.confusionHeader':'Actual / predicted','record.roc':'ROC · false positive rate → true positive rate','record.pr':'Precision–recall · recall → precision',
      'query.description':'The bounded read API drives these curves. Source data remains read-only.','query.json':'Query JSON',
      'storage.events':'Retained events','storage.pending':'Pending uploads','storage.acked':'Acknowledged','storage.historyBytes':'History bytes','storage.mediaBytes':'Media bytes','storage.retention':'record retains history after ACK. cache may have incomplete local coverage. sync=off creates no pending upload references.','storage.resume':'Resume uploads from the terminal:',
      'error.request':'The request failed. Refresh to retry.','error.network':'Cannot reach the local viewer. Check that it is running.','error.session':'The local session could not be opened. Reopen the session URL.','error.runLimit':'Select at most 10 runs.','error.metricLimit':'More than 1,000 metrics. Use Query inspector with explicit names.',
      'error.unauthorized':'Open the session URL printed by mlboard serve.','error.invalidHost':'This host is not allowed.','error.invalidOrigin':'Cross-origin access is disabled.','error.invalidRequest':'The request is invalid.','error.bodyTooLarge':'The request body exceeds the limit.','error.invalidJson':'Enter valid JSON.','error.notFound':'The requested item was not found.','error.readOnly':'This source is read-only.','error.invalidArgument':'A request argument is invalid.','error.invalidCursor':'The page cursor is invalid.','error.invalidArtifact':'The media reference is invalid.',
      'error.sourceBusy':'The local source is busy. Retry shortly.','error.sourceUnavailable':'The local source is unavailable. Check the log directory.','error.budget':'The query exceeds its budget. Select fewer runs, metrics or points.','error.timeout':'Query timed out. Refresh to retry.','error.busy':'A local query is still running. Retry shortly.','error.snapshotExpired':'The page snapshot expired or its source changed. Run the query again.','error.snapshotLimit':'The paged-query limit is reached. Retry after existing snapshots expire.','error.invalidQuery':'The query is invalid. See Query inspector for details.','error.unsupported':'The requested query operation is not supported locally.'
    },
    'zh-CN':{
      'page.title':'MLBoard · 本地实验','settings.language':'语言','settings.theme':'主题',
      'theme.system':'跟随系统','theme.light':'浅色','theme.dark':'深色',
      'nav.workspace':'工作区','nav.config':'配置与摘要','nav.records':'结构化数据','nav.files':'文件','nav.logs':'日志','nav.query':'查询检查器','nav.storage':'存储与同步',
      'view.workspace':'实验工作区','view.config':'实验配置','view.records':'结构化数据','view.files':'记录的文件','view.logs':'训练日志','view.query':'查询检查器','view.storage':'存储与同步',
      'experiments.heading':'实验','experiments.selected':'实验 · 已选 {count} 项','experiments.ungrouped':'未分类','experiments.project':'项目','experiments.search':'搜索实验路径',
      'action.loadMore':'加载更多','action.refresh':'刷新','action.runQuery':'运行查询','action.nextPage':'下一页','action.openMedia':'打开媒体','action.inspectPayload':'查看原始数据',
      'empty.experiments':'尚无实验记录。','empty.selection':'请在列表中选择实验。','empty.scalars':'尚无标量记录。','empty.finite':'此坐标轴上没有有限值；缺失值不会补零。','empty.files':'尚无文件记录。','empty.records':'尚无结构化数据记录。','empty.logs':'尚无日志记录。',
      'chart.axis':'X 轴','chart.smoothing':'平滑','chart.metrics':'指标','chart.metricCount':{other:'{count} 个指标'},'chart.loading':'正在加载记录…','chart.limit':'已达查询上限，请收起其他指标组或使用查询检查器。','chart.updated':'{count} 个点 · {time}','chart.description':'已记录的指标曲线',
      'axis.step':'步数','axis.epoch':'轮次','axis.wall_time':'时间','axis.elapsed_seconds':'累计秒数','axis.session_elapsed_seconds':'会话秒数','axis.processed_samples':'已处理样本数',
      'state.pending':'等待中','state.running':'运行中','state.finished':'已完成','state.failed':'失败','state.stopped':'已停止',
      'summary.metric':'指标','summary.context':'上下文','summary.last':'最近值','summary.min':'最小值','summary.max':'最大值','summary.count':'数量','detail.config':'配置','detail.summary':'摘要 · 主逻辑流','detail.source':'来源与会话',
      'kind.histogram':'直方图','kind.confusion_matrix':'混淆矩阵','kind.curve':'曲线','kind.table':'表格','kind.text':'文本','kind.media':'媒体','record.step':'步数 {step}','record.confusionHeader':'实际 / 预测','record.roc':'ROC · 假正例率 → 真正例率','record.pr':'精确率–召回率 · 召回率 → 精确率',
      'query.description':'曲线使用有界读取 API，源数据保持只读。','query.json':'查询 JSON',
      'storage.events':'保留的事件','storage.pending':'待上传','storage.acked':'已确认','storage.historyBytes':'历史数据大小','storage.mediaBytes':'媒体大小','storage.retention':'record 在 ACK 后保留历史；cache 的本地覆盖可能不完整；sync=off 不创建待上传引用。','storage.resume':'在终端恢复上传：',
      'error.request':'请求失败，请刷新重试。','error.network':'无法连接本地查看器，请确认服务正在运行。','error.session':'无法建立本地会话，请重新打开终端提供的会话地址。','error.runLimit':'最多选择 10 个实验。','error.metricLimit':'指标超过 1,000 个，请在查询检查器中明确选择名称。',
      'error.unauthorized':'请打开 mlboard serve 输出的会话地址。','error.invalidHost':'不允许访问此主机。','error.invalidOrigin':'不允许跨来源访问。','error.invalidRequest':'请求无效。','error.bodyTooLarge':'请求正文超过上限。','error.invalidJson':'请输入有效的 JSON。','error.notFound':'未找到请求的项目。','error.readOnly':'此数据源只允许读取。','error.invalidArgument':'请求参数无效。','error.invalidCursor':'分页游标无效。','error.invalidArtifact':'媒体引用无效。',
      'error.sourceBusy':'本地数据源繁忙，请稍后重试。','error.sourceUnavailable':'本地数据源不可用，请检查日志目录。','error.budget':'查询超过预算，请减少实验、指标或点数。','error.timeout':'查询超时，请刷新重试。','error.busy':'仍有本地查询运行中，请稍后重试。','error.snapshotExpired':'分页快照已过期或源数据已变更，请重新运行查询。','error.snapshotLimit':'已达分页查询上限，请等待现有快照过期后重试。','error.invalidQuery':'查询无效，请在查询检查器中查看详情。','error.unsupported':'本地查看器不支持请求的查询操作。'
    }
  };
  const errorKeys={SESSION_FAILED:'error.session',RUN_LIMIT:'error.runLimit',METRIC_LIMIT:'error.metricLimit',UNAUTHORIZED:'error.unauthorized',INVALID_HOST:'error.invalidHost',INVALID_ORIGIN:'error.invalidOrigin',INVALID_REQUEST:'error.invalidRequest',BODY_TOO_LARGE:'error.bodyTooLarge',INVALID_JSON:'error.invalidJson',NOT_FOUND:'error.notFound',READ_ONLY:'error.readOnly',INVALID_ARGUMENT:'error.invalidArgument',INVALID_CURSOR:'error.invalidCursor',INVALID_ARTIFACT:'error.invalidArtifact',SOURCE_BUSY:'error.sourceBusy',SOURCE_UNAVAILABLE:'error.sourceUnavailable',BUDGET_EXCEEDED:'error.budget',CLIENT_BUDGET_EXCEEDED:'error.budget',QUERY_TIMEOUT:'error.timeout',CLIENT_QUERY_TIMEOUT:'error.timeout',QUERY_BUSY:'error.busy',SNAPSHOT_EXPIRED:'error.snapshotExpired',SNAPSHOT_LIMIT:'error.snapshotLimit',INVALID_QUERY:'error.invalidQuery',UNSUPPORTED_OPERATION:'error.unsupported'};
  function normalizeLanguage(value){return /^zh(?:-|$)/i.test(String(value||''))?'zh-CN':'en'}
  function number(language,value,options={}){return new Intl.NumberFormat(language,options).format(value)}
  function translate(language,key,params={}){
    const locale=normalizeLanguage(language);
    let value=messages[locale][key]??messages.en[key]??key;
    if(typeof value==='object')value=value[new Intl.PluralRules(locale).select(params.count)]??value.other;
    return value.replace(/\{([A-Za-z_][A-Za-z0-9_]*)\}/g,(token,name)=>{
      const replacement=params[name];
      if(replacement===undefined)return token;
      return typeof replacement==='number'||typeof replacement==='bigint'?number(locale,replacement):String(replacement);
    });
  }
  function errorText(language,error){
    const key=errorKeys[error?.code]||(error?.name==='SyntaxError'?'error.invalidJson':error?.name==='TypeError'?'error.network':'error.request');
    return translate(language,key);
  }
  function read(storage,key){try{return storage?.getItem(key)??null}catch{return null}}
  function write(storage,key,value){try{storage?.setItem(key,value)}catch{}}
  function createPreferences({storage,language='en',media}={}){
    const storedLanguage=read(storage,'mlboard-local-language');
    let selectedLanguage=['en','zh-CN'].includes(storedLanguage)?storedLanguage:normalizeLanguage(language);
    let selectedTheme=read(storage,'mlboard-local-theme');
    if(!['system','light','dark'].includes(selectedTheme))selectedTheme='system';
    const listeners=new Set();
    const snapshot=()=>({language:selectedLanguage,theme:selectedTheme,resolvedTheme:selectedTheme==='system'?(media?.matches?'dark':'light'):selectedTheme});
    const emit=()=>{for(const listener of listeners)listener(snapshot())};
    const mediaChange=()=>{if(selectedTheme==='system')emit()};
    if(media?.addEventListener)media.addEventListener('change',mediaChange);
    else if(media?.addListener)media.addListener(mediaChange);
    return {
      snapshot,
      subscribe(listener){listeners.add(listener);return()=>listeners.delete(listener)},
      setLanguage(value){if(!['en','zh-CN'].includes(value))throw RangeError('unsupported language');selectedLanguage=value;write(storage,'mlboard-local-language',value);emit()},
      setTheme(value){if(!['system','light','dark'].includes(value))throw RangeError('unsupported theme');selectedTheme=value;write(storage,'mlboard-local-theme',value);emit()},
      dispose(){if(media?.removeEventListener)media.removeEventListener('change',mediaChange);else if(media?.removeListener)media.removeListener(mediaChange);listeners.clear()}
    };
  }
  function axisPreferenceKey(project,metric){
    const context=Object.fromEntries(Object.keys(metric.context||{}).sort().map(key=>[key,metric.context[key]]));
    return 'mlboard-local-axis:'+JSON.stringify([project,metric.name,context]);
  }
  function readAxis(storage,project,metric){const value=read(storage,axisPreferenceKey(project,metric));return AXES.includes(value)?value:'step'}
  function writeAxis(storage,project,metric,axis){if(!AXES.includes(axis))throw RangeError('unsupported axis');write(storage,axisPreferenceKey(project,metric),axis)}
  return {AXES,messages,errorKeys,normalizeLanguage,number,translate,errorText,createPreferences,axisPreferenceKey,readAxis,writeAxis};
});
