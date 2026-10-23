# 本地查看器与查询

```sh
outputs/client-venv/bin/mlboard serve --logdir ./outputs/mlboard-runs --port 8081 --open
```

viewer 独立读取已有 RunStore，不启动中心服务、同步数据或修改源 event/ACK/schema。默认监听 `127.0.0.1:8081`，`--port 0` 自动选端口。终端 URL fragment 中的随机 bootstrap 凭据换取 HttpOnly/SameSite cookie 后从地址栏移除。Host/Origin 检查限制跨站读取；非 loopback 监听需要显式 `--token`。远程训练主机通常可使用 SSH 转发。

## 界面设置

支持中文/英文，默认跟随浏览器语言；主题提供 system/light/dark，默认 system 并随系统深浅色更新。偏好只保存在当前浏览器来源，不写入 store 或中心账户。指标名、run path、用户表格数据和协议 JSON 保持原文。

手机实验选择默认折叠，展开后在页面中完整显示；桌面保留侧栏。工作区可选择/对比 run、展开指标组和平滑曲线。每张标量图独立选择 X 轴，默认 step，偏好按 project ID、指标名与完整 context 保存，不强制所有图使用同一轴，也不关联任意其他指标作为 X。结构记录提供 histogram、confusion matrix、ROC、precision–recall、文本和表格；文件仅按受限类型查看或下载，不执行 HTML。

图表以可用宽度绘制，保持约 260px 高、11px 轴字体。resize 只合并重绘现有 frames，语言/主题切换也不重新取数。一次刷新按所选 X 轴分组串行查询，组间共用 5 秒、20 series、20,000 返回点、250,000 扫描点和 2 MiB 预算；选取上限按本次所有可见图计算。查询一次一个、只保留最新 pending；初始 details toggle 去重，旧请求读完后丢弃结果，过期后不继续后续轴组。全部组完成才应用新结果。timer 仅在可见且空闲时刷新。

## 实际支持的本地 API

`/api/v1` 支持 capabilities、projects/categories/runs、series、原始 events、sessions、logs、records、artifacts/content、local/status，以及 `POST /projects/{id}/query`。

查询目前只返回 scalar frames；结构记录使用单独 records 读取端点。支持 primary/显式 producers、all/latest/显式 sessions、latest_logical/all 重复 step 策略。`raw` 与别名 `none` 不降采样；`downsample` 仅支持 `min_max` v1。`consistency`、`reuse`、`viewport`、`read.at_revision` 及未知字段明确报错。没有本地中心账号、项目 ACL 或 dashboard 编辑 API。

```json
{
  "schema_version": "mlboard.query/v1",
  "source": {"kind": "scalar", "metrics": [{"name": "train/loss", "context": {}}]},
  "x": {"field": "step", "range": {"gte": "0", "lte": "1000"}},
  "transform": {"kind": "downsample", "algorithm": "min_max", "points_per_series": 600},
  "page": {"limit": 2000}
}
```

X 轴支持 step/epoch/wall_time/elapsed_seconds/session_elapsed_seconds/processed_samples。整数坐标和范围保持 int63 精度；wall time 按 RFC3339 offset 与纳秒解析。先按 session ordinal/seq 合并重复 step，再过滤 X 范围；缺 X 有诊断，非有限值保留缺口。min/max 按真实 X 分桶，保留 first/min/max/last、缺口边界及相邻有限点，预算不够时报错。

| 预算 | 上限 |
| --- | --- |
| runs / series | 10 / 20 |
| 每 series / 总返回点 | 2,000 / 20,000 |
| 扫描点 / 完整结果 | 250,000 / 2 MiB |
| 查询时间 | 2 秒 |
| 分页快照 | 最多 8 个，120 秒 |

快照游标绑定查询语义，源替换或选中 run 的 cache 删除使其失效。单页刷新不占用或驱逐分页快照。原始 records/logs 使用复合游标，单 event 可跨页而不遗漏。

分类选择中，两个字段都省略表示不限分类；显式 `path_prefixes:[]` 不选分类组，`include_ungrouped:true` 只额外选根级 run。prefix 只匹配后代，排除同名根 run。目录 GET 的 `path_prefix=` 表示根级。summary 的 key 为 canonical JSON `[name,context]`，value 同时含 name/context。

读取使用 `mode=ro`、`query_only` 和短事务，不跨浏览器分页持有源事务。当前没有百万点多分辨率索引；选择较小数据范围，超过预算会报错，不伪造完整结果。
