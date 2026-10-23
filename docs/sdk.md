# Python SDK

`mlboard` 支持 Python 3.11+，核心仅使用标准库。`import mlboard` 不导入 viewer 或 TensorBoard。所有示例从 MLBoard 工作区根目录运行；先设置 `MLBOARD_ROOT` 并加载 `scripts/env.sh`，见 [README](../README.md)。

## 记录一个实验

```python
import mlboard

with mlboard.init(
    project="vision",  # 可使用稳定 project ID
    run_path="run-series1/resnet18/seed-1",
    config={"lr": 0.01, "seed": 1},
    tags=["baseline"],
    spool_dir="./outputs/mlboard-runs",
    sync="off",
    retention="record",
) as run:
    for step in range(100):
        receipt = run.log({"train/loss": 1 / (step + 1)}, step=step, epoch=step / 20)
        if not receipt.accepted:
            print(receipt.reason)
    print(run.status())
```

`mlboard.log/status/finish` 使用当前 Python context 最近初始化的 run；多个实验建议保留各自的 `Run`。run path 只作分类元数据，不会变成存储文件路径。

`log_scalar` 记录单个指标，run 与 module-level 接口都接受与 `log` 相同的坐标。每次调用可使用自己的 step；仅同一次 `log({...})` 的 fields 共用该 event 坐标，不会按 seq 或全 run 步长改写。

```python
run.log_scalar("train/loss", 0.8, step=100)
mlboard.log_scalar("val/loss", 0.9, step=1)
run.log_scalar("train/loss", 0.6, step=200)
mlboard.log_scalar("val/loss", 0.7, step=2)
```

两条 series 分别保留 `[100,200]` 与 `[1,2]`。此行为对应 TensorBoard 每次 `add_scalar(tag,data,step,wall_time)` 独立创建 event；不是任意指标间的坐标 join。

## 训练热路径与完成检查点

`init()` 分配身份并启动 daemon sender，不等待磁盘、collector 启动或远端网络。`log()` 只验证有界的内置 CPU 值并尝试非阻塞入内存队列，不打开 SQLite、读取媒体文件、联网，或调用 tensor 的 `.item()`、`.cpu()`、`.numpy()`。请调用者显式完成 tensor 的 CPU 转换。Python 调度和 GC 不提供硬实时延迟保证。

默认队列最多 512 events / 8 MiB，每 event 最多 128 fields / 64 KiB。队列满或锁竞争时丢弃新 event，并返回 `accepted=False`。`accepted=True` 仅表示已入内存队列。

| 检查点 | 调用 | 成功的含义 |
| --- | --- | --- |
| 本地 | `run.finish(timeout=2, wait_for="local")` | 已接受事件及终态提交 SQLite |
| 远端 | `run.finish(timeout=2, wait_for="remote")` | 本地完成、事件 ACK、run/session/producer 注册及远端终态确认 |
| 不等待 | `run.finish(wait_for="none")` | 请求完成；不承诺持久性 |

超时返回不完整 receipt，sender 仍尝试排空队列；进程结束会丢失尚未本地持久化的事件。空 run 也必须确认远端元数据。事件 ACK 后终态 PATCH 失败不算远端完成。可在本地完成后再次 `finish(wait_for="remote")`，新调用在有限预算内刷新检查点，必要时恢复该 run 原有凭据。

`mode="safe"` 将日志错误放入 receipt/status；`mode="strict"` 对验证失败和显式未完成的 finish 抛异常。无效初始化参数直接抛异常。context manager 以 `failed` 记录训练异常，并保留原异常；正常退出会执行默认本地 finish。

## 结构化数据与文件

```python
run.log_histogram("weights/layer1", bin_edges=[-2, -1, 0, 1, 2], counts=[1, 4, 4, 1], step=9)
run.log_confusion_matrix("val/confusion", [[43, 7], [4, 46]], labels=["cat", "dog"], step=9)
run.log_roc("val/roc", thresholds=[float("-inf"), .3, .7, .9, float("inf")],
            tp=[10, 9, 6, 2, 0], fp=[10, 4, 1, 0, 0],
            fn=[0, 1, 4, 8, 10], tn=[0, 6, 9, 10, 10], step=9)
run.log_text("notes/training", "Learning rate reduced after epoch 2", step=9)
run.log_table("val/examples", ["label", "prediction"], [["cat", "cat"]], step=9)
run.log_media("files/report", "./outputs/report.txt", kind="file", step=9)
run.log_message("Checkpoint completed", level="info", step=9)
```

histogram、matrix、threshold curve 需要 step。helper 只处理显式提供的有界 CPU 数据，不采样数组或访问 GPU。`log_pr()` 与 `log_roc()` 共用阈值/count schema；由 TP/FP/FN/TN 导出 rates 和梯形 ROC AUC，只接受 `averaging="binary"|"micro"|"per_class"`。聚合 counts 无法直接给出 macro/weighted 曲线，因此拒绝这些标签。

NaN/Inf 标量保存显式状态，JSON 中数值为 null；图表显示缺口，不补零。context 值为有限原始 JSON 值，名称与完整 context 一同标识 series。

`log_media()` 仅排队路径引用；本地完成前必须保留源文件且不修改。独立 worker 检查普通文件、256 MiB 上限、大小/mtime，流式复制、hash、fsync 后发布快照。其队列有界，失败在 status/finish 中可见。离线稳定 artifact UUID 先上传，再发送引用它的不可变 event。

## 恢复与多 producer

`run_id=existing_id` 为恢复训练创建新持久 session ordinal；同一 session/producer 不允许用新 token 从 seq 0 重新开始。DDP 可显式共享 run/session/ordinal/primary producer，各 rank 使用不同 producer ID。SDK 没有隐式 barrier、reduction 或跨进程协调。

持久化、保留策略与故障处理见[存储与恢复](storage-and-recovery.md)；上传见[同步与 key](sync-and-keys.md)。
