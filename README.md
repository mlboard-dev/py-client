# MLBoard Python client

Python 3.11+ 的轻依赖实验记录 SDK，包含独立 collector、本地 SQLite history/outbox、HTTP 同步、只读本地 viewer 和可选 TensorBoard 导入。核心仅使用标准库，中心服务数据库与本地 RunStore 分离。

## 安装与运行

从整个 MLBoard 工作区根目录运行，让工具链、cache 和临时文件保留在仓库内：

```sh
export MLBOARD_ROOT="$PWD"
. ./scripts/env.sh
python3 -m venv outputs/client-venv
outputs/client-venv/bin/python -m pip install -e './src/py-client[serve]'
```

`serve` extra 当前没有额外依赖。源码方式可设 `PYTHONPATH=src/py-client`。

```python
import mlboard

with mlboard.init("vision", "baseline/seed-1", spool_dir="./outputs/mlboard-runs") as run:
    for step in range(100):
        receipt = run.log({"train/loss": 1 / (step + 1)}, step=step)
        if not receipt.accepted:
            print(receipt.reason)
```

```sh
outputs/client-venv/bin/mlboard serve --logdir ./outputs/mlboard-runs --port 8081 --open
```

默认只在本地记录，并在远端 ACK 后保留历史。`log()` 成功仅表示内存入队；`finish(wait_for="local")` 才检查本地提交，`wait_for="remote"` 还检查 ACK 和远端终态。热路径不访问磁盘、网络或 GPU。

## 文档

- [SDK 与结构化记录](docs/sdk.md)：receipt、finish、safe/strict、媒体、恢复与 DDP。
- [持久化、保留与恢复](docs/storage-and-recovery.md)：collector、WAL、outbox、record/cache、配额和故障边界。
- [同步与 key](docs/sync-and-keys.md)：后台/手工上传、项目 ID、权限、凭据隔离与幂等。
- [本地 viewer 与查询](docs/local-viewer.md)：手机查看、语言/主题、认证、查询语义和预算。
- [TensorBoard 导入](docs/tensorboard.md)：可选依赖、增量 offset、CRC、watch 与明确不支持的数据。
- [开发与测试](docs/development-and-testing.md)：仓库内环境、真实 demo、Python/Node 回归与排查。

已有原始历史不会因上传成功自动删除。显式 `retention="cache"` 会回收已 ACK 数据；当前不宣称网络文件系统、Windows collector、大媒体压力或百万点交互性能已验证。
