# 开发、测试与排查

从 MLBoard 工作区根目录运行，不向全局环境安装依赖。工具链、cache、临时文件和测试产物都保留在工作区 `outputs/`。

```sh
export MLBOARD_ROOT="$PWD"
. ./scripts/env.sh
python3 -m venv outputs/client-test-venv
outputs/client-test-venv/bin/python -m pip install -e './src/py-client[tensorboard]'
PYTHONPATH=src/py-client outputs/client-test-venv/bin/python -m unittest discover -s src/py-client/tests -v
node --test src/py-client/tests/test_*.mjs
```

不安装 TensorBoard 时，其真实 protobuf 测试会跳过；发布验收应安装固定可选依赖，检查没有该 skip。Node 测试使用内置 test/assert/vm，不安装额外 Node 包。viewer 没有 frontend bundle 构建步骤，HTML/JS/CSS 随 Python 包分发。

## 真实演示数据

```sh
PYTHONPATH=src/py-client outputs/client-test-venv/bin/python src/py-client/examples/demo.py --logdir outputs/demo
PYTHONPATH=src/py-client outputs/client-test-venv/bin/python -m mlboard serve --logdir outputs/demo
```

demo 通过真实 SDK/collector 写入四个确定性合成实验，含标量、原生结构、文件和日志；不会在中心默认插入数据。`--sync` 使用已有中心/key，创建项目另需 `--create-project`。选择的 finish 检查点未完成时 demo 退出失败。

## 回归覆盖

Python 测试覆盖内存热路径不联网/读盘/转换 tensor、有限 finish、NaN/Inf、保留训练异常、真实 collector kill/reconnect、恢复 session、不可变媒体、身份冲突、ACK 丢失重放、空 run 和终态失败、key 跨来源隔离、离线组织映射/ID 优先与歧义拒绝、record/cache、配额、长历史 append 工作量、查询精度/快照/缺口/summary，以及本地 Host/Origin/auth、静态资源白名单与复合分页。

Node 回归覆盖重复图表请求合并、单 active/latest pending、切代次排空、失败继续调度、有界 429 重试、超时、逐图轴偏好、分组共享预算/截止时间/过期取消，以及本地语言/主题设置。页面修改需实际检查桌面和 390×844 手机：实验选择展开后能到最后一项，曲线和轴文字可读，语言/主题切换不改用户数据、不额外争抢查询槽。

## 排查顺序

1. 用 `run.status()` / `mlboard status` 区分内存入队、本地 durable、待上传与 manifest 检查点。
2. 检查 `<logdir>/diagnostics/collector.log`、磁盘空间和 finish reason；不要公开 endpoint 或 key。
3. 本地 viewer 错误先看 query inspector 和 capabilities 实际预算；未知操作应报错，不应静默忽略。
4. 中心同步先验证项目稳定 ID、key scope 和当前 project ACL；远端错误不得通过更换事件身份绕开。
5. 需要恢复时保留原目录，在新的 logdir 验证；备份边界见[存储与恢复](storage-and-recovery.md)。

与中心服务联调、容器或 PostgreSQL 验证由根工作区脚本负责；Python 单测通过不代表整栈发布验收完成。
