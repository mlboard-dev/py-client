# TensorBoard 导入

依赖可选，核心 SDK 不要求 TensorFlow 或 TensorBoard。工作区安装与缓存设置见 [README](../README.md)。

```sh
outputs/client-venv/bin/python -m pip install -e './src/py-client[tensorboard]'
outputs/client-venv/bin/mlboard import ./outputs/tb-events --logdir ./outputs/imported --project legacy
outputs/client-venv/bin/mlboard watch ./outputs/tb-events --logdir ./outputs/imported --interval 2
```

当前固定可选依赖为 TensorBoard 2.20.0；验证环境为 Python 3.14.2、protobuf 7.36.2。`watch` 间隔为 0.1–3600 秒。源目录必须存在，文件总数最多 10,000，单条 TFRecord 最大 1 MiB。

导入器增量读取原 TFRecord，验证长度和正文 CRC32C。完整 frame 的 offset 与导入 events 经唯一 collector writer 原子提交；不完整尾部等待后续补全，不提前推进 offset。源文件不被修改。

导入 `simple_value` 和标量 numeric TensorProto。image/audio/graph/histogram/non-scalar tensor 计入 unsupported，不假装转换为 scalar。单个 summary 的重复 tag 使用最后一个值。来源目录、文件身份与 offset 决定稳定 run/session/producer/seq。

每个 tag 保留其原 event 的 step；train 的 `[100,200]` 与 validation 的 `[1,2]` 不会按读取顺序改为同一全局步长。此语义已对照 TensorBoard 2.20.0 的 [`add_scalar`](https://github.com/tensorflow/tensorboard/blob/474d3eeff366a4217899be2814e9b25dc9067083/tensorboard/summary/_writer.py#L70)、[`emit_scalar`](https://github.com/tensorflow/tensorboard/blob/474d3eeff366a4217899be2814e9b25dc9067083/tensorboard/summary/_output.py#L110) 和 [Event 定义](https://github.com/tensorflow/tensorboard/blob/474d3eeff366a4217899be2814e9b25dc9067083/tensorboard/compat/proto/event.proto#L17) 核验：一次 scalar 调用创建独立 event，仅同一 event 的字段共享坐标。SDK 对应用法见 [`log_scalar`](sdk.md)。

源文件被替换或截断会明确拒绝继续，需选择新导入 store；不要编辑 offset 或重用旧身份掩盖替换。损坏 CRC 不推进对应 frame 的 offset。恢复、完整检查与实际 protobuf 测试见[开发与测试](development-and-testing.md)。
