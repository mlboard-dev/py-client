# 同步与 key

本地记录是默认行为；仅传 `server=` 不会启用上传。上传使用中心服务的真实项目/资源权限，SDK 不模拟账户或权限成功。

## 持续上传与手工恢复

```python
run = mlboard.init(
    project="stable-project-id", run_path="baseline/seed-1",
    spool_dir="./outputs/mlboard-runs",
    sync="background", server="http://127.0.0.1:8080",
    # api_key 可显式传入；通常由 MLBOARD_API_KEY 注入。
)
```

```sh
export MLBOARD_SERVER=http://127.0.0.1:8080
# 通过部署的秘密机制设置 MLBOARD_API_KEY，不写进脚本或仓库。
outputs/client-venv/bin/mlboard sync ./outputs/mlboard-runs --timeout 30
outputs/client-venv/bin/mlboard sync ./outputs/mlboard-runs --project vision --timeout 30
```

`--project` 选择本地项目并仅等待该项目检查点；不同远端的 backlog 不应被一起改绑。无效超时、权限、完整验证和远端完成失败以非零 CLI 状态退出。

优先使用稳定 project ID。现有 slug 可经 `/projects` 解析；项目必须对当前 key 可见。SDK 先匹配 ID，再匹配 slug，不按可重复的显示名选择项目。创建项目需显式 `create_project=True` / `--create-project`，以及具有 `projects:write` scope 的 global key；project key 只能使用已有项目。

## 选择创建项目的组织

```python
run = mlboard.init(
    "new-project-slug", "baseline/seed-1",
    spool_dir="./outputs/mlboard-runs", sync="background",
    create_project=True, organization="stable-organization-id",  # 也可用组织 slug
)
```

```sh
export MLBOARD_ORGANIZATION=stable-organization-id
outputs/client-venv/bin/mlboard sync ./outputs/mlboard-runs --create-project --organization stable-organization-id
```

显式 `organization` / `--organization` 优先于 `MLBOARD_ORGANIZATION`。ID 可由已可见项目的 `organization_id` 确认；组织 slug 经 `/organizations` 唯一解析，再向 `POST /projects` 传 `organization_id`。发现组织不授予项目读取或创建权限，最终仍由中心验证。project key 使用预创建 project ID 时不需要组织发现权限。

省略组织时，SDK 不选第一个组织；只保留服务端「恰有一个可创建组织」的兼容行为，否则创建返回 422。离线初始化保存组织选择，首次同步保存稳定 organization/project ID。collector 重启后的手工 sync 沿用这些映射；显式传组织会重新确认远端检查点，冲突报错，不重建或改绑 run。绑定后即使 slug 改名也优先用稳定 ID。

## 凭据与映射

Global/project bearer key 取显式参数或 `MLBOARD_API_KEY`；默认中心为 `http://127.0.0.1:8080`。keys 只留在进程内存，不写入 event、RunStore metadata 或诊断。API 拒绝重定向，避免 Authorization 跟随到另一来源。

每个 run 绑定一个 server/organization/project 映射，改变目标会被拒绝。组织选择与解析后的 ID 是非秘密 metadata，不会写入 event 正文。collector 重启后，新注册只恢复其显式 run 的凭据；旧 run 需要自己的注册或手工 sync，不能借用新 run 的 key。

注册 run/session/producer 后才发送 event。每批最多 512 events 且小于 1 MiB；标量批次后每轮最多上传一个媒体文件，引用该文件的 event 等文件上传完成。终态 PATCH 使用修订号，遇 412 有限重读重试。ACK 丢失时发送完全相同的身份和正文。

`retention="record"` 保留上传后的 history；`cache` 的回收行为见[存储与恢复](storage-and-recovery.md)。细粒度 key 的 scope、project ACL 和 run 限制均由中心再次验证；组织成员身份不自动授予项目访问权。
