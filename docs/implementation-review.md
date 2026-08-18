# ForgeReplay v0.2 实现与总体 Review

> Review 日期：2026-08-19  
> 上游基线：`rasbt/mini-coding-agent@717cae4`  
> 上游标签：`upstream-baseline-717cae4`  
> 实现分支：`codex/durable-agent-runtime`

## 1. 结论

ForgeReplay 已经从上游的教学型单文件 Agent，形成一个可以独立演示和审计的 Coding Agent Harness：它能把模型响应、审批、预算、工具意图、实际派发、结果和终态持久化；能在文件副作用崩溃窗口中恢复；会对无法证明的 Shell 结果安全停机；每个任务在独立 Git Worktree 中运行，并能导出结果和脱敏事件轨迹。

对个人项目的准确定位是“成熟的作品集级 Durable Coding Agent Harness”，而不是“生产级多租户 Agent 平台”。核心可靠性闭环、测试和评测已经具备；真正的 OS 沙箱、远端 Worker、高可用调度、秘密管理和多 Agent 编排不在 v0.2 范围内。

## 2. 已交付能力

| 领域 | 已实现内容 | 可核验证据 |
|---|---|---|
| Durable Ledger | SQLite WAL/FULL、Schema Migration、append-only 类型化事件、SHA-256 Blob、确定性 Projection | `persistence/`、事件/重建/损坏测试 |
| Resume | Checkpoint + tail replay；模型响应先持久化；工具状态可恢复 | checkpoint、projection、runtime 测试 |
| Tool Identity | UUIDv7、canonical args、approval fingerprint、稳定逻辑调用 | tool identity/call 测试 |
| File Effects | 原子写、前后 Hash、冲突检测、dispatch 后崩溃 reconciliation | file tools/executor 故障测试 |
| Process Effects | argv-only、超时、输出上限、最小环境、进程树清理；未知结果不重放 | process/shell executor 测试 |
| Governance | 持久审批、预算 reserve/settle、取消、模型有界重试、Run Lease/Fencing | approval/budget/model/lease 测试 |
| Workspace | 干净仓库预检、独立 Worktree、路径防逃逸、结果导出、clean-only 清理 | workspace/path/result 测试 |
| Operations | start/resume/approve/cancel/status/trace/export/cleanup CLI | CLI 集成测试 |
| Evaluation | 共享故障点 A/B、10k event replay microbenchmark、24 题 16/8 真实模型任务集 | `eval/` 与原始 JSON 报告 |

## 3. 状态机和恢复语义 Review

### 3.1 可以安全自动恢复

- 读取、列目录和搜索属于无副作用操作，可以重新执行。
- `write_file` / `patch_file` 在派发前保存意图和预条件；崩溃后比较 before/after SHA-256。目标已是 after 状态时复用成功结果；仍为 before 状态时执行一次；两者都不匹配时进入冲突而不是覆盖。
- Checkpoint 是缓存，不是事实来源。损坏或版本不匹配时可以从 append-only 事件重建 Projection。
- 同一逻辑 Tool Call 和审批决定具有唯一约束，重复提交不会产生第二个逻辑调用。

### 3.2 必须安全停止

- 任意进程已派发但 receipt 未持久化时，系统无法仅靠 PID 证明结果，因此标记 `UNCERTAIN` / `NEEDS_ATTENTION`，不会盲目重跑。
- 路径逃逸、符号链接重定向、保留设备名、超出文件/输出限制会被拒绝。
- 预算耗尽、取消请求和模型最终失败都会形成显式终态。

### 3.3 不宣称的语义

- 不宣称任意 Shell exactly-once。
- Worktree 只保护 base checkout，不等同于容器或 VM 沙箱。
- 当前 SQLite + 本机 Worktree 是单机执行架构，不是分布式高可用调度器。
- 当前 Checkpoint 优化证明的是 Projection reducer CPU 开销，不是完整进程重启时延。

## 4. 量化结果 Review

### 4.1 故障恢复

固定 12 个确定性任务，在 hardened 和 upstream-semantics adapter 上分别触发 `before_effect`、`after_effect_before_persist` 两个共同文件副作用边界：

- Hardened：24/24 达到安全终态并通过后置条件。
- Baseline adapter：0/24 能判断安全终态；其中 12/24 因崩溃发生在写后而偶然满足文件后置条件，但没有恢复证据。
- Hardened 覆盖运行中的重复文件副作用：0；必须连同分母“24 次、两个 crash window”一起陈述。
- 自动恢复处理时延：P50 15.99 ms、P95 18.66 ms；不包含模型调用。

这组实验只证明共享文件 crash window 的 Harness 语义，不是 Coding Agent 任务成功率，也不能外推到任意外部 API 或 Shell。

### 4.2 Projection 性能

在 Windows 11 / Python 3.13.12 的本机 CPU microbenchmark 中，10,000 个事件、50 次重复：

- 全量 reducer replay P50：42.37 ms。
- 从 checkpoint 后 200 个 tail events replay P50：0.78 ms。
- P50 reducer speedup：54.65x。

结果反映纯 Projection replay 成本；SQLite 打开、Blob 加载、Worktree 检查、模型和工具时延不在测量范围。

### 4.3 真实模型 Coding 能力

已冻结 24 题、6 类、16 Dev + 8 Held-out 的 `forge-replay-coding-tasks-v1`，每题从失败 seed commit 开始，隐藏 evaluator 位于 Agent Worktree 之外。Runner 会保存 task/run manifest、event trace、binary patch、测试结果和耗时，并把所有已启动运行按 intent-to-treat 计入分母。

当前机器未安装 Ollama，所以没有发布真实 Task Success Rate。Scripted Model 只能验证状态机，不能作为 Coding 成功率。Held-out 正式数字应在无秘密、默认断网的一次性环境中，固定模型 digest、参数、硬件和每题 3 次运行后生成。

## 5. 本轮 Review 发现并修复的问题

1. 原 Runtime 的租约只在 Run 开始获取一次，长循环可能超过 TTL。已改为每个模型/工具边界续租，并保留 fencing epoch。
2. 模型最终失败曾只记录事件后向 CLI 抛异常。现在转为明确 `NEEDS_ATTENTION` 终态。
3. 结果导出中途失败可能留下看似正式的目录。现在先写 staging，完整后原子发布。
4. Workspace disposition 曾允许任意状态跳转。现在只接受白名单迁移。
5. 故障报告曾对不能恢复的 baseline 计算接近 0 ms 的“恢复时延”。现在仅对安全恢复样本报告时延，并加入 planned/started/triggered/evaluable 分母和按故障分层结果。
6. 原 CLI 缺少取消和可审计轨迹出口。现已增加 durable cancel、blob-content-excluded trace 和运行计数。
7. 真实模型基准若直接运行会执行模型生成的代码。Runner 现在默认拒绝，要求显式确认，并在文档中把一次性隔离环境设为前置条件。

## 6. 剩余风险与下一版本

| 优先级 | 风险 | v0.3 建议 |
|---|---|---|
| P0 | 本地 Process Supervisor 不是安全沙箱 | 接入无秘密、默认断网、只读基础镜像的一次性 VM/容器后端 |
| P1 | SQLite 写入 API 尚未对每次事务强制校验 fencing token | 将 lease epoch 作为所有执行期变更的 compare-and-swap 条件 |
| P1 | 任意 Shell crash window 只能 UNKNOWN | 为明确幂等的命令引入 receipt adapter；其余继续人工 reconcile |
| P1 | 没有真实模型 Held-out 结果 | 在隔离环境固定模型 digest 后跑 8×3 held-out，并提交原始报告 |
| P2 | 未提供远端 Artifact Store / Worker Lease 服务 | 数据量和并发需求出现后再拆分，不提前复杂化 |
| P2 | 没有 OpenTelemetry exporter | 事件 trace 已可导出；下一版再接 OTLP，不把 run_id 放高基数 metrics label |

## 7. 发布验收清单

- 全量测试通过。
- ForgeReplay 新增代码和测试通过 Ruff；上游遗留告警单独记录，不伪装为新增问题。
- `git diff --check` 无空白错误。
- 两个确定性 benchmark 可从命令重跑并生成带 Git/平台信息的 JSON。
- 24 个 task seed 全部在修改前被隐藏 evaluator 证明失败。
- README 明确上游归属、个人增量、量化边界和沙箱限制。
- 每个实施阶段由独立 Commit 表达，并已推送个人远端分支。
