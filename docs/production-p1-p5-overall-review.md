# ForgeReplay v0.4 P1-P5 总体验收报告

验收日期：2026-08-19  
验收分支：`codex/production-p1-p5`  
设计基线：`docs/production-coding-agent-upgrade.md`

## 1. 最终结论

P1-P5 的核心控制机制已经形成可执行、可测试的代码闭环，P0 的 Durable
Harness 不变量在升级过程中保持成立。该版本可以称为“生产导向的 Coding
Agent Harness 与控制面参考实现”，不能称为“已经 GA 的多租户托管服务”。

原因很明确：仓库能够实现并拒绝不安全状态，但无法仅靠源代码制造 Linux
gVisor、真实 PostgreSQL 集群、多机 Worker、对象存储、KMS/WORM、多可用区、
28 天 SLO 和渗透测试证据。`GaReadinessGate` 会在这些证据缺失时拒绝 GA。

## 2. 分阶段验收

| 阶段 | 已完成的代码闭环 | 本地验证 | 仍需部署环境提供的证据 |
|---|---|---|---|
| P1 | OCI/gVisor Provider、不可变镜像、非 Root、只读 RootFS、默认断网、资源限制、Runtime Attestation、签名策略、CLI 生产模式 Fail-closed 和退出销毁 | 精确校验创建参数、Attestation 拒绝、路径映射、回执和清理 | Linux `runsc`、cgroup/seccomp、Canary Credential、网络命名空间与 Escape 测试 |
| P2 | PostgreSQL 真相源、Tenant RLS、Run-local Stream CAS、幂等 API、`SKIP LOCKED` Queue、Transactional Outbox、Tenant CAS Artifact | Schema/API/CAS 确定性测试；真实 PostgreSQL 测试已设环境开关 | OIDC/SCM Broker、S3/MinIO+KMS、10 Worker/1,000 Run、PITR 与数据库重启演练 |
| P3 | Worker Lease Epoch、Stream Version 双 Fencing、旧 Sandbox 终止、优先重连、Snapshot/CAS 回退、Base SHA 与 Workspace Root Hash 校验 | Snapshot 精确恢复、错误 Base SHA 拒绝、接管顺序和旧 Epoch 拒绝 | 多主机 Dispatcher/Worker、72 小时 Soak、Egress Proxy 与外部 Beta 安全攻击集 |
| P4 | Provider/Region Policy、合规 Fallback、熔断、Retry Budget、Run/User/Team 分层预算、最坏成本预留、Price Book 结算、联合 Release Gate | 路由、预算拒绝、未知结果结算和 Fail-closed Gate 测试 | 实际 Provider RPM/TPM、429/5xx 演练、200+ Task、多语言/容量/72 小时评测 |
| P5 | 区域 Generation Fencing、签名备份清单、供应链证据、审计哈希链、删除 Tombstone、GA Gate、故障切换与 Kill Switch Runbook | 篡改检测、过期 Generation 拒绝、证据不全拒绝、GA 不满足时拒绝 | 多可用区、Firecracker/Kata、KMS/Cosign/WORM、独立渗透测试、28 天 SLO 和完整灾备演练 |

阶段详情分别记录在：

- `docs/production-p1-review.md`
- `docs/production-p2-review.md`
- `docs/production-p3-review.md`
- `docs/production-p4-review.md`
- `docs/production-p5-review.md`

## 3. 关键设计复审

### 3.1 执行边界

生产模式不再能够静默退回宿主执行。CLI 在创建 Run 前校验 Provider；只有
`gvisor` 与 `sha256` 镜像摘要组合才能进入生产路径。运行时再次 Attest 实际
Runtime、镜像、RootFS、网络、权限、Capabilities、安全选项和用户身份，任一
不匹配都会删除容器并拒绝执行。

仍然不宣称任意 Shell Exactly-once。文件副作用可用 SHA-256 前后回执对账；
无法判断的进程崩溃保持 `UNCERTAIN`，不能盲目重放。

### 3.2 分布式一致性

PostgreSQL 被定义为唯一状态真相，Queue 仅负责唤醒。Worker 写入同时受 Tenant、
Lease Owner、Lease Epoch 与 Event Stream Version 约束；旧 Worker 即使仍存活也
不能继续提交。Run、初始事件、命令、Outbox 与幂等响应能够在同一事务形成。

### 3.3 恢复与数据完整性

Workspace Snapshot 使用确定性 Tree Manifest、Tenant CAS、Base SHA、路径/符号
链接校验和 Root Hash。接管先尝试重连同一 Sandbox；失败后从可信 Base SHA 与
Snapshot 恢复，并重新计算 Root Hash，而不是相信未经验证的目录。

### 3.4 模型与成本治理

Model Gateway 在物理请求前检查 Provider/Region/Data Policy，按最坏情况预留
预算，并依据带版本的 Price Book 结算。Provider 结果不确定时按预留上界收费，
避免“可能已计费但本地退款”。发布门禁同时看质量、恢复、安全、副作用、重试、
成本、延迟和基础设施有效性，不能只凭单一成功率上线。

### 3.5 GA 与灾备

区域提升使用持久化 Generation Fencing，陈旧控制面不能完成晋升。备份、审计
与供应链证据均可签名/链式校验。GA Gate 要求连续 SLO、恢复演练、审计、供应链、
孤儿资源和成本归属全部满足；这保证“缺证据即不发布”。

## 4. 提交链与可审计边界

| 提交 | 内容 |
|---|---|
| `dbd4ff6` | P1：Fail-closed gVisor 执行与策略边界 |
| `68f9d8e` | P2：PostgreSQL 控制面与 Durable Queue |
| `2e253af` | P3：多 Worker 接管与 Workspace 恢复 |
| `fd4de53` | P4：模型网关、FinOps 与发布门禁 |
| `2dd36dc` | P5：GA 就绪、审计与灾备控制 |
| `048bebd` | 终审修复：将生产 Sandbox 接入 CLI，禁止宿主降级 |

所有内容位于独立分支，保留了 `v0.3.0` 作为 P0 对照基线，可直接审计增量。

## 5. 最终验证结果

- `uv run pytest -q`：209 passed，4 skipped。
- `uv run ruff check .`：通过。
- `git diff --check`：通过，仅有 Windows 行尾提示。
- 4 个 skipped 均为显式环境条件，不计作通过：真实 PostgreSQL、可选外部环境等
  只有配置后才运行。

## 6. 发布判定

### 可以对外陈述

- 已实现生产导向的 Durable Coding Agent Harness 核心与 P1-P5 控制面参考实现。
- 已实现 gVisor Fail-closed 接口、PostgreSQL/RLS/Queue/Outbox、Worker Fencing、
  Snapshot 恢复、模型成本治理、发布与 GA 就绪门禁。
- 本地确定性测试与已有故障评测数据可复现。

### 暂时不能陈述

- 已达到生产 GA、生产 SLA 或 28 天可用性。
- 已完成真实多租户公网部署、10 Worker/1,000 Run 或 72 小时多机 Soak。
- 已通过 gVisor/Firecracker 逃逸测试、独立渗透测试或多区域灾备。
- 任意 Shell Exactly-once，或 Worktree 等价于安全 Sandbox。

## 7. 后续唯一正确顺序

1. 在无凭证的 Linux Runner 上安装并验证 `runsc`，执行 P1 Canary/Attestation。
2. 配置真实 PostgreSQL 与对象存储，运行 RLS、并发、重启、PITR 和 Artifact 演练。
3. 部署至少两个 Worker，完成 Kill/Takeover、清空本地盘和 72 小时 Soak。
4. 运行冻结的 200+ Task 与 429/5xx/成本/安全评测，由 Release Gate 决定灰度。
5. 完成多可用区、KMS/WORM、供应链签名、渗透测试和连续 28 天 SLO 后，再由
   `GaReadinessGate` 给出 GA 结论。

在上述证据完成前，外部不可信多租户入口应保持关闭。这不是实现失败，而是生产
工程最重要的边界：代码负责收集和验证证据，部署事实必须由真实环境产生。
