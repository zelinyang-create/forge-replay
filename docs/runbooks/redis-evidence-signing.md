# Redis Phase 4.2 证据生成与签名 Runbook

## 1. 信任边界

生产证据分为三个独立步骤，禁止在同一普通 CI Job 中合并：

1. **Live runner** 使用真实 PostgreSQL 与 Redis，在隔离 schema/keyspace 中执行容量或故障
   演练，输出 canonical report、去凭据原始结果和 runner attestation。
2. **Evidence signer** 只读取不可变 artifact，验证两类 runner attestation、release/config/context、
   时间与摘要后，生成 `SignedEvidenceEnvelope` 和 `EvidenceSigningReceipt`。
3. **Release gate** 仅消费 envelope、canonical report 和当前 deployment context，不能运行压测，
   也不能自行补写或修改测量值。

容量和故障 runner 使用独立 runner key；release signer 使用另一把 key。runner key 不得出现在
应用容器、普通单元测试或 release gate 中。release key 不得出现在 live runner 中。

当前 `SignedEvidenceEnvelope` 为 HMAC，是项目现阶段的内部信任边界：验证端持有共享 secret，
因此不提供强隔离的不可抵赖性。生产化应迁移为 KMS/HSM 托管的 Ed25519 或 ECDSA 签名，
运行时只分发 public verification key。在迁移完成前，release secret 只能注入隔离 signer Job，
并应把 `EvidenceSigningReceipt` 存入 append-only artifact store 供审计；不能把 HMAC 当作第三方
可验证签名。

## 2. Canonical artifact 要求

每个 live runner 必须保留三份相互绑定的内容：

- canonical report：稳定字段顺序、UTF-8、拒绝 NaN/Inf，包含完整 `ReleaseContext`；
- raw results：不含 DSN、密码、tenant/run/user 标识，canonical 后计算 SHA-256；
- runner attestation：绑定 `report_sha256`、`raw_results_sha256`、隔离资源摘要、runner build
  摘要、唯一 execution ID、起止时间、真实服务类型和 runner key ID。

Capacity runner 的主 artifact 仍是 `CapacityReport.sha256`，这样 SHADOW→1% 门禁可以逐字节
匹配 `SignedEvidenceEnvelope.artifact_sha256`。Fault runner artifact 由 signing receipt 绑定；
signer 缺少任意一种通过的 attestation 时不得签发 envelope。

以下情况只允许输出诊断报告，不得生成 `PASSED` attestation：

- 使用 fake/mock/in-memory client，或服务类型不是同时包含 PostgreSQL 与 Redis；
- 无法完成真实握手、隔离 schema/keyspace、清理或最终对账；
- 任一 required scenario 未触发、被 skip、超时、提前退出或仅手填布尔值；
- command/event 丢失、重复外部副作用、stale fence 接受或跨 tenant/pool 泄漏不为零；
- fallback、outbox、wake、吞吐或样本门槛未通过。

生产 fault report 必须包含 `FaultScenario` 定义的完整矩阵，不能只跑单节点可注入的子集。除
disconnect、stream trim、NOGROUP、worker terminate、outbox 重复/乱序和 stale fencing 外，
还必须在受控 staging/production-like 集群实际执行 Redis flush/eviction/failover/full rebuild、
PostgreSQL failover/backup restore/PITR、TLS/ACL 轮换、跨 tenant/pool 隔离和完整 kill-window
矩阵。`tests/test_live_fault_drill.py` 是本地单节点子集演练；即使全部断言通过，生成的 report
也必须保持 `qualifies=false`，不能进入签名步骤。

基础设施故障应使 runner Job 失败。不得把 “环境不可用” 解释成演练通过，也不得沿用上次
成功产物。

## 3. 独立签名步骤

签名前逐项验证：

1. capacity 与 fault attestation 的 key ID 均在 signer 的 allowlist，且签名有效；
2. 两者均为 `live_external_services`、`PASSED`，并声明 PostgreSQL 与 Redis 探针；
3. environment、region、release SHA、config SHA-256、cohort version 与待发布实例完全一致；
4. report/raw/isolation/runner-build 摘要格式正确，capacity report 摘要与请求逐字一致；
5. artifact 未过期、未来自未来，且执行结束时间不晚于被签观察窗口；
6. observation 为 SHADOW，子指标重新校验，NaN/Inf/负数/类型混淆全部失败关闭；
7. runner key 与 release signing key 不同。

签名结果必须整体保存：envelope、capacity attestation、fault attestation 和 receipt。只复制
envelope 会丢失审计链；只复制 receipt 不能获得发布授权。

## 4. CI/CD 分工与命令

普通 PR CI 不配置 runner/release key，也不访问生产服务，只运行模型、canonicalization 和
防篡改单测：

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_evidence_signing.py tests/test_capacity_gate.py tests/test_canary_release.py tests/test_hot_layer_capacity.py tests/test_fault_drill.py -q
.\.venv\Scripts\ruff.exe check src/forge_replay/production/evidence_signing.py tests/test_evidence_signing.py
.\.venv\Scripts\pyright.exe src/forge_replay/production/evidence_signing.py tests/test_evidence_signing.py
```

PR CI 或开发机可以生成 `local_isolated`/unsigned artifact 供调试，但受保护签名 Job 的环境
allowlist 与部署上下文必须拒绝这类 artifact。模型层仍会校验 exact context，不能代替 CI
主体/环境授权。禁止在仓库、CI variables、日志、artifact 或命令行参数中存放 runner/release
key。

受保护 live-capacity Job 的 unsigned runner 命令为：

```powershell
.\.venv\Scripts\python.exe -m forge_replay.eval.hot_layer_capacity `
  --expected-peak-claims-per-second 100 `
  --environment production --region us-east-1 `
  --release-sha $env:CI_COMMIT_SHA --config-sha256 $env:RELEASE_CONFIG_SHA256 `
  --cohort-version redis-canary-v1 --output capacity-artifact.json
```

该命令要求 Job 通过临时 secret injection 提供真实 PostgreSQL/Redis 连接信息；命令本身只
生成 unsigned live artifact，不拥有 runner/release key。随后由受保护的 runner-attestation
步骤签名其 report/raw/isolation/build 摘要。Fault drill 同样先生成 qualifying canonical
`FaultDrillReport`，未完整执行 destructive external matrix 时不得进入 attestation 步骤。当前
仓库只提供可在单机复现的 destructive 子集；Redis/PostgreSQL 集群故障、备份恢复、PITR 与
证书/ACL 轮换必须由部署环境的专用 runner 补齐后，报告才会 `qualifies=true`。

生产流水线必须拆成受保护环境中的两个手动审批 Job：

- `phase4-live-evidence`：短时获取 runner key 和专用测试数据库/Redis 凭据；输出只读 artifact；
- `phase4-sign-evidence`：无数据库/Redis 网络权限，只能读取前一 Job 的 immutable artifact，
  短时获取 release signer key，签名后立即销毁工作目录与凭据。

两 Job 之间以 artifact store 的 SHA-256/对象版本传递，不能通过可编辑 workspace 传递。
发布 Job 重新验证 retained package，再把 envelope 交给统一 gate。任何验证错误均保持
SHADOW，不允许人工改 JSON 后重试签名；必须重新运行 live runner。

## 5. 密钥轮换与撤销

- runner key ID 和 release key ID 必须版本化；新旧 key 的重叠期不超过一次发布窗口；
- key 泄漏时撤销对应 allowlist、将相关 Redis 能力回滚到 OFF/SHADOW，并废弃该 key 签发的
  未过期 envelope；
- 轮换 cohort version、release SHA 或 config SHA-256 后，旧 artifact 即使未过期也不能复用；
- artifact store 至少保留 report、raw digest、两份 attestation、receipt、审批人和流水线 run ID。

## 6. 当前限制

`evidence_signing.py` 负责纯模型和签名边界，不负责网络、凭据获取或 artifact store I/O。
Capacity/Fault runner 必须在各自模块中保证真实握手、故障触发和清理。HMAC→非对称签名、
KMS key policy、append-only 存储和 CI 平台的 protected-environment 配置是进入公开生产前的
硬化项。
