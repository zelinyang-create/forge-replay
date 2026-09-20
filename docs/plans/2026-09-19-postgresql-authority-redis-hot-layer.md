# PostgreSQL 权威状态与 Redis 派生热层设计

日期：2026-09-19

状态：Accepted for staged implementation

适用范围：ForgeReplay 托管、多 Worker 运行路径；本地 CLI 可继续使用 SQLite 适配器

## 1. 背景与结论

ForgeReplay 当前已经具备追加式事件账本、事务型运行投影、检查点恢复、
`stream_version` 乐观并发控制和 `lease_epoch` 围栏。现有 SQLite 热路径不是每次
扫描全部事件：`runs`、`tool_calls`、`model_calls` 等表承担当前状态查询，事件和投影
在同一事务中更新。

当前主要问题不是缺少 Redis，而是运行时仍由 `SQLiteEventStore` 驱动，而
`PostgresControlPlaneStore` 是另一套较简化的控制面模型，尚未覆盖完整 Runtime
契约。若直接增加 Redis 权威状态，将形成 SQLite、PostgreSQL、Redis 三套状态源。

本设计采用以下不可变原则：

> PostgreSQL 是托管运行的唯一正确性平面；Redis 是可清空、可重建的延迟与通知平面。

明确禁止以下模式：

- 业务状态先写 Redis，再批量或定时刷入 SQL；
- 用 Redis 锁替代 SQL `lease_epoch` 和写入 fencing；
- 仅在 Redis 保存审批、取消、预算、工具派发或终态；
- 业务线程直接双写 PostgreSQL 和 Redis，并把两次成功视为一个原子提交；
- 把 Pub/Sub 消息当成 Run 状态真相。

## 2. 目标与非目标

### 2.1 目标

1. 托管模式只存在一个权威状态源。
2. 状态变更、事件、强一致投影、命令和 outbox 在一个 PostgreSQL 事务中提交。
3. Worker 的所有持久化写都受 `lease_owner + lease_epoch + stream_version` 保护。
4. Redis 整体丢失时不丢已提交事实、不重复已确认副作用、不绕过审批和预算。
5. Redis 故障时系统可以降级到 PostgreSQL 读取和 durable queue。
6. SQLite 与 PostgreSQL 适配器通过同一组 store contract/invariant tests。
7. 只有真实指标达到准入条件时才开启 Redis 读缓存或 Streams 消费。

### 2.2 非目标

- 不宣称任意外部副作用 exactly-once。
- 不用 Redis 替代 PostgreSQL 备份、PITR 或审计。
- 不在第一阶段删除 SQLite 本地运行模式。
- 不在没有负载证据时迁移全部热查询到 Redis。
- 不把大模型输出、工作区快照或制品长期保存在 Redis。

## 3. 当前架构评估

### 3.1 已有正确性基础

- SQLite 使用 WAL 和 `synchronous=FULL`。
- `events` 是不可变事实账本。
- `runs`、`tool_calls`、`tool_attempts`、`model_calls`、`approvals`、
  `budget_reservations` 是事务型 operational projections。
- `model_calls` 通过部分唯一索引保证一个 Run 最多一个未消费模型调用。
- Checkpoint 带状态版本与 SHA-256；损坏时可以回退旧 checkpoint 或全量重放。
- Runtime 使用 `ExecutionContext` 携带 `lease_epoch` 与 `stream_version`。
- PostgreSQL 参考控制面已有 run-local stream、durable command queue、outbox、
  API idempotency 和租户 RLS。

### 3.2 必须先修复的断点

1. `forge-replay` CLI 无条件创建 `SQLiteEventStore`，`--production` 不切换数据面。
2. PostgreSQL Store 没有实现完整 `RuntimeStorePort`。
3. Port 层仍引用 SQLite Store 中定义的 DTO，领域契约与适配器边界不完整。
4. PostgreSQL command claim 缺少 visibility timeout 与 abandoned claim 回收。
5. PostgreSQL lease 缺少显式 renew/release；同 owner 重复 acquire 会递增 epoch。
6. Outbox 领取与标记分属不同事务，允许重复发布但没有明确 claim 元数据。
7. 长模型/工具调用期间缺少独立 heartbeat，租约过期只能阻止旧 Worker 落库，
   不能自动撤销已派发的外部动作。
8. 大 Blob 在本地 SQLite 内可接受，托管模式应迁往对象存储，SQL 仅留内容地址和元数据。

## 4. 目标架构

```text
Client / API / Control Command
              |
              v
       PostgreSQL transaction
       - validate idempotency
       - CAS stream_version
       - validate lease fencing when worker mutation
       - append run_event
       - update strong projections
       - enqueue durable command
       - insert outbox row
              |
              | commit is the only success boundary
              v
          Outbox Relay
              |
              +--> Redis versioned run cache
              +--> Redis Pub/Sub for UI hints
              +--> Redis Stream for optional worker wake-up
              |
              v
          Worker consumer
              |
              v
       PostgreSQL acquire/renew lease
              |
              v
       model/tool external effect
              |
              v
       fenced PostgreSQL transaction
              |
              v
       Redis XACK after SQL commit
```

### 4.1 PostgreSQL correctness plane

下列数据必须同步、权威地保存在 PostgreSQL：

| 类别 | 数据 | 原因 |
|---|---|---|
| 历史 | `run_events`、审计链、tombstone | 回放、审计、追责 |
| 生命周期 | Run 状态、phase、terminal reason、`stream_version` | 状态机正确性 |
| 协调安全 | lease owner、epoch、expiry、writer epoch | 防旧 Worker 写入 |
| 工具执行 | call、attempt、effect class、receipt、uncertain | 副作用恢复 |
| 模型执行 | call、attempt、response reference、consumption | 避免重复消费 |
| 人工控制 | approval、grant、cancel command | 权限与控制不可丢 |
| 成本 | reservation、settlement、durable consumption | 防超支与计费 |
| 交付 | command、outbox、API idempotency | 至少一次和去重 |
| 恢复 | checkpoint metadata、snapshot hash | 有界恢复 |
| 制品 | object key、hash、length、tenant ownership | 完整性和租户边界 |

### 4.2 Redis latency plane

Redis 只能保存能够从 PostgreSQL 或对象存储重建的数据：

| 用途 | 数据结构 | 一致性 |
|---|---|---|
| Run 状态缓存 | HASH/JSON | eventual，带 `stream_version` |
| 最近事件窗口 | STREAM | 可丢失，SQL 回源 |
| 活跃 Run 索引 | ZSET | 可重建 |
| Worker presence | STRING/HASH + TTL | 仅观测，不作 fencing |
| UI 实时通知 | Pub/Sub | 提示型，客户端按 seq 补读 |
| Worker 唤醒 | Streams consumer group | 至少一次，SQL 命令权威 |
| API token bucket 限流 | Redis TIME + Lua + HASH | 常数空间快速拒绝；财务预算仍在 SQL |
| Provider 健康与熔断 | TTL key | 可过期、可重建 |

## 5. Redis Key 与消息设计

Key 必须带环境和 schema version；Redis Cluster 中需要同 Run 同 slot。Worker wake v1
固定单 shard，并把 tenant 与 pool 分别做用途隔离的 HMAC；consumer name 同样使用 HMAC，
Redis key、pending 列表、日志和监控不得暴露原始 tenant、pool 或 worker ID：

```text
fr:<env>:v1:{t:<tenant>:r:<run>}:projection
fr:<env>:v1:{t:<tenant>:r:<run>}:recent
fr:<env>:v1:active:<tenant>:<shard>
fr:<env>:v1:{qw:<tenant-hmac>:<pool-hmac>:00}:commands
fr:<env>:v1:worker:<worker-id-hmac>:heartbeat
fr:<env>:v1:{rl:<tenant-hmac>}:rate:<route-class>:p:<policy-hmac>:tenant
fr:<env>:v1:{rl:<tenant-hmac>}:rate:<route-class>:p:<policy-hmac>:user:<user-hmac>
fr:<env>:v1:outbox-dedup:<outbox-id>
```

限流 key 不得包含裸 `tenant_id`、`user_id`、bearer token、Run ID 或幂等键。
`tenant-hmac` 使用注入的限流 key secret 对带用途前缀的 canonical tenant identity 执行
HMAC-SHA-256 后生成；`user-hmac` 同时绑定 tenant 与 user，避免跨租户同名主体共享桶。
secret 来自 secret manager/KMS，不进入 Redis、日志、指标或异常。两个 bucket key 复用
`{rl:<tenant-hmac>}` hash tag，保证 Redis Cluster 中可以由一个 Lua script 原子判定。
`policy-hmac` 绑定 route class、window、tenant limit 与 user limit；策略升降级或窗口调整会
切换到新桶，旧桶仅等待 TTL 回收，避免用新容量误解旧余额或在关键路由制造持续 `503`。

Projection 最少字段：

```json
{
  "tenant_id": "tenant-1",
  "run_id": "run-1",
  "stream_version": 42,
  "status": "active",
  "phase": "awaiting_model",
  "last_event_seq": 42,
  "updated_at": "2026-09-19T12:00:00Z"
}
```

更新必须使用 Lua 或 Redis transaction 比较版本：仅当 incoming version 大于当前版本时覆盖。
这防止 outbox 重试或跨 Relay 乱序把旧状态覆盖到新状态。

建议 TTL：

- active projection：滑动 1 小时；
- terminal projection：24 小时，产品确有历史列表需求时可延长；
- recent events：与 projection 同 TTL，`MAXLEN ~ 128` 或 `256`；
- worker heartbeat：30 秒；
- outbox dedup：7 天，仅作优化，不代替 SQL 唯一约束；
- command stream：不设置简单 TTL；可用 `MAXLEN ~ N` 近似裁剪。提示被裁剪只增加延迟，
  固定 PostgreSQL polling 必须保证最终领取。

Command Stream 字段：

```text
schema_version
outbox_id
command_id
```

Redis message ID 不作为业务身份；稳定业务身份始终是 SQL 中的 `command_id`、
`outbox_id` 和 `event_id`。tenant 与 worker pool 只来自进程启动配置；Run、command 类型、
版本、`available_at`、业务 payload、审批和预算必须重新读取 PostgreSQL。

## 6. 写入协议与一致性

### 6.1 API/控制命令

1. 验证认证主体、租户与 idempotency key。
2. 在 PostgreSQL 事务中锁定或 CAS Run。
3. 若同 idempotency key 已提交且请求摘要相同，返回原响应。
4. 若摘要不同，返回冲突。
5. 追加事件、更新强一致投影、插入 command/outbox。
6. 提交事务后才向调用方确认成功。
7. Redis 不在请求事务的成功条件中。

### 6.2 Worker mutation

所有 Worker 写入使用：

```text
tenant_id
run_id
lease_owner
lease_epoch
lease_expires_at > database_clock
expected_stream_version
```

任何条件不匹配都必须影响 0 行并失败关闭。外部动作派发前先提交 durable intent；
外部动作完成后以同一 fencing token 提交 receipt/result。无法确认结果时进入
`UNCERTAIN`，不得盲目重试非幂等进程。

### 6.3 Transactional outbox

业务事务只写 PostgreSQL outbox。Relay：

1. 领取未发布行并写入 `claimed_by/claimed_at`；
2. `XADD` Redis Stream，并发布 cache invalidation/fanout；
3. 以 outbox ID 标记 SQL 行已发布；
4. 超过 visibility timeout 的 claim 可重新领取。

`XADD` 成功后、SQL 标记前崩溃会重复发布，这是预期的 at-least-once 语义。
消费者必须依赖 SQL 唯一键和 CAS 消除重复逻辑效果。

### 6.4 Worker 消费

本项目采用 **wake-only**，不把 Redis Stream 变成第二套任务队列：

1. Worker 启动和每轮循环都先从 PostgreSQL 按 `(tenant_id, worker_pool)` 执行一次
   `SKIP LOCKED` claim；有积压时持续 drain SQL，不先读 Redis。
2. SQL 暂无可领取命令时，才用 `XREADGROUP` 有界阻塞等待提示；阻塞时间不得超过
   `sql_fallback_poll_interval`，超时或 Redis 错误后立即回到 SQL polling。
3. Stream 提示严格只含 `schema_version/outbox_id/command_id`。租户和 worker pool 来自
   受信启动配置，提示里的任何业务状态、payload、审批、预算或版本一律不采信。
4. 收到提示后仍执行普通 PostgreSQL claim。当前实现仅在一次权威 `ManagedWorker.run_once()`
   明确返回后 `XACK`，包括返回 0 行的 stale/duplicate/过早提示；SQL claim/处理向上抛出
   异常则不 `XACK`。长任务期间提示可能留在 pending 或被重复领取，但另一个 Worker 的
   SQL claim 会返回 0 行；Worker 崩溃后的执行恢复仍由 SQL visibility timeout 和固定
   polling 保证，不能由 pending hint 直接恢复业务执行。
5. 真正领取到 command 后，获取 SQL lease；acquire/takeover 才递增 epoch，renew 不递增。
   执行前再次验证 Run 状态、审批、预算和 stream version，结果仍在 fenced SQL 事务提交。
6. `XAUTOCLAIM` 只回收 read 到 SQL 决议之间遗留的 pending 提示，不承担业务正确性。
   malformed 提示作为 poison hint 记录低基数指标并确认；重复、丢失、trim、flush、
   `NOGROUP`、断连最多增加唤醒延迟，不得造成 command 丢失或越权执行。

首版每次 SQL poll 强制 `claim_limit=1`。当前 `LeaseGuard` 只续租正在执行的一条命令；
允许批量预取会使本地等待命令的 claim 过期并被其他 Worker 接管。未来只有实现整批续租，
或在每条命令执行前增加 owner CAS 后，才可重新开放批量 claim。

`run_commands.worker_pool` 是 SQL 权威路由字段，首版默认 `default`。Redis key 使用
HMAC 后的 tenant/pool 和固定单 shard，消息不暴露原始 tenant、pool、run 或 worker ID。
初始 command 与 pool 路由的 `command-wakeup-v1:<worker_pool>` outbox 在同一 SQL 事务
创建；不同 pool 的 Relay 只领取自己的 destination，不能互相吞掉提示。retryable command 的
延迟重排队首版不提前发布提示，到期后由有界 PostgreSQL fallback poll 领取，避免将未来
任务过早 `XADD` 后确认丢弃。后续若要优化延迟，应给 outbox 增加权威 `available_at`，由
Relay 到期后发布，不能靠 Redis 消息里的时间字段决定执行。

## 7. 读取协议

### 7.1 强一致读取

审批、取消、预算、工具派发、恢复决策、终态提交等操作只读 PostgreSQL，或在同一
事务中读取并写入。Redis 命中不能跳过 SQL fencing。

### 7.2 可陈旧读取

UI 状态、活跃列表和进度展示可以 cache-aside：

1. 读取 Redis projection；
2. miss 时读 SQL 并回填；
3. 客户端携带的最低版本高于缓存版本时强制回源；
4. SSE/WebSocket 消息携带 event seq；发现 gap 后从 SQL `after_seq` 补读。

Prompt working set 可以缓存，但 miss/驱逐时必须能从 SQL 最近事件和 Blob Store 重建。

### 7.3 Prompt working set 安全缓存

该缓存只用于模型调用前的 prompt 组装加速，不得用于恢复、checkpoint、状态机、
fencing、审批、预算、工具派发、审计或 UI。缓存对象是已经按 prompt 语义筛选并水合的
用户消息与最多 12 条 transcript entry，而不是通用 `EventEnvelope` 或完整 Blob。

权威构建流程固定为：

1. 在 tenant/RLS 作用域内读取 Run 当前 `stream_version` 和最近 64 条 run-local 事件；
2. 验证事件窗口严格递增且无重复；
3. 先筛选 prompt 会使用的事件并截取最后 12 条，再读取这些事件引用的 Blob；
4. 用户消息经 Turn 的 user event 单独读取，因为它不属于 run-local 事件窗口；
5. 校验 Blob tenant ownership、SHA-256、长度、媒体类型和 UTF-8 解码；
6. 生成带 prompt contract version 的 canonical working set。

只要 Redis 保存用户消息、模型回复或工具输出正文，应用层 AEAD 就是启用读路径的硬门槛。
使用 AES-256-GCM、每次写入随机 96-bit nonce，并由注入的 tenant key provider 按租户提供
版本化 DEK；不同租户不能共享同一解密 key ring，且必须支持按租户 rotation/revoke。
密钥不得进入 Redis、日志、指标或异常。AAD 至少绑定 environment、用途、
wire schema、tenant、run、covered version、prompt contract、Redis key 和 key ID。跨 tenant、
run、version 或 key 搬运密文必须认证失败并整份回源。

Redis key 使用独立 namespace key 做 HMAC-SHA256，不嵌入可逆 tenant/run ID；同一 Run 的
key 使用同一 cluster hash tag。外层只允许 wire version、key ID、nonce、ciphertext、
covered version 和认证指纹。Lua CAS 使用 canonical decimal string 比较版本，不能转换为
Lua number：旧版本为 stale，相同版本同内容为 duplicate 且不续 TTL，相同版本不同内容为
conflict，新版本才 applied。

默认约束：绝对 TTL 15 分钟、每 Run 最多 64 个事件、12 条 transcript、canonical 明文
最多 256 KiB；超限直接绕过缓存，不能截断 prompt。首版采用 cache-aside。相同版本的
认证命中可直接使用；较旧版本只能作为候选文本，必须重新读取当前 SQL 事件窗口，以
event ID、seq、type 和 Blob SHA 逐项验证后才能复用，新增/变化条目再从 Blob Store 水合。
Redis ahead、miss、eviction、flush、超时、未知 key ID、AEAD 失败、schema 损坏、身份不匹配
或 SQL 窗口错误时，整份从 PostgreSQL + Blob Store 重建，并 best-effort 回填。PostgreSQL/
Blob 失败时不得使用旧 Redis 候选维持服务。

功能开关必须独立于 projection：

```text
redis_prompt_cache_write
redis_prompt_cache_read
redis_prompt_cache_shadow_compare
```

三者默认关闭。read 必须要求 write、PostgreSQL/Blob fallback、AEAD key provider、Redis
TLS/ACL、语义 shadow compare 和 flush/eviction/tamper/cross-tenant/KMS outage 演练全部
通过。读取按 tenant/run 稳定散列执行 canary，实际 rollout percent 不得超过已经验证的
canary percent；按 1% → 5% → 25% → 100% 推进。只记录 hit、miss、stale-assist、fallback、
read/write error、shadow match/mismatch 和 conflict-delete 等无正文聚合指标。同版本不同
canonical 内容必须删除冲突条目，不能继续命中旧值。若后续需要预热，使用独立
`prompt-working-set-v1` outbox destination；不得把可选
prompt cache 的成功加入 `run-projection-v1` 的 ACK 条件。

### 7.4 API 共享限流

API 限流的目的，是在多 API 实例之间提供一致的短时 admission window，在请求进入昂贵的
PostgreSQL 查询、Run 创建或 SSE 建连前快速拒绝过载。它不是身份认证、角色授权、DDoS
边界防护、财务预算、计费、审批、取消、幂等或审计机制。边缘网关仍负责未认证来源/IP
防护；PostgreSQL 中的预算、审批、控制命令、API idempotency key 与 Run 状态仍是权威事实。

首版固定使用 Redis `TIME` 驱动的 Lua token bucket。每个 route class 同时检查 tenant bucket
与 tenant/user bucket；每个 bucket 只保存 `tokens` 与 `last_refill_ms` 等常数字段，并设置
覆盖完整补充周期的 TTL。Lua 先计算两个 bucket 的补充结果，只有二者都允许时才同时扣减；
任一拒绝都不得只扣其中一个。两个 key 必须使用同一 tenant HMAC hash tag，保证 Redis
Cluster 单 slot 内 all-or-none。禁止使用应用实例时钟、按请求写一条 member 的无界 ZSET、
原始 URL、Run ID 或幂等键作为 bucket/指标维度。

route class 是协议常量，不能从 path 动态生成：

| HTTP 路径/条件 | route class | Redis 判定不可用时 | 备注 |
|---|---|---|---|
| `POST /v1/runs` | `run_create` | fail-closed，返回 `503` | 先认证；SQL 幂等仍处理安全重试 |
| `GET /v1/runs`，`force_sql=false` | `active_list` | fail-open | Redis 索引失败仍由既有 SQL fallback 决定 |
| `GET /v1/runs/{run_id}/status`，`force_sql=false` | `ui_status` | fail-open | 不改变状态缓存的一致性校验 |
| 上述两个端点，`force_sql=true` | `force_sql` | fail-closed，返回 `503` | 防止客户端绕过缓存持续打 SQL |
| `GET /v1/runs/{run_id}` | `run_read` | fail-open | PostgreSQL 读取仍做 tenant/RLS 隔离 |
| `GET /v1/runs/{run_id}/events` | `event_read` | fail-open | 现有 SQL page/window 上限保持不变 |
| `GET /v1/runs/{run_id}/stream` | `stream_connect` | fail-closed，返回 `503` | 只在发送 SSE headers 前限制握手 |
| 未来 cancel 控制端点 | `run_cancel` | fail-open | 独立高容量保留额度，不与普通流量共桶 |

本阶段 SSE 只限制连接握手；heartbeat 和每条事件不再次扣 token。并发连接 lease、断连释放、
TTL 回收和每租户连接上限属于后续独立能力，必须另设 feature gate 与故障演练，不能宣称已由
握手限流覆盖。未来新增 approval 或其他控制端点时，必须在此矩阵中显式分配独立 route
class 和失败策略，不能默认继承 `run_create` 或普通读取策略。无论限流结果如何，审批
fingerprint/expected version、取消幂等、预算 reserve/settle 与 API idempotency 均继续由 SQL
事务验证；Redis 决策不得写入或推进业务状态。

健康 Redis 在 ENFORCE 模式拒绝请求时返回：

```http
HTTP/1.1 429 Too Many Requests
Retry-After: <向上取整且至少为 1 的秒数>
Cache-Control: no-store
Content-Type: application/json

{"code":"rate_limit_exceeded","detail":"rate limit exceeded","limit_class":"<route-class>","retry_after_seconds":<seconds>}
```

`Retry-After` 由两个 bucket 中较长的等待时间计算，响应不得泄露 tenant/user、Redis key、
token 数或内部 policy。Redis timeout、连接失败、`CROSSSLOT`、Lua/protocol 异常不是配额
拒绝，不能伪装成 `429`：矩阵中的 fail-closed route 返回通用 `503`，fail-open route 继续
现有权威路径并记录降级指标。SHADOW 模式下所有 route 都只观察、不拒绝。

功能开关独立于 projection、prompt cache、fanout 和 queue：

```text
api_rate_limit_mode = off | shadow | enforce
api_rate_limit_rollout_percent
api_rate_limit_admission_evidence
```

模式默认 `off`，且只接受 OFF、SHADOW 和 ENFORCE 三种状态。ENFORCE 的准入证据必须
覆盖先前 SHADOW 观察与故障演练。SHADOW 执行同一 Redis 判定并记录
`would_allow`/`would_reject`，但不改变 HTTP 结果。ENFORCE 按 tenant identity 做稳定
HMAC-SHA-256 canary；不得按 run、request、user 或 idempotency key 分桶，避免调用方枚举绕过。
未进入 canary 的 tenant 继续 SHADOW。实际 rollout percent 不得超过 admission evidence 已验证
的 canary percent，并按 1% → 5% → 25% → 100% 推进。

进入 ENFORCE 前必须有真实证据证明：确有多实例统一窗口需求或持续约 500 次判定/秒；
两倍预计峰值下 Redis 判定 P95 < 10 ms；双 bucket 原子边界、Redis server time、TTL、
hot tenant、公平性、Cluster 同 slot、flush/eviction、disconnect/failover、协议损坏、跨租户
隔离、`429`/`Retry-After` 及本阶段各 route 失败策略演练全部通过。未来 cancel 保留额度在
该端点实施时进入其独立准入门禁。单元测试或
fake Redis 只能验证契约，不能替代准入压测和故障注入。操作步骤见
[`API Rate Limit Degradation Runbook`](../runbooks/api-rate-limit-degradation.md)。

## 8. 故障语义

| 故障窗口 | 正确行为 |
|---|---|
| SQL commit 前崩溃 | 没有已提交事实；安全重试命令 |
| SQL commit 后、Relay 发布前 | outbox 保留；恢复后补发 |
| Redis 发布后、标记 outbox 前 | 可能重复消息；按稳定 ID 幂等 |
| Worker 收到消息后、执行前 | pending message 可接管；SQL 判定是否仍需执行 |
| 外部副作用后、SQL receipt 前 | 对账；不能证明时标记 `UNCERTAIN` |
| SQL commit 后、XACK 前 | 消息重投；SQL 显示已完成，消费者 no-op 后确认 |
| Redis 全部丢失 | 从 SQL 重建；0 已提交事实丢失 |
| Redis 网络分区 | 读回 SQL；Worker 使用 PG queue fallback |
| PostgreSQL 不可用 | 停止状态推进和新的外部副作用 |
| Worker 长暂停导致 lease 过期 | 新 Worker 可接管；旧 Worker SQL 写被 epoch 拒绝 |

独立 heartbeat 必须在长模型/工具调用期间运行。Heartbeat 失败或剩余 TTL 低于安全窗口时，
执行器应尝试取消外部工作；无法确认的结果按 `UNCERTAIN` 处理。

## 9. PostgreSQL Schema 演进

第一阶段至少补齐：

- `run_commands.claimed_by`、`claimed_at`、`claim_expires_at`、`last_error_json`；
- ready partial index：`(tenant_id, available_at)` where status = `queued`；
- `run_outbox.claimed_by`、`claimed_at`、`claim_expires_at`、`last_error_json`；
- pending partial index；
- 显式 `renew_worker_lease` 与 `release_worker_lease`；
- command/outbox reclaim API；
- worker registry heartbeat/upsert 与 draining；
- 完整 Runtime projection tables；
- 所有 Worker event 记录 `writer_lease_epoch`；
- 正式版本化 migration，替换单个大字符串作为长期迁移机制。

托管模式中的大字节放入租户隔离对象存储：对象 key 不直接使用用户输入，SQL 保存
SHA-256、长度、media type、tenant 和引用关系。

## 10. 分阶段实施

### Phase 0：基线与契约

- 把 Store DTO 移到独立 domain/contracts 模块；Port 不再反向引用 SQLite adapter。
- 固化 SQLite/PostgreSQL 共用 invariant test suite。
- 记录 SQL transaction latency、lock wait、query latency、event rate、活跃 Run 数。

### Phase 1：统一 PostgreSQL 权威路径

- 实现完整 `PostgresRuntimeStore`。
- CLI/服务通过配置选择 local SQLite 或 managed PostgreSQL。
- 补齐 command reclaim、outbox claim、lease renew/release、heartbeat。
- 将对象正文移出 PostgreSQL/SQLite 托管路径。
- 在没有 Redis 时完成多 Worker 正确性与恢复验证。

### Phase 2：Redis Shadow Projection

- 实现 outbox relay。
- 写 Redis versioned projection，但读取仍使用 SQL。
- 持续对比 Redis 与 SQL 的 version/status，记录 mismatch。
- `FLUSHALL`、乱序、重复发布和 Relay crash 测试必须通过。

### Phase 3：逐项启用 Redis Reads

按风险从低到高：

1. UI Run 状态缓存；
2. SSE/WebSocket fanout；
3. active-run 索引；
4. recent prompt/event cache；
5. 共享 rate limit/circuit breaker；
6. Redis Streams worker wake-up。

第 6 项交付为默认关闭的 wake-only 基础设施：独立 transactional outbox relay、
tenant/pool 隔离 Stream、consumer group、`XAUTOCLAIM`、有界 SQL fallback loop 和独立
准入证据。它不会解除准入门槛；未提供真实 Redis 故障演练证据时只允许 `OFF` 或
publish-only 预热，不允许消费路径进入生产流量。

每项使用独立 feature flag，支持立即回退 PostgreSQL。
共享 API rate limit 与 provider circuit breaker 必须是两个独立能力、开关和状态空间；
本阶段只定义 API rate limit，不能用它推断 provider 健康。

### Phase 4：容量与生产门禁

Phase 4 分三段交付，不能把单元测试里的布尔值直接当成生产证据：

1. **Phase 4.1 — 证据信任链与统一门禁**：统一 tenant HMAC cohort，固定
   `OFF → SHADOW → 1% → 5% → 25% → 100%` 阶梯；升级只能相邻，降级可立即执行。
   证据必须绑定 environment、region、release SHA、配置摘要、cohort version、原始报告
   SHA-256、观察窗口、过期时间和签名 key ID。容量与通用 release/GA gate 对 NaN、Inf、
   负数、bool 冒充整数全部失败关闭。
2. **Phase 4.2 — 真实容量与故障产物**：在隔离的 PostgreSQL schema 和 Redis keyspace
   执行两倍峰值、1,000 queued / 20 active、SQL-only、Redis wake、Redis disconnect fallback、
   crash-window、failover、TLS/ACL、备份恢复、PITR 和 Redis 全量重建。报告必须记录实际
   触发点；`pytest skip`、fake client 和手填 `True` 不得生成生产授权。
3. **Phase 4.3 — 逐级 Canary**：容量报告只允许开始 1%；5%/25%/100% 各自需要同一
   release/config/cohort 的前一级生产观察报告、足够样本与前一报告摘要。任意安全违规立即
   回滚；软 SLO 连续超窗后能力级回滚并进入冷却期。

Phase 4.3 由 `production.canary_lifecycle` 承担有状态编排。`SHADOW → 1%` 必须提交不可
拆分的 `Phase42AdmissionBundle`（capacity/fault report、两份独立 runner attestation、release
receipt 和 envelope）；只传裸 envelope 或容量报告一律拒绝。所有 serving 升级还必须携带
签名的实例全集收敛快照，绑定权威 inventory revision、当前 manifest 摘要/generation、精确实例集
和短时有效期；inventory revision 与 `state_revision` 必须在同一事务中 CAS。控制面保存连续窗口、
最后健康证据摘要、serving 过期时间、最后 admission 摘要和冷却截止时间；`must_persist=true` 的
拒绝结果也必须落库。运行时通过 `LifecycleTenantPolicy` 对过期、缺失或 context 不一致的状态失败
关闭。健康窗口必须摘要相链；replay、乱序、重叠或缺口直接回到 `SHADOW`。硬安全违规或
committed fact loss 单窗回到 `OFF`，软违规连续两窗回到 `SHADOW`。5%/25%/100% 升级强制
验证 coverage-complete 的安全快照；所有回滚/显式降级至少冷却 30 分钟，且禁止复用回滚前的
Phase 4.2 admission package。

Redis failover、flush、eviction、网络分区，以及 PostgreSQL failover、stale epoch、重复
command、乱序 outbox 均为真实环境门禁。Phase 4.2 产物未生成前最多允许 SHADOW、缓存
写预热或 wake publish-only，不得签发 1% 生产读、ENFORCE 或 consume 授权。

Phase 4.2 的实现入口为 `forge_replay.eval.hot_layer_capacity`、`production.fault_drill` 和
`production.evidence_signing`，操作边界见 `docs/runbooks/redis-evidence-signing.md`。仓库内的
Toxiproxy 演练只覆盖可在单节点安全复现的故障子集；缺少集群 failover、备份恢复、PITR、
TLS/ACL 轮换或完整 kill-window 时，`FaultDrillReport.qualifies` 必须失败关闭。容量 runner
即使完成真实服务对账，只要吞吐或任一 P95 硬线失败，也只能输出诊断 artifact，不能签发。

## 11. Redis 准入门槛

下列数值是本项目的初始工程门槛，不是通用行业标准，需按生产基线调整：

- 两台以上 Worker 或跨机恢复：必须先迁 PostgreSQL，不能用 Redis 给 SQLite 续命。
- 读缓存：两倍预计峰值压测下目标 SQL 查询 P95 > 20 ms，或数据库 CPU 持续 > 65%，
  且热点读写比 >= 10:1、预计命中率 >= 80%。上线后 SQL 读负载至少下降 30%。
- Redis Streams：优化 PG 索引后 command claim P95 仍 > 25 ms、唤醒延迟 P95 > 100 ms，
  或持续约 1,000 claim/s 以上。
- 共享限流：确有多 API 实例统一窗口需求，或持续约 500 次判断/s；两倍预计峰值下
  Redis 双 bucket 判定 P95 必须 < 10 ms，并通过 7.4 节全部故障与安全演练。
- Redis 上线故障门禁：清空 Redis 后 0 已提交事件丢失、0 权限/预算绕过、
  0 可观察重复副作用；SQL fallback 恢复 < 60 秒；outbox 投影延迟 P95 < 2 秒。

没有达到门槛时，优先使用 PostgreSQL 索引、连接池、合并领域查询、`LISTEN/NOTIFY`
或进程内 bounded LRU。

## 12. 观测指标

PostgreSQL：

- transaction/query P50/P95/P99；
- lock wait、deadlock、connection pool saturation；
- WAL bytes/s、replication lag；
- command depth、oldest age、claim/redelivery；
- outbox pending、oldest age、publish retries；
- lease conflict、takeover、stale write rejection。

Redis：

- cache hit ratio、version lag、stale-update rejection；
- timeout/error/fallback QPS；
- memory、eviction、fragmentation；
- stream lag、`XPENDING`、oldest pending、redelivery、claim age；
- Pub/Sub subscriber/fanout failures；
- full rebuild duration。
- rate-limit decision latency、allow/reject/would-reject、Redis error、route-class failure
  action、canary cohort 和 mode；只按 route class/tier/region 聚合，不带 tenant/user/run label。

业务与安全：

- duplicate logical command；
- duplicate external side effect；
- approval/budget bypass；
- `UNCERTAIN` 比率；
- recovery RTO/RPO；
- 每 Run 模型/工具/存储延迟占比。

指标按 tenant tier、worker pool、region 和 release 聚合，避免以 run ID 造成高基数。

## 13. 测试与验收

### 13.1 Store contract

SQLite 与 PostgreSQL 必须共同通过：

- 事件追加和 stream version CAS；
- lease acquire/renew/release/takeover；
- stale epoch 100% 拒绝；
- tool/model call 唯一性；
- approval/cancel idempotency；
- budget reserve/settle；
- checkpoint-tail 恢复和损坏回退；
- terminal transition 原子性。

### 13.2 PostgreSQL/Redis 集成

- SQL commit/rollback 与 outbox 原子性；
- visibility timeout/reclaim；
- 重复、乱序 outbox；
- `XREADGROUP`、`XACK`、`XAUTOCLAIM`；
- command 创建与 wake outbox 必须同事务 commit/rollback；消息严格无 tenant/run/payload；
- consumer 启动先 SQL drain，timeout、Redis error、flush/trim、`NOGROUP` 后均在固定上限
  内恢复 SQL polling；SQL claim 异常不确认提示；
- crash 窗口覆盖 read 前、read 后 SQL poll 前、SQL command claim commit 后执行前，以及
  command 完成后 Stream `XACK` 前；当前实现不存在“ACK 后执行前”的顺序；
  验收时 SQL visibility timeout 必须能恢复已领取命令，且提示重复不产生重复逻辑效果；
- worker pool 隔离同时由 SQL 谓词和 Redis key 验证，伪造提示不能改变 tenant/pool；
- versioned cache CAS；
- Redis miss、timeout、flush、eviction、network partition；
- PostgreSQL fallback。
- API 限流双 bucket 原子扣减、TTL、server time、同 slot、tenant canary、`429`/`Retry-After`
  与逐 route fail-open/fail-closed；测试报告必须区分 fake/单元测试和真实 Redis 演练。

### 13.3 Kill-window matrix

每个故障窗口至少覆盖：SQL commit 前、commit 后 publish 前、publish 后 mark 前、
副作用后 receipt 前、SQL result commit 后 XACK 前。验收条件是无已提交事实丢失，
且重复执行要么被幂等消除，要么进入明确的 `UNCERTAIN` 状态。

### 13.4 容量验证

- 多 Worker 并发 claim；
- 热租户和 shard 倾斜；
- 1,000 queued / 20 active 基线；
- Redis 禁用时 PostgreSQL fallback 容量；
- Redis 重建期间的延迟和数据库冲击。

容量报告中的 outbox lag 从 SQL outbox `created_at` 量到 owner-fenced publish mark；Redis wake
latency 采用保守的端到端口径，从 Redis publish 调用开始量到 `XREADGROUP` 返回，包含客户端、
网络与 `XADD` 耗时。一个 pipeline 批次内的 hint 共用该批次调用前的单调时钟起点；即使消费者
早于生产者收到 pipeline 响应，也不得把延迟截断为 0 或改用响应后的时间。两段分别保留，且
不能用 wake 指标掩盖 outbox backlog。runner 必须为全部 queued command 各保留一条 claim 与
wake 样本，缺样本或时钟顺序异常直接判环境/演练失败。`20 active` 表示所有 Worker 已完成
Redis 连接、consumer group 和首次 pending scan 预热；publish 必须等全部 Worker ready 后才可
开始，避免把进程冷启动混入 wake SLO，同时不得在预热阶段发布或消费测试 hint。relay 可一次
从 SQL owner-fenced claim 100 条，但 Redis pipeline 默认最多 25 条；每个子批次独立使用调用前
起点并保留逐项结果，SQL 最后仍只批量 mark 明确发布成功的 outbox ID。该上限用于压低 1,000
条突发下的 wake 尾延迟，不改变 outbox 所有权、at-least-once 语义或 100 ms 硬门槛。
Worker 只可在对应 SQL 决定持久化后批量 `XACK` 本次明确处理的消息；批量 ACK 仅合并 Redis
往返，不得提前确认、扩大 ID 集合或代替 PostgreSQL command owner/lease fencing。

## 14. 发布、回滚与完成定义

独立开关：

```text
redis_cache_write
redis_cache_read
redis_fanout
api_rate_limit_mode
api_rate_limit_rollout_percent
redis_queue_publish
redis_queue_consume
postgres_queue_fallback
```

回滚顺序：关闭 Redis read/consume，恢复 PostgreSQL read/claim；停止新 Redis 投影；
保留 outbox 等待修复后重放。Redis 数据从不需要反向迁回 SQL。
API 限流误拒绝时先从 ENFORCE 降到 SHADOW；Redis 本身过载或不可用时关闭 shadow，
并按 runbook 使用上游保护或 Run admission kill switch，而不是把限流计数迁入 PostgreSQL。

本设计完成的定义：

1. 托管 Runtime 不再依赖 SQLite 具体类型或存储路径。
2. PostgreSQL 是唯一权威状态源，并通过共用 contract tests。
3. Worker 写入全部受 lease epoch 与 stream version fencing。
4. Command/outbox 支持 claim、ack、visibility timeout 和 reclaim。
5. Redis 可以被完全清空而不影响正确性。
6. Redis 每项能力都有指标、开关、故障演练和 SQL fallback。
7. 文档中的故障矩阵和容量门禁均有可复现测试证据。
