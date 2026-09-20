# API Rate Limit Degradation Runbook

本手册定义 Redis API 限流进入异常或产生疑似误拒绝时的观察、降级、回滚和恢复步骤。
它是发布前的操作契约，不代表真实 Redis 压测、failover、Cluster、flush 或网络分区演练已经
执行。每次演练或事故必须另存时间、环境、版本、负责人、指标和结论证据。

## 1. 不变量

- PostgreSQL 继续权威保存 Run、事件、预算、审批、取消和 API 幂等事实。
- 限流故障不得跳过认证、角色授权、tenant/RLS、审批 fingerprint、预算或幂等校验。
- `429` 只表示健康限流器作出的配额拒绝；限流器不可用使用 route matrix 的 `503` 或
  fail-open，不能伪装成 `429`。
- 不把限流 bucket 迁入 PostgreSQL，不清空整个 Redis，不修改业务事实来恢复限流。
- `run_create`、`force_sql` 和 `stream_connect` 在 ENFORCE 下遇到限流器错误时 fail-closed；
  ordinary read fail-open。未来 `run_cancel` 使用独立保留额度并在限流器错误时 fail-open。

## 2. 触发条件

出现下列任一情况时开始本流程：

- rate-limit Redis error/timeout/protocol error 持续上升；
- decision P95 达到或超过 10 ms，或 API 线程池/连接池出现等待；
- `429` 比率、SHADOW `would_reject` 与基线明显背离；
- `run_create`、`force_sql` 或 SSE 握手出现限流相关 `503`；
- Redis 报告 `CROSSSLOT`、`NOSCRIPT`、OOM、eviction、failover 或 keyspace 异常；
- 单一 route class/tier/region 显示热租户或策略配置倾斜；
- 客户端报告 `Retry-After` 缺失、非整数、为零或与响应 body 不一致。

## 3. 首轮观察

1. 记录事件开始时间、环境、应用版本、配置版本、当前模式和 rollout percent；不要记录
   tenant/user identity、bearer token、Run ID、幂等键或 Redis 完整 key。
2. 按 route class、mode、canary cohort、region 和 tenant tier 查看 allow、reject、
   would-reject、Redis error、fail-open、fail-closed、decision P50/P95/P99。
3. 区分健康策略拒绝与适配器故障：健康拒绝必须是 `429` 且带一致的整数
   `Retry-After`；Redis 故障只应产生矩阵定义的 `503` 或 fail-open 指标。
4. 检查 Redis latency、连接池、CPU、memory、eviction、key TTL、failover 状态和 Lua 错误。
   `CROSSSLOT` 优先核对两个 bucket 是否共享 `{rl:<tenant-hmac>}`，不要临时拆成非原子请求。
5. 检查注入的 HMAC secret 是否可用且版本一致。secret 不得输出到日志；轮换导致 bucket
   重置时，只记录 secret version/配置版本。
6. 对比 API 的 PostgreSQL latency、连接池和 Run admission 指标，确认 fail-open 是否把
   压力转移到权威层。审批、预算、取消和幂等异常必须作为独立高优先级事故处理。

## 4. 降级与回滚

### 4.1 疑似误拒绝或策略错误

1. 将 `api_rate_limit_mode` 从 ENFORCE 切换为 SHADOW。
2. 确认新的 `429` 在配置传播窗口后停止，同时 `would_reject` 仍可用于对比。
3. 观察 PostgreSQL、SSE 建连和 Run admission 是否承受回流；若权威层接近容量上限，使用
   上游网关保护或现有 Run admission kill switch，不要恢复错误的 Redis 策略。
4. 保留 bucket 等待自然 TTL；不要为策略回滚执行 `FLUSHALL` 或扫描删除共享 keyspace。

### 4.2 Redis 过载、不可用或协议异常

1. 先关闭 enforce，恢复业务路径的 SHADOW 语义。
2. 若 shadow 调用继续放大 Redis 故障，将 `api_rate_limit_mode` 切换为 OFF。
3. 验证 ordinary read 正常 fail-open；验证 `run_create`、`force_sql` 和 `stream_connect`
   不再因 OFF 模式调用 Redis。OFF 是显式回滚，不等同于在 ENFORCE 中忽略 fail-closed。
4. 在应用限流关闭期间启用或收紧上游网关的有界保护。若无法安全承受新 Run，启用 Run
   admission kill switch，同时保留状态、事件、取消和制品读取。
5. 不改变 PostgreSQL 审批、预算、取消、idempotency 或 lease 配置来补偿 Redis 故障。

## 5. 故障定位清单

- `429` 激增但 Redis 健康：核对 route policy、tenant canary、双 bucket capacity/refill、请求
  cost 和 rollout percent；检查是否把 `force_sql=false` 错分为 `force_sql`。
- `503` 激增：核对 Redis timeout、连接池、DNS/TLS/ACL、Lua 返回 schema 和配置传播；确认
  只影响 fail-closed classes。
- `CROSSSLOT`：核对 tenant/user key 的 tenant HMAC hash tag 完全相同。
- `NOSCRIPT`：允许受控重新加载版本化 Lua；禁止退化为两个非原子客户端命令。
- retry 时间异常：核对 Lua 使用 Redis `TIME`、单位换算和向上取整；禁止使用应用时钟修补。
- key 数或内存增长：核对每 bucket 常数字段、TTL、route class 白名单；禁止接受动态 URL、
  Run ID 或客户端自报 dimension。
- 租户串桶：停止 ENFORCE，检查 HMAC domain separation 与 tenant/user canonical identity；按
  安全事件处理，不能通过延长 TTL 掩盖。

## 6. 恢复 ENFORCE

1. 在隔离环境复现根因并完成修复；单元测试/fake Redis 结果不能标记为真实故障演练。
2. 使用真实 Redis 验证双 bucket all-or-none、server time、TTL、Cluster 同 slot、flush、
   eviction、disconnect/failover、跨租户隔离以及逐 route 失败策略。
3. 在两倍预计峰值下确认 decision P95 < 10 ms，并验证 `429` body 与 `Retry-After`。
4. 生产先进入 SHADOW，确认 `would_reject`、Redis error 和 PostgreSQL 回流符合基线。
5. 只有更新 admission evidence 后，才能按 tenant 稳定 cohort 依次恢复
   1% → 5% → 25% → 100%；每一级必须有观察窗口和明确回滚负责人。
6. SSE 本阶段只验证握手限流。不得把连接并发 lease、断连释放或 TTL 回收标记为已覆盖；
   该能力实现独立 gate 和演练后才能验收。

## 7. 证据记录

每次演练或事故至少记录：

- 环境、版本、配置版本、mode、rollout percent、route class；
- 起止时间、触发条件、负责人、执行的降级步骤；
- 决策吞吐与 P50/P95/P99、allow/reject/would-reject/error/503、PostgreSQL 回流指标；
- Redis failover/flush/eviction/网络故障的注入方式及原始监控链接；
- tenant 隔离、审批/预算/幂等未绕过、取消可用性的验证结论；
- 恢复级别、观察窗口、遗留风险和下一步。

没有上述证据时，配置仍保持 OFF 或 SHADOW，不能仅凭本手册或单元测试声明 ENFORCE
准入已经完成。
