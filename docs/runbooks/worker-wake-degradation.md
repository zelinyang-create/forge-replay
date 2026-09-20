# Worker Wake 降级与回滚 Runbook

## 适用范围

本 Runbook 处理 Redis Streams Worker wake-up 的超时、断连、`NOGROUP`、wrong-type、
pending 堆积、trim/flush 和错误 canary。Redis 只是低延迟唤醒层；PostgreSQL
`run_commands`、run lease、stream version 和 fencing 始终是权威。

## 正常不变量

- Worker 启动先扫 PostgreSQL，只有 SQL 暂无工作才阻塞读 Stream。
- Redis block 上限不超过 SQL fallback poll interval。
- Redis 消息只含 `schema_version/outbox_id/command_id`；tenant、pool 来自启动配置。
- 每轮最多领取一条 SQL command，领取和执行由同一 Worker 的 lease guard 续租。
- hint 的 ACK 只代表一次权威 SQL Worker 周期已明确返回；command 的完成事实仍只在 SQL。
- retryable command 到期由 PostgreSQL polling 兜底，首版不依赖延迟 Stream 消息。

## 告警信号

- `worker_wake.redis_error`、`ack_error` 或 reconnect 持续增长；
- `XPENDING` 数量、oldest pending age、redelivery 或 `XAUTOCLAIM` 激增；
- hint useful ratio 快速下降，malformed/stale/wrong-pool 增长；
- `available_at -> claimed_at` 延迟超过 SQL fallback 上限；
- PostgreSQL command depth/oldest age 增长，或 claim P95 明显回归；
- 任意重复外部副作用、跨 tenant/pool claim 或 fencing rejection。

指标 label 只允许 mode、pool、shard、result、canary；禁止 tenant、run、command 等高基数或
敏感 label。

## 立即降级

1. 将 consume 模式降为 publish-only；Worker 下一轮立即只使用 PostgreSQL polling。
2. 若 Redis 持续过载，再关闭 publish relay；保留未发布 outbox，修复后可安全重放。
3. 不删除 PostgreSQL command/outbox，不把 Redis pending 反向写成业务事实，不执行
   `FLUSHALL`/全库 `SCAN` 清理。
4. 检查 PostgreSQL command depth、oldest age、claim latency 和 Worker heartbeat，确认
   fallback 在目标上限内恢复。若 SQL 本身异常，按 database failover Runbook 处理。
5. 若发现跨 pool/tenant 或重复副作用，立即关闭 consume 和 publish，并使用总 kill switch
   停止新 Run admission；保留 SQL/Redis 证据用于审计。

## 恢复

1. 修复 Redis 后，用同一 tenant/pool 绑定幂等创建 consumer group；`NOGROUP` 只能做一次
   受控重建。不要从消息推导 tenant 或 pool。
2. 先验证纯 PostgreSQL Worker 能持续 drain，再开启 publish-only 观察 outbox publish、
   Stream length、pending 和 malformed 指标。
3. 完成真实 Redis 的重复、flush/trim、断连、wrong-type、ACL、failover、`NOGROUP`、
   `XAUTOCLAIM` 与 crash-window 演练，确认零事实丢失、零越权、零可观察重复副作用。
4. 仅在独立准入证据通过后按 1% → 5% → 25% → 100% 恢复 consume；每级都验证 SQL
   fallback 容量和 `available_at -> claimed_at` 延迟，再扩大流量。

## 数据清理

Stream 可按容量使用近似 `MAXLEN`，因为提示可丢且 SQL 会补偿；不得设置会让 consumer
长期依赖的 TTL。测试环境使用随机 environment key，并只删除精确测试 key。生产环境删除
Stream 或 consumer group 前，必须先关闭 consume、验证 SQL fallback，并保留对应 outbox。
