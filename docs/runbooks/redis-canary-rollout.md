# Redis Canary 发布与回滚 Runbook

## 原则

Redis 能力按 tenant 的 keyed HMAC 选择统一 cohort，所有能力共享同一 cohort version。
worker pool 或 run 只能在已经入选的 tenant 内进一步缩小范围，不能扩大爆炸半径。生产实例
只接受统一门禁签发的授权；其签名证据必须与实例的 environment、region、release SHA、配置
摘要完全匹配且未过期。

固定阶梯为 `OFF → SHADOW → 1% → 5% → 25% → 100%`。升级只能进入相邻一级；任何
级别都可以立即降级。容量报告只允许从 SHADOW 进入 1%，不能直接证明更高比例安全。
UI 状态、活跃索引、Prompt Cache、Fanout、API 限流执行、Worker wake 消费六种能力均从同一
versioned manifest 取得 tenant policy；旧配置中的浮点百分比不再单独决定生产暴露。

## 升级前检查

1. 校验证据签名、原始 artifact SHA-256、release/config/cohort 绑定和有效期。
   `SHADOW → 1%` 时 artifact 必须是门禁通过的 CapacityReport，摘要与签名 envelope 完全
   一致，且报告 environment/region/release/config/cohort 与目标实例逐项一致。
2. 确认真实 PostgreSQL 与 Redis 容量门禁通过：至少 2 倍预计峰值、1,000 queued、
   20 active Worker、steady 与 SQL fallback 吞吐至少达到目标的 95%、wake P95 不高于 100 ms、
   outbox P95 低于 2 秒、fallback 恢复低于 60 秒。
3. 确认 command loss、重复外部副作用、stale fence 接受、跨租户、审批/预算/认证绕过为 0。
4. 1% 以上升级必须携带前一级报告摘要，并验证至少 30 分钟观察、candidate/control 样本量、
   错误率与延迟回归、Redis error fallback 比率；manifest 升级前必须由明确的预期实例全集
   报告当前 generation，缺实例、额外实例或 generation 不一致均禁止升级。
5. 真实 failover、网络黑洞、TLS/ACL、OS 进程强杀、PITR 等未实际执行时，对应字段必须保持
   未通过；跳过的测试不是成功证据。

## 硬回滚

以下任一计数大于 0，立即将相关 Redis 能力降为 `OFF`；若涉及跨租户、权威数据或系统性
副作用，则扩大为关闭全部 Redis read/consume/enforce，并关闭新 Run admission，同时保留
SQL、outbox、Redis 与证据 artifact：

- duplicate external effect；
- approval、budget、authentication bypass；
- cross-tenant / cross-pool disclosure；
- stale fencing write accepted；
- prompt ciphertext tamper 或语义 mismatch 被接受；
- committed command/event loss。

能力级动作：API rate limit 从 ENFORCE 降为 SHADOW；worker wake 从 consume 降为
publish-only；UI/active/prompt read 关闭但可保留健康 shadow write；fanout 关闭并使用 SQL
gap-fill。Redis 本身过载时再关闭 publish/shadow write，绝不能把 Redis 状态反向写成 SQL
业务事实。

## 软回滚与恢复

连续两个至少 30 分钟、具备签名证据链的窗口超过策略阈值时，相关能力直接回到 `SHADOW`：
包括 Redis error、candidate 相对 control 错误率或延迟回归、fallback 比率、PostgreSQL
CPU/连接池余量、outbox P95。设计硬线仍是
fallback 恢复 `<60s`、outbox P95 `<2s`、限流判定 P95 `<10ms`。

回滚后至少冷却 30 分钟，禁止自动重新升级。确认实例均应用新的配置 generation，再按新的
release/config 生成证据并从相邻阶梯重新推进；不得复用跨环境、跨版本或过期授权。
