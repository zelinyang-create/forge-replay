# ForgeReplay Runtime 热路径事件扫描优化技术方案

> 状态：Proposed  
> 日期：2026-08-21  
> 目标读者：后续负责实现与评审的 Codex / ForgeReplay 维护者

## 1. 背景

ForgeReplay 已经具备 append-only 事件账本、RunProjection、Checkpoint + tail replay、稳定工具身份、工具状态表和有界 Prompt 工作集。当前 `get_run_projection()` 可以从最新有效 Checkpoint 开始，只归约 `through_seq` 之后的事件。

但是，Runtime 的部分决策仍通过 `load_run_events(run_id)` 加载整个 Run 事件流后在 Python 中扫描：

| 位置 | 当前目的 | 当前复杂度 |
|---|---|---|
| `runtime/agent.py::_latest_unfinished_tool` | 查找待审批、待执行或待恢复的工具调用 | O(N) 事件加载与反向扫描 |
| `runtime/agent.py::_latest_unconsumed_model_response` | 恢复已落账但尚未消费的模型响应 | O(N) 事件加载、集合构造与反向扫描 |
| `runtime/agent.py::_next_model_step` | 判断新建下一步还是重试未完成步骤 | O(N) 事件加载与集合构造 |
| `runtime/agent.py::_call_model` | 计算同一逻辑模型调用已有多少次物理尝试 | O(N) 事件加载与计数 |

此外，`SQLiteEventStore.commit_run_checkpoint()` 当前先全量加载 Run 事件，再生成新 Checkpoint。这样旧 Checkpoint 可以加速读取，却没有加速下一个 Checkpoint 的创建。

结果是：即使 RunProjection 恢复只重放少量 tail events，一个长 Run 的每轮 Runtime 循环仍可能多次扫描完整事件流。

## 2. 问题定义

假设一个 Run 已有 100,000 条事件，最新 Checkpoint 的 `through_seq=99,980`：

- RunProjection 恢复只需读取约 20 条 tail events；
- 查找未完成工具仍可能读取 100,000 条；
- 查找未消费响应仍可能再读取 100,000 条；
- 计算模型 step 和 attempt offset 还会继续重复读取。

Checkpoint 本身不应该被扩展成容纳所有运行时细节的万能快照。正确边界是：

1. `events`：不可变事实与审计来源；
2. `RunProjection + Checkpoint`：Run 级状态及其恢复缓存；
3. `tool_calls/tool_attempts/approvals/budget_reservations`：现有事务型操作投影；
4. 新增 `model_calls`：模型调用进度的事务型操作投影；
5. Runtime 热路径只查询 Checkpoint tail 或有索引的操作投影，不扫描完整账本。

## 3. 目标

### 3.1 功能目标

- 从 Runtime 生产链路移除上述四处 `load_run_events(run_id)` 全量扫描。
- 使用现有 `tool_calls` 表直接定位未完成工具。
- 新增模型调用操作投影，直接回答：
  - 当前/下一模型 step；
  - 同一 `model_call_id` 的物理尝试次数；
  - 最新尚未被工具提议、拒绝事件或最终答案消费的模型响应。
- 创建 Checkpoint 时复用最新兼容 Checkpoint + tail，而不是重新归约完整 Run。
- 保持现有崩溃恢复、稳定 ID、审批、预算、Lease/Epoch fencing 和 `UNCERTAIN` 语义不变。

### 3.2 性能目标

- Runtime 每轮读取量不再随历史事件总数 N 线性增长。
- 未完成工具查询读取至多 2 条候选记录；模型进度查询读取至多 1 条记录。
- 在存在有效 Checkpoint 时，RunProjection 和新 Checkpoint 创建都只归约 `through_seq` 之后的事件。
- Prompt 路径继续保持“最近 64 个事件、最多 12 条 transcript”，本方案不改变 Prompt 语义。

### 3.3 非目标

- 不删除或压缩历史事件。
- 不把 SQL 暴露给模型。
- 不实现跨 Session 长期语义记忆。
- 不把多 Worker 执行改造成多 Agent DAG。
- 不改变文件工具的 pre/post hash reconciliation。
- 不宣称全系统 exactly-once；Shell 等不可证明副作用仍进入 `UNCERTAIN/NEEDS_ATTENTION`。

## 4. 核心不变量

实现必须保持以下不变量：

1. **事件先落账语义不变**：模型响应、工具 dispatch intent、审批和执行结果仍必须持久化后才能推进下一状态。
2. **同事务更新**：事件和对应操作投影必须在同一个数据库事务中更新，禁止“事件成功、投影失败”或相反的双写窗口。
3. **稳定身份不变**：
   - `model_call_id = model-call:{run_id}:{step}` 的逻辑身份语义保持不变；
   - `(run_id, response_event_id, ordinal)` 仍唯一确定一个 `tool_call_id`；
   - 恢复或重复投递不得创建第二个逻辑调用。
4. **Checkpoint 可丢弃**：Checksum、版本或内容不合法时，仍回退旧 Checkpoint；全部不可用时仍可全量重放 RunProjection。
5. **操作投影可校验**：新 `model_calls` 表必须能够由事件回填/审计；发现同一身份语义冲突时 fail closed。
6. **并发安全不变**：所有新写路径继续经过现有 execution context、Lease Epoch 和 stream version 校验。
7. **不静默选择歧义状态**：同一个 Run 若出现两个未完成工具调用，查询不得任意取一个，必须报告账本/投影不变量冲突。

## 5. 方案概览

```text
append typed event
       │
       ├── events（append-only 事实）
       │
       ├── RunProjection / Checkpoint（Run 级状态）
       │
       ├── tool_calls / tool_attempts（工具操作投影，已有）
       │
       └── model_calls（模型操作投影，新增）

Runtime loop
       │
       ├── get_run_projection()       -> checkpoint + tail
       ├── get_unfinished_tool_call() -> tool_calls 索引
       ├── get_pending_response()     -> model_calls 索引
       ├── get_next_model_step()      -> model_calls 索引
       └── get_model_attempt_count()  -> model_calls 主键
```

Checkpoint 继续只负责 Run 级 Projection。未完成工具和模型调用游标由专用表回答，避免把所有运行时细节复制到 Checkpoint JSON。

## 6. 数据模型

### 6.1 复用现有工具投影

现有 `tool_calls` 已包含：

- `tool_call_id`；
- `run_id`；
- `response_event_id` 和 `ordinal`；
- `state`；
- 参数 Hash、审批指纹、执行计划和最终输出/错误。

现有索引 `tool_calls_by_run_state(run_id, state)` 可以支持未完成工具查询，不需要新增重复的 `pending_tool_call_id` 字段。

查询必须联结 `events` 获取提议模型响应的 `seq`，并读取最多两条候选：

```sql
SELECT tc.*
FROM tool_calls AS tc
JOIN events AS response_event
  ON response_event.event_id = tc.response_event_id
WHERE tc.run_id = ?
  AND tc.state IN ('proposed', 'waiting_approval', 'ready', 'dispatched')
ORDER BY response_event.seq DESC, tc.ordinal DESC
LIMIT 2;
```

- 0 条：没有未完成工具；
- 1 条：返回该工具；
- 2 条：违反单工具串行 Runtime 不变量，抛出 `LedgerIntegrityError` 或更具体的冲突异常。

### 6.2 新增 `model_calls` 操作投影

新增 schema migration 4：

```sql
CREATE TABLE model_calls (
    model_call_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    step INTEGER NOT NULL CHECK(step >= 0),
    model_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('started', 'responded', 'consumed')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    latest_attempt_no INTEGER NOT NULL DEFAULT 0 CHECK(latest_attempt_no >= 0),
    first_started_event_id TEXT REFERENCES events(event_id),
    latest_attempt_event_id TEXT REFERENCES events(event_id),
    latest_failure_event_id TEXT REFERENCES events(event_id),
    response_event_id TEXT UNIQUE REFERENCES events(event_id),
    response_blob_sha256 TEXT REFERENCES blobs(sha256),
    response_seq INTEGER,
    consumed_event_id TEXT REFERENCES events(event_id),
    consumed_seq INTEGER,
    updated_seq INTEGER NOT NULL,
    UNIQUE(run_id, step)
);

CREATE INDEX model_calls_by_run_step
ON model_calls(run_id, step DESC);

CREATE INDEX model_calls_pending_response
ON model_calls(run_id, status, response_seq DESC);
```

字段语义：

- `started`：至少一个物理模型尝试已开始，但还没有持久化模型响应；恢复时继续同一个 step；
- `responded`：模型响应已经落账，但还没有被工具提议、拒绝事件或最终答案消费；
- `consumed`：该响应已经转化为下一确定性动作；
- `attempt_count/latest_attempt_no`：替代 `_call_model()` 对 `ModelCallStarted` 事件的全量计数；
- `response_event_id/response_blob_sha256`：替代 `_latest_unconsumed_model_response()` 的全量反向扫描；
- `step`：替代 `_next_model_step()` 通过解析完整事件流推断步骤。

不要仅依赖字符串解析 `model-call:{run_id}:{step}` 获取 step。为新写事件在 `ModelCallStartedPayload` 增加向后兼容的可选字段：

```python
step: int | None = Field(default=None, ge=0)
```

新 Runtime 必须写入 `step`；旧事件缺少该字段时，回填器可以按已冻结的 legacy `model_call_id` 格式解析。解析失败必须报告明确迁移错误，不得默认为 0。

## 7. 事务型投影更新

在 `_insert_event_in_transaction()` 插入事件后、提交事务前，调用内部纯分派函数，例如：

```python
_apply_operational_projection_in_transaction(connection, event)
```

只处理与 `model_calls` 有关的事件。任何冲突都必须回滚整笔事务。

### 7.1 `ModelCallStarted`

- 若 `model_call_id` 不存在：插入 `status='started'`，记录 `run_id/step/model_name`；
- 若已存在：验证 `run_id/step/model_name` 一致；
- `attempt_no` 必须严格大于旧 `latest_attempt_no`，或在完全相同事件重放场景下幂等返回；
- 更新 `attempt_count`、`latest_attempt_no`、`latest_attempt_event_id` 和 `updated_seq`；
- 已经 `responded/consumed` 的调用不得再追加新的 started attempt。

### 7.2 `ModelCallFailed`

- `model_call_id` 必须已经存在；
- 更新 `latest_failure_event_id/updated_seq`；
- 状态保持 `started`，因为现有恢复语义允许同一个逻辑 step 再次尝试；
- 不单独增加 attempt count，attempt count 以 `ModelCallStarted` 为准。

### 7.3 `ModelResponseReceived`

- `model_call_id` 必须已经存在；
- 若尚无响应，设置 `status='responded'`、`response_event_id`、blob Hash、`response_seq`；
- 若已经存在完全相同响应，允许幂等读取；
- 同一个 `model_call_id` 出现不同响应必须 fail closed。

### 7.4 响应被消费

以下事件提交时，把对应 model call 更新为 `consumed`：

- `ToolCallProposed`：使用事件的 `causation_event_id` 定位 `ModelResponseReceived`；
- `ModelOutputRejected`：使用 Payload 的 `response_event_id`；
- `FinalAnswerCommitted`：Runtime 写事件时补充 `causation_event_id=response_event.event_id`，投影器使用该因果引用。

消费更新必须验证：

- 引用的是同一个 Run 的 `ModelResponseReceived`；
- model call 当前为 `responded`，或已经由完全相同的消费事件标记为 `consumed`；
- 一个响应不得被两个不同后续动作消费。

## 8. Store API

在 `RuntimeStorePort` 和 `SQLiteEventStore` 增加明确的领域查询，Runtime 不直接拼 SQL：

```python
def get_unfinished_tool_call(self, run_id: str) -> ToolCallRecord | None: ...

def get_model_call(self, model_call_id: str) -> ModelCallRecord | None: ...

def get_next_model_step(self, run_id: str) -> int: ...

def get_latest_unconsumed_model_response(
    self, run_id: str
) -> PendingModelResponse | None: ...
```

建议新增不可变返回类型：

```python
@dataclass(frozen=True)
class ModelCallRecord:
    model_call_id: str
    run_id: str
    step: int
    model_name: str
    status: str
    attempt_count: int
    response_event_id: str | None
    response_blob_sha256: str | None

@dataclass(frozen=True)
class PendingModelResponse:
    event: EventEnvelope
    model_call_id: str
    step: int
    response_blob_sha256: str
```

查询语义：

### 8.1 `get_next_model_step(run_id)`

- 没有模型调用：返回 0；
- 最大 step 的调用没有响应：返回同一个 step，表示恢复该逻辑调用；
- 最大 step 已有响应：返回 `step + 1`；Runtime 在调用本方法前仍必须先检查未消费响应。

### 8.2 `get_latest_unconsumed_model_response(run_id)`

查询 `status='responded'`，按 `response_seq DESC` 取 1 条，并按 `response_event_id` 加载单条事件进行 Payload Hash 和类型验证。不得读取完整 Run 事件流。

### 8.3 模型 attempt offset

`_call_model()` 通过 `get_model_call(model_call_id)` 读取 `attempt_count`。不存在时为 0；存在时必须验证 record 的 `run_id/step` 与当前调用一致。

## 9. Runtime 改造

删除或改写以下私有扫描函数：

| 当前函数/逻辑 | 替换方式 |
|---|---|
| `_latest_unfinished_tool()` | `store.get_unfinished_tool_call(run_id)` |
| `_latest_unconsumed_model_response()` | `store.get_latest_unconsumed_model_response(run_id)` |
| `_next_model_step()` | `store.get_next_model_step(run_id)` |
| `_call_model()` 中 `previous_attempts = sum(...)` | `store.get_model_call(model_call_id).attempt_count` |

改造后，`runtime/agent.py` 的生产链路不得再调用 `load_run_events(run_id)`。评测、审计、CLI 导出仍可显式使用完整事件流，不属于 Runtime 热路径。

## 10. Checkpoint 创建优化

当前 `commit_run_checkpoint()` 使用：

```python
reduce_run_events(self._load_run_events_in_transaction(connection, run_id))
```

改为在同一事务中复用现有恢复逻辑：

```python
recovered = self._recover_run_projection_in_transaction(
    connection,
    run_id=run_id,
    run_row=row,
)
projection = recovered.projection
```

然后基于该 Projection 创建新快照。因为新 Checkpoint 尚未插入，恢复查询只会看到旧 Checkpoint，不会递归读取正在创建的 Checkpoint。

保持现有语义：

- Snapshot 的 `through_seq` 是创建快照前 Projection 的 `last_event_seq`；
- `CheckpointCommitted` 审计事件随后获得下一个 seq；
- 下一次恢复从 `seq > through_seq` 开始，因此会读到该审计事件；
- 审计事件不改变业务状态，但推进 `last_event_seq`。

## 11. 旧数据库升级与回填

DDL migration 只能创建表，已有事件还需要一次性回填 `model_calls`。

新增投影回填元数据表：

```sql
CREATE TABLE operational_projection_migrations (
    name TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    completed_at TEXT NOT NULL
);
```

初始化流程：

1. 应用 schema migration 4；
2. 检查 `model_calls` 投影版本标记；
3. 若未完成，在 `BEGIN IMMEDIATE` 事务内按 `(session_id, seq)` 顺序读取与模型调用有关的事件；
4. 验证每条事件 Payload Hash；
5. 复用同一个投影 reducer/updater 写入 `model_calls`；
6. 完成后写入投影版本标记并提交；
7. 崩溃时事务整体回滚，下次启动重试。

回填是升级时的一次性 O(N) 操作，可以接受；正常启动和 Runtime 热路径不得重复全量回填。

旧事件缺少显式 `step` 时：

- 仅允许解析冻结格式 `model-call:{run_id}:{non_negative_int}`；
- 必须验证解析出的 run_id 与事件 run_id 一致；
- 不符合格式的旧库升级失败并给出事件 ID，不得跳过事件。

## 12. 一致性与故障处理

### 12.1 事件与投影不一致

- 新写路径依靠同一 SQLite 事务避免不一致；
- 升级回填使用事件作为输入；
- 可增加只读审计命令，重新从相关事件计算 `model_calls` 并与表内容比较；
- 审计不一致时不得由 Runtime 猜测，进入明确的完整性错误。

### 12.2 多 Worker

- 新 API 只负责查询；推进状态仍要求有效 `ExecutionContext`；
- 投影更新发生在已经经过 Lease/stream version 校验的事件事务内；
- 被 fencing 的旧 Worker 不能提交事件，也不能提交对应投影更新。

### 12.3 多个未完成工具

当前 Agent Runtime 每轮只允许一个工具调用。若索引查询返回两个候选，说明历史或投影违反不变量。不要使用 `ORDER BY ... LIMIT 1` 静默吞掉异常，应读取 `LIMIT 2` 并 fail closed。

## 13. 测试计划

### 13.1 Store 单元测试

新增 `tests/test_runtime_operational_projections.py`，覆盖：

- 首次 `ModelCallStarted` 创建 `model_calls`；
- 同一 model call 多次 started 正确累计 attempt；
- 不同 run/step/model 复用同一 ID 被拒绝；
- response 将状态更新为 responded；
- tool proposal、model rejection、final answer 分别消费 response；
- 一个 response 被两个不同事件消费时被拒绝；
- pending response 查询只返回未消费的最新响应；
- next step 在无调用、未完成调用、已有响应三种情况下正确；
- 未完成工具查询使用现有 tool state；
- 两个未完成工具触发完整性错误；
- 投影更新失败时事件插入一并回滚，不消耗 seq。

### 13.2 Checkpoint 测试

扩展 `tests/test_checkpoint_recovery.py`：

- 创建第二个 Checkpoint 时，观察 `_load_run_events_in_transaction(..., after_seq=old_through_seq)`；
- 禁止创建新 Checkpoint 时使用 `after_seq=0`，除非不存在有效 Checkpoint；
- 最新 Checkpoint 损坏时回退旧版本；
- 所有 Checkpoint 损坏时仍允许一次全量归约；
- `through_seq` 与 `CheckpointCommitted` 事件 seq 继续保持现有关系。

### 13.3 Runtime 回归测试

在完整 Agent 生命周期测试中 monkeypatch 公共 `load_run_events()` 使其直接失败，验证：

- 新 Run 正常完成；
- 已落账模型响应可以恢复而不再次调用模型；
- 待审批工具可以恢复；
- dispatched 文件工具可以 reconcile；
- 模型 retry attempt 编号连续；
- Runtime 全链路不依赖完整事件扫描。

不要 monkeypatch `_load_run_events_in_transaction()`，因为 Checkpoint tail replay 合法使用它；测试目标是禁止 `after_seq=0` 的无界热路径和公共全量加载。

### 13.4 迁移测试

- 用 migration 3 结构构造旧数据库并写入多 step、多 attempt、pending response 历史；
- 升级后 `model_calls` 内容与旧扫描逻辑结果一致；
- 第二次启动不重复回填；
- 回填中注入崩溃后事务回滚，下一次启动成功重试；
- 非法 legacy model_call_id 提供明确错误和 event_id。

### 13.5 性能与查询计划

新增确定性 benchmark 或测试 fixture：

- 同一 Run 写入至少 10,000 个无关历史事件；
- 断言四个 Runtime 查询返回正确结果；
- 断言它们不调用 `load_run_events()`；
- 对关键 SQL 执行 `EXPLAIN QUERY PLAN`，确认使用：
  - `tool_calls_by_run_state`；
  - `model_calls_by_run_step`；
  - `model_calls_pending_response`；
  - events 主键查单条响应事件。

性能报告必须说明这是本地 SQLite 查询/Reducer 指标，不包装成端到端生产 SLA。

## 14. 实施顺序

1. 先增加失败测试，固定当前四个扫描点的业务语义。
2. 添加 migration 4、`ModelCallRecord` 和 Store Port 方法。
3. 实现事务型 `model_calls` 投影更新与冲突校验。
4. 实现旧事件一次性回填和迁移测试。
5. 使用现有 `tool_calls` 索引实现未完成工具查询。
6. 将 Runtime 四个扫描点切换为 Store 领域查询。
7. 为 FinalAnswerCommitted 补齐模型响应因果引用和消费标记。
8. 优化 Checkpoint 创建为旧 Checkpoint + tail。
9. 增加无全量扫描回归测试与查询计划测试。
10. 运行全量测试、Ruff 和类型检查，更新实现评审文档中的性能边界。

## 15. 验收标准

以下条件全部满足才算完成：

- `src/forge_replay/runtime/agent.py` 不再出现 `load_run_events(run_id)`；
- 未完成工具、pending response、next step、attempt count 均通过 Store 的有界索引查询获得；
- 事件与 `model_calls` 在同一事务提交或回滚；
- 旧 migration 3 数据库可以一次性、可恢复地回填；
- 新 Checkpoint 创建在有有效旧 Checkpoint 时只归约 tail events；
- corrupt checkpoint fallback 和全量重放兜底继续通过；
- 多 pending tool、多响应消费、身份语义冲突均 fail closed；
- Prompt 最近 64/最多 12 transcript 行为不变；
- 全量 `pytest` 通过；
- `ruff check .` 通过；
- `pyright` 通过；
- benchmark/查询计划证据保存，但不夸大为生产多机性能证明。

## 16. 交付文件预期

实现预计至少涉及：

- `src/forge_replay/persistence/schema.py`
- `src/forge_replay/persistence/store.py`
- `src/forge_replay/ports.py`
- `src/forge_replay/events.py`
- `src/forge_replay/runtime/agent.py`
- `tests/test_runtime_operational_projections.py`
- `tests/test_checkpoint_recovery.py`
- `tests/test_durable_agent_runtime.py`
- 相关 migration/backfill 测试与 benchmark 文件

实现者应先根据当前分支代码确认文件和类型名称，没有证据时不要顺带重构无关模块。
