&nbsp;
# ForgeReplay

ForgeReplay is a durable, replay-aware coding agent harness built on the small
model/tool loop from
[`rasbt/mini-coding-agent`](https://github.com/rasbt/mini-coding-agent). It is an
Apache-2.0 fork: upstream owns the original single-file agent, workspace
context, structured tools, approval modes, JSON session resume, context
reduction, and bounded delegation. The `forge_replay` package and its durable
execution/evaluation layers are the additions in this fork.

The upstream snapshot is preserved as tag `upstream-baseline-717cae4`.

## What ForgeReplay Adds

- Typed, append-only runtime events in SQLite with WAL, checksums, immutable
  blobs, deterministic projections, and checkpoint-tail replay.
- A durable model/tool state machine with stable UUIDv7 identities, approval
  fingerprints, budget reservations, cancellation, bounded provider retries,
  and expiring fenced run leases.
- Per-run Git Worktrees plus strict path validation. User changes in the source
  checkout are refused by default and are never silently reset.
- Replay-safe file reads, writes, patches, listings, and searches. Mutations use
  before/after SHA-256 receipts and atomic replacement to reconcile crashes.
- Bounded argv-only process execution with output limits, timeout/cancellation,
  process-tree cleanup, and an explicit `UNCERTAIN` state instead of replaying
  arbitrary shell effects after an ambiguous crash.
- Result export as a binary patch, copied untracked files, and a SHA-256
  manifest; clean-only worktree cleanup; durable cancellation and redacted
  event-trace export.
- Deterministic crash conformance, projection microbenchmarks, and a frozen
  24-task real-model coding suite with a 16/8 development/held-out split.

## Quick Start

Install Python 3.10+, Git, `uv`, and Ollama, then pull a model:

```bash
ollama pull qwen3.5:4b
uv sync
```

Start a durable run in a clean Git repository:

```bash
uv run forge-replay start "Fix the failing parser tests" --repo /path/to/repo
```

File mutations and processes pause for approval unless their explicit
auto-approval flags are passed. Continue a pending call and resume the run:

```bash
uv run forge-replay approve <approval-id> allow --reason "reviewed exact call"
uv run forge-replay resume <run-id>
```

Inspect, cancel, export a redacted trace, or preserve results:

```bash
uv run forge-replay status <run-id>
uv run forge-replay cancel <run-id> --reason "no longer needed"
uv run forge-replay trace <run-id> --output trace.json
uv run forge-replay export <run-id>
```

The original educational CLI remains available as `mini-coding-agent` for an
upstream-compatible baseline.

## Measured Evidence

All numbers below are local deterministic measurements, not production SLAs:

| Evaluation | Result | Valid interpretation |
|---|---:|---|
| Shared file-effect crash boundaries | 24/24 safe hardened recoveries; 0/24 baseline safe terminals | Harness recovery semantics under two injected file crash windows |
| Duplicate file effects | 0 across 24 hardened fault runs | No duplicate write in the covered deterministic scenarios |
| Automatic recovery latency | P50 20.1 ms; P95 39.0 ms | Local recovery handler time, excluding a real model call |
| 10,000-event projection replay | 41.58 ms full vs 0.78 ms from a 200-event checkpoint tail | 53.21x CPU reducer microbenchmark speedup, not end-to-end resume latency |
| Bailian `qwen3-coder-plus` Held-out coding | 14/24 runs passed (58.3%); task-cluster bootstrap 95% CI 25.0%–87.5% | 8 frozen tasks × 3 repeats, temperature 0, thinking off, process tool disabled |
| Bailian end-to-end run latency | P50 11.65 s; P95 16.87 s | Model + Harness file-tool loop + durable persistence; hidden evaluator runs afterward |

Reports, per-run patches/model outputs, Git metadata, platform, and denominators
are in [`benchmarks/results`](benchmarks/results). The published Bailian result
used the official OpenAI-compatible endpoint with `qwen3-coder-plus`: all 24
runs reached a normal Harness terminal state; 14 passed hidden tests. Five of
eight tasks passed at least once and four passed all three repeats. Repeated runs
are correlated, so the report uses a task-cluster bootstrap rather than treating
24 rows as independent tasks.

Run the same suite with an environment variable; never put a key in a config or
command committed to Git:

```bash
uv run python -m forge_replay.eval.real_model_benchmark \
  --provider openai-compatible \
  --base-url https://dashscope.aliyuncs.com/compatible-mode/v1 \
  --api-key-env DASHSCOPE_API_KEY \
  --split held_out --repeats 3 --model qwen3-coder-plus \
  --i-understand-model-generated-code-runs-locally
```

## Safety Boundary

Git Worktrees protect the base checkout; they are not an operating-system
sandbox. The process supervisor cannot prevent generated code from reading the
host, contacting the network, or affecting services available to the current
account. Do not auto-approve processes on a workstation containing credentials.
Use an ephemeral VM/container, a low-privilege account, no mounted secrets, and
default-deny networking for real-model evaluation.

Arbitrary processes are deliberately not claimed as exactly-once. Read-only
tools are retryable; file writes/patches are detectable and reconcilable; an
ambiguous process crash becomes `UNCERTAIN` and requires attention.

See the [technical design](docs/harness-technical-design.md) and
[implementation review](docs/implementation-review.md) for architecture,
evidence, deferred scope, and remaining risks.

## Upstream Tutorial and Baseline Documentation

The remainder of this README is retained from the upstream project for
attribution and baseline usage.

### Modification Notice

ForgeReplay adds the `forge_replay` package, durable CLI, tests, benchmarks, and
design/review documentation. The upstream license and history are retained;
modified upstream-derived files are identified by Git history and this release
notice.

<a href="https://magazine.sebastianraschka.com/p/components-of-a-coding-agent">
  <img src="https://substack-post-media.s3.amazonaws.com/public/images/49b97718-57f4-4977-99c8-8ad5c4d32af3_1548x862.png" width="500px">
</a>

<br>

**[The detailed tutorial: Components of a Coding Agent](https://magazine.sebastianraschka.com/p/components-of-a-coding-agent)**


&nbsp;
## Six Core Components

<a href="https://magazine.sebastianraschka.com/p/components-of-a-coding-agent">
  <img alt="Six core components of a coding agent" src="https://sebastianraschka.com/images/github/mini-coding-agent/six-components.webp" width="500px">
</a>

This coding harness is organized around six practical building blocks:

1. **Live repo context**  
   The agent collects stable workspace facts upfront, such as repo layout, instructions, and git state.
2. **Prompt shape and cache reuse**  
   A stable prompt prefix, which is separate from the changing request, transcript, and memory so repeated model calls can reuse the static parts efficiently.
3. **Structured tools, validation, and permissions**  
   The model works through named tools with checked inputs, workspace path validation, and approval gates instead of free-form arbitrary actions.
4. **Context reduction and output management**  
   Long outputs are clipped, repeated reads are deduplicated, and older transcript entries are compressed to keep prompt size under control.
5. **Transcripts, memory, and resumption**  
   The runtime keeps both a full durable transcript and a smaller working memory so sessions can be resumed while preserving important state via working memory.
6. **Delegation and bounded subagents**  
   Scoped subtasks can be delegated to helper agents that inherit enough context to help (but operate within limits).

&nbsp;
## Requirements

You need:

- Python 3.10+
- Ollama installed
- an Ollama model pulled locally

Optional:

- `uv` for environment management and the `mini-coding-agent` CLI entry point

This project has no Python runtime dependency beyond the standard library, so you can run it directly with `python mini_coding_agent.py` if you do not want to use `uv`.

&nbsp;
## Install Ollama

Install Ollama on your machine so the `ollama` command is available in your shell.

Official installation link: [ollama.com/download](https://ollama.com/download)

Then verify:

```bash
ollama --help
```

Start the server:

```bash
ollama serve
```

In another terminal, pull a model. Example:

```bash
ollama pull qwen3.5:4b
```

Qwen 3.5 model library:

- [ollama.com/library/qwen3.5](https://ollama.com/library/qwen3.5)

The default in this project is `qwen3.5:4b`. If you have sufficient memory, it is worth trying a larger model such as `qwen3.5:9b` or another larger Qwen 3.5 variant. The agent just sends prompts to Ollama's `/api/generate` endpoint.

&nbsp;
## Project Setup

Clone the repo or your fork and change into it:

```bash
git clone https://github.com/rasbt/mini-coding-agent.git
cd mini-coding-agent
```

If you forked it first, use your fork URL instead:

```bash
git clone https://github.com/<your-github-user>/mini-coding-agent.git
cd mini-coding-agent
```



&nbsp;
## Basic Usage

Start the agent:

```bash
cd mini-coding-agent
uv run mini-coding-agent
```

Without `uv`, run the script directly:

```bash
cd mini-coding-agent
python mini_coding_agent.py
```

By default it uses:

- model: `qwen3.5:4b`
- approval: `ask`

For a concrete usage example, see [EXAMPLE.md](EXAMPLE.md).

&nbsp;
## Approval Modes

Risky tools such as shell commands and file writes are gated by approval.

- `--approval ask`
  prompts before risky actions (default and recommended)
- `--approval auto`
  allows risky actions automatically, including arbitrary command execution and file writes by the model; use only with trusted prompts and trusted repositories
- `--approval never`
  denies risky actions

Example:

```bash
uv run mini-coding-agent --approval auto
```



&nbsp;
## Resume Sessions

The agent saves sessions under the target workspace root in:

```text
.mini-coding-agent/sessions/
```

Resume the latest session:

```bash
uv run mini-coding-agent --resume latest
```


Resume a specific session:

```bash
uv run mini-coding-agent --resume 20260401-144025-2dd0aa
```


&nbsp;
## Interactive Commands

Inside the REPL, slash commands are handled directly by the agent instead of
being sent to the model as a normal task.

- `/help`
  shows the list of available interactive commands
- `/memory`
  prints the distilled session memory, including the current task, tracked files, and notes
- `/session`
  prints the path to the current saved session JSON file
- `/reset`
  clears the current session history and distilled memory but keeps you in the REPL
- `/exit`
  exits the interactive session
- `/quit`
  exits the interactive session; alias for `/exit`

&nbsp;
## Main CLI Flags

```bash
uv run mini-coding-agent --help
```

Without `uv`:

```bash
python mini_coding_agent.py --help
```

CLI flags are passed before the agent starts. Use them to choose the workspace,
model connection, resume behavior, approval mode, and generation limits.

Important flags:

- `--cwd`
  sets the workspace directory the agent should inspect and modify; default: `.`
- `--model`
  selects the Ollama model name, such as `qwen3.5:4b`; default: `qwen3.5:4b`
- `--host`
  points the agent at the Ollama server URL (usually not needed); default: `http://127.0.0.1:11434`
- `--ollama-timeout`
  controls how long the client waits for an Ollama response (usually not needed); default: `300` seconds
- `--resume`
  resumes a saved session by id or uses `latest`; default: start a new session
- `--approval`
  controls how risky tools are handled: `ask`, `auto`, or `never`; default: `ask`
- `--max-steps`
  limits how many model and tool turns are allowed for one user request; default: `6`
- `--max-new-tokens`
  caps the model output length for each step; default: `512`
- `--temperature`
  controls sampling randomness; default: `0.2`
- `--top-p`
  controls nucleus sampling for generation; default: `0.9`

&nbsp;
## Example

See [EXAMPLE.md](EXAMPLE.md)

&nbsp;
## Notes & Tips

- The agent expects the model to emit either `<tool>...</tool>` or `<final>...</final>`.
- Different Ollama models will follow those instructions with different reliability.
- If the model does not follow the format well, use a stronger instruction-following model.
- The agent is intentionally small and optimized for readability, not robustness.
