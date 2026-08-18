"""Build a privacy-safe, statistically explicit real-model report."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
from collections import defaultdict
from pathlib import Path


def build_public_report(private: dict, *, bootstrap_samples: int = 10_000) -> dict:
    raw_runs = private.get("raw_runs", [])
    grouped: dict[str, list[dict]] = defaultdict(list)
    public_runs = []
    for run in raw_runs:
        grouped[run["task_id"]].append(run)
        public_run = dict(run)
        public_run["evaluator_error"] = _last_error(run.get("evaluator_stderr", ""))
        public_run.pop("evaluator_stderr", None)
        public_runs.append(public_run)

    task_results = {
        task_id: {
            "runs": len(runs),
            "passed": sum(bool(run["hidden_tests_passed"]) for run in runs),
            "pass_rate": sum(bool(run["hidden_tests_passed"]) for run in runs) / len(runs),
        }
        for task_id, runs in sorted(grouped.items())
    }
    rates = [
        [1.0 if run["hidden_tests_passed"] else 0.0 for run in runs]
        for _, runs in sorted(grouped.items())
    ]
    ci_low, ci_high = _cluster_bootstrap(rates, samples=bootstrap_samples)
    public = {
        key: value
        for key, value in private.items()
        if key not in {"raw_runs"}
    }
    public.update(
        {
            "artifact_policy": (
                "API credentials and local paths excluded; evaluator errors reduced to final line"
            ),
            "task_cluster_bootstrap_95_ci": [ci_low, ci_high],
            "bootstrap_samples": bootstrap_samples,
            "tasks": len(task_results),
            "tasks_with_any_pass": sum(item["passed"] > 0 for item in task_results.values()),
            "tasks_with_all_repeats_passed": sum(
                item["passed"] == item["runs"] for item in task_results.values()
            ),
            "by_task": task_results,
            "raw_runs": public_runs,
        }
    )
    encoded = json.dumps(public, sort_keys=True)
    if re.search(r"sk-[A-Za-z0-9]{16,}", encoded):
        raise ValueError("public report appears to contain an API credential")
    return public


def publish(input_path: Path, output_path: Path) -> dict:
    raw = input_path.read_bytes()
    private = json.loads(raw)
    public = build_public_report(private)
    public["private_report_sha256"] = hashlib.sha256(raw).hexdigest()
    encoded = json.dumps(public, indent=2, sort_keys=True) + "\n"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary.write_text(encoded, encoding="utf-8")
    os.replace(temporary, output_path)
    return public


def _last_error(stderr: str) -> str | None:
    lines = [line.strip() for line in stderr.splitlines() if line.strip()]
    return lines[-1] if lines else None


def _cluster_bootstrap(clusters: list[list[float]], *, samples: int) -> tuple[float, float]:
    if not clusters:
        return 0.0, 0.0
    rng = random.Random(20260819)
    estimates = []
    for _ in range(samples):
        selected = [clusters[rng.randrange(len(clusters))] for _ in clusters]
        values = [value for cluster in selected for value in cluster]
        estimates.append(sum(values) / len(values))
    estimates.sort()
    low = estimates[int(samples * 0.025)]
    high = estimates[min(samples - 1, int(samples * 0.975))]
    return low, high


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    report = publish(args.input, args.output)
    print(
        json.dumps(
            {
                "output": str(args.output),
                "runs": report.get("runs"),
                "task_success_rate": report.get("task_success_rate"),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
