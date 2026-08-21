from forge_replay.eval.runtime_hot_path_benchmark import run_benchmark


def test_runtime_hot_path_benchmark_reports_paired_raw_samples(tmp_path):
    report = run_benchmark(
        tmp_path / "benchmark",
        history_sizes=(100,),
        iterations=3,
        warmups=1,
    )

    assert report["cache_mode"] == "warm-os/new-connection"
    assert report["iterations"] == 3
    workload = report["workloads"][0]
    assert workload["history_events"] == 100
    assert workload["main_database_bytes"] > 0
    assert set(workload["metrics"]) == {
        "unfinished_tool",
        "pending_response",
        "next_model_step",
        "model_attempt_offset",
    }
    for metric in workload["metrics"].values():
        assert len(metric["baseline_samples_ms"]) == 3
        assert len(metric["indexed_samples_ms"]) == 3
        assert metric["p50_speedup"] > 0
