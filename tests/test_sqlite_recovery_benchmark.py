from forge_replay.eval.sqlite_recovery_benchmark import run_benchmark


def test_sqlite_recovery_benchmark_compares_real_store_paths():
    report = run_benchmark(event_count=50, tail_count=5, iterations=2)

    assert report["suite"] == "sqlite_checkpoint_recovery_end_to_end"
    assert report["full_recovery_ms_p50"] > 0
    assert report["checkpoint_recovery_ms_p50"] > 0
    assert report["p50_speedup"] > 0
