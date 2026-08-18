from forge_replay.eval.projection_benchmark import run_benchmark


def test_projection_benchmark_checks_equivalent_replay_paths():
    report = run_benchmark(event_count=100, tail_count=10, iterations=2)
    assert report["event_count"] == 100
    assert report["checkpoint_tail_events"] == 10
    assert report["full_replay_ms_p50"] > 0
    assert report["checkpoint_replay_ms_p50"] > 0
    assert report["suite"].endswith("not_end_to_end_resume")
