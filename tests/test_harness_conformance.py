from forge_replay.eval.harness_conformance import run_benchmark


def test_conformance_report_uses_triggered_denominators_and_raw_runs():
    report = run_benchmark(task_count=2)

    baseline = report["variants"]["upstream_baseline_adapter"]
    hardened = report["variants"]["hardened"]
    assert report["suite"] == "deterministic_harness_conformance_not_coding_ability"
    assert report["analysis_population"] == "all_started_runs_intent_to_treat"
    assert baseline["runs"] == baseline["faults_triggered"] == 4
    assert baseline["safe_terminal"] == 0
    assert baseline["recovery_ms_p50"] is None
    assert hardened["runs"] == hardened["faults_triggered"] == 4
    assert hardened["planned"] == hardened["started"] == hardened["evaluable"] == 4
    assert hardened["safe_terminal"] == hardened["tests_passed"] == 4
    assert hardened["duplicate_effects"] == 0
    assert len(report["raw_runs"]) == 8
