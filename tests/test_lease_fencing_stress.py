from forge_replay.eval.lease_fencing_stress import run_stress


def test_stale_epoch_stress_rejects_every_mutation():
    report = run_stress(stale_attempts=50, valid_every=10)

    assert report["stale_attempts"] == report["stale_rejected"] == 50
    assert report["stale_accepted"] == 0
    assert report["valid_commits"] == report["durable_valid_events"] == 5
    assert report["current_epoch"] == report["old_epoch"] + 1
