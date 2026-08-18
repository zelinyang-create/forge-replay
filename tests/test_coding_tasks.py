import json

from forge_replay.eval.coding_tasks import TASKS, catalog_sha256, public_manifest
from forge_replay.eval.real_model_benchmark import _evaluate, _seed_repo


def test_frozen_catalog_has_24_balanced_unique_tasks():
    assert len(TASKS) == 24
    assert len({task.task_id for task in TASKS}) == 24
    assert sum(task.split == "dev" for task in TASKS) == 16
    assert sum(task.split == "held_out" for task in TASKS) == 8
    categories = {task.category for task in TASKS}
    assert len(categories) == 6
    assert all(sum(task.category == category for task in TASKS) == 4 for category in categories)
    assert len(catalog_sha256()) == 64


def test_public_manifest_excludes_hidden_evaluator_source():
    encoded = json.dumps(public_manifest())
    assert "evaluator_source" not in encoded
    assert public_manifest()["split_counts"] == {"dev": 16, "held_out": 8}


def test_all_seed_fixtures_are_git_repositories_and_evaluators_fail_before_fix(tmp_path):
    # A frozen benchmark should start from genuinely failing tasks; otherwise its
    # task success rate can be inflated by already-correct fixtures.
    for task in TASKS:
        repo = _seed_repo(task, tmp_path / task.task_id)
        passed, _ = _evaluate(task, repo, tmp_path / f"eval-{task.task_id}")
        assert not passed, task.task_id
        assert not list(repo.rglob("__pycache__")), task.task_id
