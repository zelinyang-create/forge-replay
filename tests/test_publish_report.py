import json

import pytest

from forge_replay.eval.publish_report import build_public_report


def private_report():
    return {
        "runs": 4,
        "hidden_tests_passed": 3,
        "task_success_rate": 0.75,
        "raw_runs": [
            {
                "task_id": "a",
                "hidden_tests_passed": True,
                "evaluator_stderr": "",
            },
            {
                "task_id": "a",
                "hidden_tests_passed": False,
                "evaluator_stderr": "File C:\\Users\\person\\Temp\\x.py\nAssertionError",
            },
            {"task_id": "b", "hidden_tests_passed": True, "evaluator_stderr": ""},
            {"task_id": "b", "hidden_tests_passed": True, "evaluator_stderr": ""},
        ],
    }


def test_public_report_removes_paths_and_reports_task_clusters():
    report = build_public_report(private_report(), bootstrap_samples=100)
    encoded = json.dumps(report)
    assert "Users" not in encoded
    assert report["tasks"] == 2
    assert report["tasks_with_any_pass"] == 2
    assert report["tasks_with_all_repeats_passed"] == 1
    assert report["by_task"]["a"]["passed"] == 1
    assert report["raw_runs"][1]["evaluator_error"] == "AssertionError"


def test_public_report_refuses_api_key_shaped_content():
    value = private_report()
    value["model"] = "sk-1234567890abcdefghijkl"
    with pytest.raises(ValueError, match="credential"):
        build_public_report(value, bootstrap_samples=10)
