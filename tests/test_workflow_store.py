import threading

import pytest

from runtime_service.workflow_store import (
    ClaimOutcome,
    ExecutionOutcome,
    SQLiteWorkflowStore,
    StaleAttemptError,
    StepReuseOutcome,
    ToolCallStatus,
    WorkflowStatus,
)

HASH_A = "sha256:aaaa"
HASH_B = "sha256:bbbb"


def test_create_execution_and_idempotent_reget(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")

    first = store.create_or_get_execution("run-1", "release_validation", HASH_A)
    assert first.outcome == ExecutionOutcome.CREATED
    assert first.execution.status == WorkflowStatus.PENDING

    second = store.create_or_get_execution("run-1", "release_validation", HASH_A)
    assert second.outcome == ExecutionOutcome.EXISTING
    assert second.execution.created_at == first.execution.created_at

    assert store.get_execution("run-1") is not None


def test_execution_input_mismatch(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)

    result = store.create_or_get_execution("run-1", "release_validation", HASH_B)
    assert result.outcome == ExecutionOutcome.INPUT_MISMATCH
    assert result.execution.input_hash == HASH_A


def test_execution_workflow_type_mismatch(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)

    result = store.create_or_get_execution("run-1", "benchmark", HASH_A)
    assert result.outcome == ExecutionOutcome.WORKFLOW_TYPE_MISMATCH
    assert result.execution.workflow_type == "release_validation"


def test_execution_running_to_ready(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    store.mark_running("run-1")

    finalized = store.finalize_ready("run-1", result_json='{"status": "ready"}')
    assert finalized.status == WorkflowStatus.READY
    assert finalized.result_json == '{"status": "ready"}'
    assert finalized.completed_at is not None

    events = [event.event_type for event in store.list_events("run-1")]
    assert events == ["workflow.started", "workflow.ready"]

    # Second finalize call for the same run is a no-op: no duplicate event.
    store.finalize_ready("run-1", result_json='{"status": "ready"}')
    assert [event.event_type for event in store.list_events("run-1")] == [
        "workflow.started",
        "workflow.ready",
    ]


def test_execution_running_to_blocked(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    store.mark_running("run-1")

    finalized = store.finalize_blocked(
        "run-1", result_json='{"status": "blocked"}', error_code="compatibility_unmet"
    )
    assert finalized.status == WorkflowStatus.BLOCKED
    assert finalized.error_code == "compatibility_unmet"
    events = [event.event_type for event in store.list_events("run-1")]
    assert events == ["workflow.started", "workflow.blocked"]


def test_execution_running_to_failed(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    store.mark_running("run-1")

    finalized = store.finalize_failed("run-1", error_code="unexpected_exception")
    assert finalized.status == WorkflowStatus.FAILED
    assert finalized.error_code == "unexpected_exception"
    assert finalized.result_json is None
    events = [event.event_type for event in store.list_events("run-1")]
    assert events == ["workflow.started", "workflow.failed"]


def test_claim_fresh_step_returns_token_and_attempt_one(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)

    result = store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)
    assert result.outcome == ClaimOutcome.CLAIMED
    assert result.attempt_token is not None
    assert result.step.attempt_count == 1
    assert result.step.status == ToolCallStatus.RUNNING

    events = [event.event_type for event in store.list_events("run-1")]
    assert events == ["step.claimed"]


def test_reuse_completed_step_copies_terminal_source_evidence_without_attempt(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("source", "release_validation", HASH_A)
    store.mark_running("source")
    source_claim = store.claim_step(
        "source", "unit_tests", "run_unit_test_check", HASH_A, max_attempts=2
    )
    store.complete_step(
        "source",
        "unit_tests",
        source_claim.attempt_token,
        result_json='{"passed": true}',
    )
    store.finalize_ready("source", result_json='{"status": "ready"}')
    store.create_or_get_execution("target", "release_validation", HASH_B)
    store.mark_running("target")

    copied = store.reuse_completed_step(
        "source", "target", "unit_tests", "run_unit_test_check", HASH_A
    )
    repeated = store.reuse_completed_step(
        "source", "target", "unit_tests", "run_unit_test_check", HASH_A
    )

    assert copied.outcome == StepReuseOutcome.COPIED
    assert copied.step.attempt_count == 0
    assert copied.step.result_json == '{"passed": true}'
    assert repeated.outcome == StepReuseOutcome.EXISTING
    assert store.get_step("source", "unit_tests").attempt_count == 1
    assert [event.event_type for event in store.list_events("target")].count(
        "step.replay_reused"
    ) == 1


def test_reuse_completed_step_reports_signature_mismatch_without_copy(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("source", "release_validation", HASH_A)
    store.mark_running("source")
    source_claim = store.claim_step(
        "source", "unit_tests", "run_unit_test_check", HASH_A, max_attempts=2
    )
    store.complete_step("source", "unit_tests", source_claim.attempt_token, result_json="{}")
    store.finalize_ready("source", result_json='{"status": "ready"}')
    store.create_or_get_execution("target", "release_validation", HASH_B)
    store.mark_running("target")

    result = store.reuse_completed_step(
        "source", "target", "unit_tests", "run_unit_test_check", HASH_B
    )

    assert result.outcome == StepReuseOutcome.NOT_REUSABLE
    assert store.get_step("target", "unit_tests") is None


def test_reuse_completed_step_rejects_terminal_target(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    for run_id in ("source", "target"):
        store.create_or_get_execution(run_id, "release_validation", HASH_A)
        store.mark_running(run_id)
    source_claim = store.claim_step(
        "source", "unit_tests", "run_unit_test_check", HASH_A, max_attempts=2
    )
    store.complete_step("source", "unit_tests", source_claim.attempt_token, result_json="{}")
    store.finalize_ready("source", result_json='{"status": "ready"}')
    store.finalize_ready("target", result_json='{"status": "ready"}')

    with pytest.raises(ValueError, match="must be RUNNING"):
        store.reuse_completed_step(
            "source", "target", "unit_tests", "run_unit_test_check", HASH_A
        )

    assert store.get_step("target", "unit_tests") is None


def test_concurrent_reuse_copies_one_row_and_one_event(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("source", "release_validation", HASH_A)
    store.mark_running("source")
    source_claim = store.claim_step(
        "source", "unit_tests", "run_unit_test_check", HASH_A, max_attempts=2
    )
    store.complete_step("source", "unit_tests", source_claim.attempt_token, result_json="{}")
    store.finalize_ready("source", result_json='{"status": "ready"}')
    store.create_or_get_execution("target", "release_validation", HASH_B)
    store.mark_running("target")
    barrier = threading.Barrier(2)
    outcomes: list[StepReuseOutcome] = []
    lock = threading.Lock()

    def reuse() -> None:
        barrier.wait()
        result = store.reuse_completed_step(
            "source", "target", "unit_tests", "run_unit_test_check", HASH_A
        )
        with lock:
            outcomes.append(result.outcome)

    threads = [threading.Thread(target=reuse) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(outcome.value for outcome in outcomes) == ["copied", "existing"]
    assert len(store.list_steps("target")) == 1
    assert [event.event_type for event in store.list_events("target")].count(
        "step.replay_reused"
    ) == 1


def test_two_threads_racing_first_claim_only_one_wins(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)

    barrier = threading.Barrier(2)
    outcomes: list[ClaimOutcome] = []
    lock = threading.Lock()

    def attempt() -> None:
        barrier.wait(timeout=5)
        result = store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)
        with lock:
            outcomes.append(result.outcome)

    threads = [threading.Thread(target=attempt) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert sorted(outcomes) == sorted([ClaimOutcome.CLAIMED, ClaimOutcome.ALREADY_RUNNING])
    step = store.get_step("run-1", "load_manifest")
    assert step.attempt_count == 1


def test_running_step_defaults_to_already_running(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)

    result = store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)
    assert result.outcome == ClaimOutcome.ALREADY_RUNNING
    assert store.get_step("run-1", "load_manifest").attempt_count == 1


def test_interrupted_recovery_issues_new_token_and_increments_attempt(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    first = store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)

    recovered = store.recover_interrupted_step("run-1", "load_manifest")
    assert recovered.status == ToolCallStatus.FAILED
    assert recovered.error_code == "interrupted"

    second = store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)
    assert second.outcome == ClaimOutcome.CLAIMED
    assert second.attempt_token != first.attempt_token
    assert second.step.attempt_count == 2

    events = [event.event_type for event in store.list_events("run-1")]
    assert events == ["step.claimed", "step.interrupted_recovery", "step.claimed"]


def test_old_attempt_token_cannot_complete_new_attempt(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    first = store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)
    store.recover_interrupted_step("run-1", "load_manifest")
    store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)

    with pytest.raises(StaleAttemptError):
        store.complete_step("run-1", "load_manifest", first.attempt_token, result_json="{}")


def test_failed_step_can_be_reclaimed(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    claimed = store.claim_step("run-1", "unit_tests", "run_unit_test_check", HASH_A, max_attempts=3)
    store.fail_step("run-1", "unit_tests", claimed.attempt_token, error_code="transient_test_failure")

    retried = store.claim_step("run-1", "unit_tests", "run_unit_test_check", HASH_A, max_attempts=3)
    assert retried.outcome == ClaimOutcome.CLAIMED
    assert retried.step.attempt_count == 2
    assert retried.attempt_token != claimed.attempt_token


def test_attempts_exhausted(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)

    token = store.claim_step(
        "run-1", "unit_tests", "run_unit_test_check", HASH_A, max_attempts=2
    ).attempt_token
    store.fail_step("run-1", "unit_tests", token, error_code="transient_test_failure")

    retried = store.claim_step("run-1", "unit_tests", "run_unit_test_check", HASH_A, max_attempts=2)
    assert retried.outcome == ClaimOutcome.CLAIMED
    store.fail_step("run-1", "unit_tests", retried.attempt_token, error_code="transient_test_failure")

    exhausted = store.claim_step("run-1", "unit_tests", "run_unit_test_check", HASH_A, max_attempts=2)
    assert exhausted.outcome == ClaimOutcome.ATTEMPTS_EXHAUSTED
    assert exhausted.step.status == ToolCallStatus.FAILED
    assert exhausted.step.attempt_count == 2


def test_completed_step_same_hash_returns_cached(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    claimed = store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)
    store.complete_step("run-1", "load_manifest", claimed.attempt_token, result_json='{"ok": true}')

    cached = store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)
    assert cached.outcome == ClaimOutcome.CACHED
    assert cached.step.result_json == '{"ok": true}'
    assert cached.attempt_token is None


def test_completed_step_different_hash_returns_input_mismatch(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    claimed = store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)
    store.complete_step("run-1", "load_manifest", claimed.attempt_token, result_json='{"ok": true}')

    mismatched = store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_B, max_attempts=3)
    assert mismatched.outcome == ClaimOutcome.INPUT_MISMATCH
    # The stored result must not be silently reused or overwritten.
    assert store.get_step("run-1", "load_manifest").result_json == '{"ok": true}'
    assert store.get_step("run-1", "load_manifest").input_hash == HASH_A


@pytest.mark.parametrize(
    "persisted_status",
    [ToolCallStatus.COMPLETED, ToolCallStatus.RUNNING, ToolCallStatus.FAILED],
)
def test_existing_step_different_tool_returns_definition_mismatch_without_mutation(
    tmp_path,
    persisted_status,
):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    claimed = store.claim_step(
        "run-1", "load_manifest", "old_registered_tool", HASH_A, max_attempts=3
    )
    if persisted_status == ToolCallStatus.COMPLETED:
        store.complete_step("run-1", "load_manifest", claimed.attempt_token, result_json="{}")
    elif persisted_status == ToolCallStatus.FAILED:
        store.fail_step("run-1", "load_manifest", claimed.attempt_token, error_code="boom")
    before_events = list(store.list_events("run-1"))

    mismatched = store.claim_step(
        "run-1", "load_manifest", "new_registered_tool", HASH_A, max_attempts=3
    )

    assert mismatched.outcome == ClaimOutcome.DEFINITION_MISMATCH
    persisted = store.get_step("run-1", "load_manifest")
    assert persisted is not None
    assert persisted.tool_name == "old_registered_tool"
    assert persisted.status == persisted_status
    assert persisted.attempt_count == 1
    assert store.list_events("run-1") == before_events


@pytest.mark.parametrize(
    "persisted_status",
    [ToolCallStatus.COMPLETED, ToolCallStatus.RUNNING, ToolCallStatus.FAILED],
)
def test_existing_step_different_input_returns_mismatch_for_every_status(
    tmp_path,
    persisted_status,
):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    claimed = store.claim_step(
        "run-1", "load_manifest", "registered_tool", HASH_A, max_attempts=3
    )
    if persisted_status == ToolCallStatus.COMPLETED:
        store.complete_step("run-1", "load_manifest", claimed.attempt_token, result_json="{}")
    elif persisted_status == ToolCallStatus.FAILED:
        store.fail_step("run-1", "load_manifest", claimed.attempt_token, error_code="boom")
    before_events = list(store.list_events("run-1"))

    mismatched = store.claim_step(
        "run-1", "load_manifest", "registered_tool", HASH_B, max_attempts=3
    )

    assert mismatched.outcome == ClaimOutcome.INPUT_MISMATCH
    persisted = store.get_step("run-1", "load_manifest")
    assert persisted is not None
    assert persisted.input_hash == HASH_A
    assert persisted.status == persisted_status
    assert persisted.attempt_count == 1
    assert store.list_events("run-1") == before_events


def test_state_transitions_always_pair_with_an_event(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    store.mark_running("run-1")
    claimed = store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)
    store.complete_step("run-1", "load_manifest", claimed.attempt_token, result_json="{}")
    store.finalize_ready("run-1", result_json='{"status": "ready"}')

    events = [event.event_type for event in store.list_events("run-1")]
    assert events == [
        "workflow.started",
        "step.claimed",
        "step.completed",
        "workflow.ready",
    ]


def test_event_sequence_strictly_increasing_per_run(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    store.mark_running("run-1")
    for step_id in ("step_a", "step_b", "step_c"):
        claimed = store.claim_step("run-1", step_id, "some_tool", HASH_A, max_attempts=3)
        store.complete_step("run-1", step_id, claimed.attempt_token, result_json="{}")

    events = store.list_events("run-1")
    sequences = [event.sequence for event in events]
    assert sequences == sorted(sequences)
    assert sequences == list(range(1, len(sequences) + 1))


def test_claim_step_on_missing_execution_raises_key_error(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")

    with pytest.raises(KeyError, match="missing-run"):
        store.claim_step("missing-run", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)

    assert store.list_steps("missing-run") == []
    assert store.list_events("missing-run") == []
    assert store.get_execution("missing-run") is None


def test_completed_step_event_payload_reflects_persisted_attempt(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    claimed = store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)
    store.complete_step("run-1", "load_manifest", claimed.attempt_token, result_json='{"ok": true}')

    events = store.list_events("run-1")
    completed_event = next(event for event in events if event.event_type == "step.completed")
    assert completed_event.payload == {
        "step_id": "load_manifest",
        "tool_name": "load_release_manifest",
        "attempt_count": 1,
        "error_code": None,
        "outcome": "completed",
    }


def test_failed_step_event_payload_reflects_persisted_attempt(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    claimed = store.claim_step("run-1", "unit_tests", "run_unit_test_check", HASH_A, max_attempts=3)
    store.fail_step("run-1", "unit_tests", claimed.attempt_token, error_code="transient_test_failure")

    events = store.list_events("run-1")
    failed_event = next(event for event in events if event.event_type == "step.failed")
    assert failed_event.payload == {
        "step_id": "unit_tests",
        "tool_name": "run_unit_test_check",
        "attempt_count": 1,
        "error_code": "transient_test_failure",
        "outcome": "failed",
    }


def test_finalized_event_captures_attempt_count_at_the_time_not_later(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)

    first = store.claim_step("run-1", "unit_tests", "run_unit_test_check", HASH_A, max_attempts=3)
    store.fail_step("run-1", "unit_tests", first.attempt_token, error_code="transient_test_failure")

    second = store.claim_step("run-1", "unit_tests", "run_unit_test_check", HASH_A, max_attempts=3)
    store.complete_step("run-1", "unit_tests", second.attempt_token, result_json='{"ok": true}')

    events = store.list_events("run-1")
    failed_events = [event for event in events if event.event_type == "step.failed"]
    completed_events = [event for event in events if event.event_type == "step.completed"]

    assert len(failed_events) == 1
    assert failed_events[0].payload["attempt_count"] == 1
    assert len(completed_events) == 1
    assert completed_events[0].payload["attempt_count"] == 2

    # The persisted row has since moved on to attempt 2, but the earlier
    # failure event must still show the attempt count as it was when that
    # attempt actually failed.
    current_step = store.get_step("run-1", "unit_tests")
    assert current_step.attempt_count == 2
    assert failed_events[0].payload["attempt_count"] != current_step.attempt_count


@pytest.mark.parametrize(
    "make_no_op_outcome",
    [
        "already_running",
        "cached",
        "input_mismatch",
        "definition_mismatch",
        "attempts_exhausted",
    ],
)
def test_no_op_claim_outcomes_do_not_append_events(tmp_path, make_no_op_outcome):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    store.create_or_get_execution("run-1", "release_validation", HASH_A)

    if make_no_op_outcome == "already_running":
        store.claim_step("run-1", "step_x", "some_tool", HASH_A, max_attempts=3)
        before = len(store.list_events("run-1"))
        result = store.claim_step("run-1", "step_x", "some_tool", HASH_A, max_attempts=3)
    elif make_no_op_outcome == "cached":
        claimed = store.claim_step("run-1", "step_x", "some_tool", HASH_A, max_attempts=3)
        store.complete_step("run-1", "step_x", claimed.attempt_token, result_json="{}")
        before = len(store.list_events("run-1"))
        result = store.claim_step("run-1", "step_x", "some_tool", HASH_A, max_attempts=3)
    elif make_no_op_outcome == "input_mismatch":
        claimed = store.claim_step("run-1", "step_x", "some_tool", HASH_A, max_attempts=3)
        store.complete_step("run-1", "step_x", claimed.attempt_token, result_json="{}")
        before = len(store.list_events("run-1"))
        result = store.claim_step("run-1", "step_x", "some_tool", HASH_B, max_attempts=3)
    elif make_no_op_outcome == "definition_mismatch":
        claimed = store.claim_step("run-1", "step_x", "old_tool", HASH_A, max_attempts=3)
        store.complete_step("run-1", "step_x", claimed.attempt_token, result_json="{}")
        before = len(store.list_events("run-1"))
        result = store.claim_step("run-1", "step_x", "new_tool", HASH_A, max_attempts=3)
    else:
        claimed = store.claim_step("run-1", "step_x", "some_tool", HASH_A, max_attempts=1)
        store.fail_step("run-1", "step_x", claimed.attempt_token, error_code="boom")
        before = len(store.list_events("run-1"))
        result = store.claim_step("run-1", "step_x", "some_tool", HASH_A, max_attempts=1)

    assert result.outcome == ClaimOutcome(make_no_op_outcome)
    after_events = store.list_events("run-1")
    assert len(after_events) == before


def test_store_survives_reopen_of_same_database_file(tmp_path):
    database_path = tmp_path / "workflow.db"
    store = SQLiteWorkflowStore(database_path)
    store.create_or_get_execution("run-1", "release_validation", HASH_A)
    store.mark_running("run-1")
    claimed = store.claim_step("run-1", "load_manifest", "load_release_manifest", HASH_A, max_attempts=3)
    store.complete_step("run-1", "load_manifest", claimed.attempt_token, result_json='{"ok": true}')
    store.finalize_ready("run-1", result_json='{"status": "ready"}')

    reopened = SQLiteWorkflowStore(database_path)
    execution = reopened.get_execution("run-1")
    assert execution is not None
    assert execution.status == WorkflowStatus.READY

    step = reopened.get_step("run-1", "load_manifest")
    assert step is not None
    assert step.status == ToolCallStatus.COMPLETED
    assert step.result_json == '{"ok": true}'

    assert [event.event_type for event in reopened.list_events("run-1")] == [
        "workflow.started",
        "step.claimed",
        "step.completed",
        "workflow.ready",
    ]
