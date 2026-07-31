import asyncio

from domains.travel.review.evidence import ContextProjector, PlanEvidenceBuilder
from domains.travel.review.models import (
    CostLedgerStatus,
    EvidenceRef,
    EvidenceSourceType,
    FindingBasis,
    FindingSeverity,
    FindingType,
    FindingVerdict,
    PreferenceReviewContext,
    ReviewerReport,
    ReviewerRole,
    ReviewFinding,
    TaskStatus,
    WorkflowStatus,
)
from domains.travel.review.orchestrator import ReviewWorkflowConfig, WorkflowOrchestrator
from domains.travel.review.reviewers import BudgetChecker, PreferenceReviewer, RetryableReviewerError
from domains.travel.runtime import TravelAgentRuntime
from domains.travel.state import AgentState, TravelPlan
from runtime_service.registry import build_default_registry


def review_state(
    *,
    total_cost: int = 7600,
    budget: int = 7000,
    preferences: dict | None = None,
    memory_refs: list[dict] | None = None,
    ledger_total: int | float | None = None,
    include_cost_breakdown: bool = True,
) -> AgentState:
    ledger_total = total_cost if ledger_total is None else ledger_total
    activity_cost_per_day = (ledger_total - 1800 - 5 * 650) / 5
    tool_outputs: dict[str, object] = {"memory_refs": memory_refs or []}
    if include_cost_breakdown:
        tool_outputs["cost_breakdown"] = {
            "flight_cost": 1800,
            "hotel_cost_per_day": 650,
            "activity_cost_per_day": activity_cost_per_day,
        }
    return AgentState(
        thread_id="review-test",
        destination="Tokyo",
        days=5,
        budget=budget,
        preferences=preferences or {},
        itinerary=TravelPlan(
            destination="Tokyo",
            days=5,
            flight_type="red_eye",
            hotel_tier="standard hotel",
            poi_style="balanced itinerary",
            total_cost=total_cost,
        ),
        tool_outputs=tool_outputs,
    )


def test_plan_evidence_is_built_once_then_projected_by_role():
    state = review_state(
        preferences={"avoid_red_eye": True},
        memory_refs=[
            {"memory_id": "memory-1", "key": "travel_style", "value": "relaxed"}
        ],
    )
    evidence = PlanEvidenceBuilder().build(state)
    projector = ContextProjector()

    budget = projector.project(
        evidence,
        ReviewerRole.BUDGET,
        workflow_run_id="workflow-1",
        task_id="budget-1",
    )
    preference = projector.project(
        evidence,
        ReviewerRole.PREFERENCE,
        workflow_run_id="workflow-1",
        task_id="preference-1",
    )

    assert sum(item.amount for item in evidence.cost_ledger) == 7600
    assert evidence.cost_ledger_status == CostLedgerStatus.COMPLETE
    assert evidence.evidence_issues == []
    assert not hasattr(budget, "explicit_preferences")
    assert not hasattr(budget, "relevant_memory")
    assert preference.explicit_preferences == {"avoid_red_eye": True}
    assert preference.relevant_memory[0].memory_id == "memory-1"


def test_budget_checker_uses_complete_ledger_and_records_total_mismatch():
    result = WorkflowOrchestrator().run_sync(
        review_state(total_cost=6500, budget=7000, ledger_total=7600)
    )

    assert [finding.finding_type for finding in result.findings] == [
        FindingType.BUDGET_OVERRUN
    ]
    assert result.evidence_issues == [
        "cost_total_mismatch:plan_total=6500,ledger_total=7600"
    ]
    budget_finding = result.findings[0]
    computed_total = next(
        evidence
        for evidence in budget_finding.evidence
        if evidence.field == "computed_total"
    )
    assert computed_total.observed == 7600


def test_budget_checker_does_not_report_overrun_when_complete_ledger_is_within_budget():
    result = WorkflowOrchestrator().run_sync(
        review_state(total_cost=7600, budget=7000, ledger_total=6500)
    )

    assert not any(
        finding.finding_type == FindingType.BUDGET_OVERRUN
        for finding in result.findings
    )
    assert result.evidence_issues == [
        "cost_total_mismatch:plan_total=7600,ledger_total=6500"
    ]


def test_budget_equal_to_limit_is_not_an_overrun():
    result = WorkflowOrchestrator().run_sync(
        review_state(total_cost=7000, budget=7000)
    )

    assert not any(
        finding.finding_type == FindingType.BUDGET_OVERRUN
        for finding in result.findings
    )
    assert result.evidence_issues == []


def test_missing_cost_breakdown_uses_degraded_plan_total_without_false_mismatch():
    state = review_state(
        total_cost=7600,
        budget=7000,
        include_cost_breakdown=False,
    )
    evidence = PlanEvidenceBuilder().build(state)
    result = WorkflowOrchestrator().run_sync(state)

    assert evidence.cost_ledger_status == CostLedgerStatus.FALLBACK
    assert evidence.evidence_issues == ["cost_breakdown_missing_using_plan_total"]
    assert not any(
        issue.startswith("cost_total_mismatch") for issue in result.evidence_issues
    )
    assert any(
        finding.finding_type == FindingType.BUDGET_OVERRUN
        for finding in result.findings
    )


def test_reviewers_return_structured_evidence_and_coverage():
    state = review_state(
        preferences={
            "avoid_red_eye": True,
            "hotel_near_subway": True,
            "travel_style": "relaxed",
        }
    )
    result = WorkflowOrchestrator().run_sync(state)

    assert result.status == WorkflowStatus.COMPLETED
    assert {task.status for task in result.tasks} == {TaskStatus.COMPLETED}
    assert {finding.finding_type for finding in result.findings} == {
        FindingType.BUDGET_OVERRUN,
        FindingType.RED_EYE_PREFERENCE_VIOLATION,
        FindingType.HOTEL_LOCATION_PREFERENCE_VIOLATION,
        FindingType.TRAVEL_STYLE_PREFERENCE_VIOLATION,
    }
    assert all(finding.evidence for finding in result.findings)
    assert all(
        isinstance(evidence, EvidenceRef)
        for finding in result.findings
        for evidence in finding.evidence
    )

    preference_report = next(
        task.output for task in result.tasks if task.reviewer == ReviewerRole.PREFERENCE
    )
    assert preference_report is not None
    assert "preference.avoid_red_eye" in preference_report.checked_rule_ids
    assert {
        skipped.rule_id for skipped in preference_report.skipped_checks
    } == {
        "preference.long_term_memory_alignment",
        "preference.semantic_alignment",
    }
    assert not preference_report.has_required_coverage_gap


class SlowPreferenceReviewer:
    role = ReviewerRole.PREFERENCE

    async def run(self, context):
        await asyncio.sleep(0.05)
        return ReviewerReport(reviewer=self.role, task_id=context.task_id)


def test_workflow_keeps_completed_results_when_one_reviewer_times_out():
    orchestrator = WorkflowOrchestrator(
        reviewers=[BudgetChecker(), SlowPreferenceReviewer()],
        config=ReviewWorkflowConfig(
            max_concurrency=2,
            workflow_timeout_seconds=0.03,
            task_timeout_seconds=0.01,
            max_attempts=1,
        ),
    )

    result = orchestrator.run_sync(review_state())

    assert result.status == WorkflowStatus.COMPLETED_PARTIAL
    statuses = {task.reviewer: task.status for task in result.tasks}
    assert statuses[ReviewerRole.BUDGET] == TaskStatus.COMPLETED
    assert statuses[ReviewerRole.PREFERENCE] == TaskStatus.TIMED_OUT
    assert [finding.finding_type for finding in result.findings] == [
        FindingType.BUDGET_OVERRUN
    ]


class BlockingPreferenceReviewer:
    """A controllable stand-in for a slow reviewer, driven by events instead
    of a wall-clock sleep duration.

    It signals `started` the instant it begins running -- proving the task
    is genuinely in flight, not merely scheduled -- then suspends on
    `release`, which the test deliberately never sets. The only way this
    reviewer's task can resolve is by being cancelled, so a test built on
    it does not depend on any particular sleep duration racing against
    scheduler jitter.
    """

    role = ReviewerRole.PREFERENCE

    def __init__(self, started: asyncio.Event, release: asyncio.Event):
        self.started = started
        self.release = release

    async def run(self, context):
        self.started.set()
        await self.release.wait()
        return ReviewerReport(reviewer=self.role, task_id=context.task_id)  # pragma: no cover


def test_workflow_cancellation_preserves_completed_findings_without_replan_directives():
    started = asyncio.Event()
    release = asyncio.Event()
    orchestrator = WorkflowOrchestrator(
        reviewers=[BudgetChecker(), BlockingPreferenceReviewer(started, release)],
        config=ReviewWorkflowConfig(
            max_concurrency=2,
            workflow_timeout_seconds=1.0,
            task_timeout_seconds=1.0,
            max_attempts=1,
        ),
    )

    async def cancel_once_preference_reviewer_is_blocked():
        task = asyncio.create_task(orchestrator.run(review_state()))
        # Deterministically wait for the slow reviewer to actually start
        # running -- not for a guessed sleep duration to probably have
        # elapsed relative to however fast this machine's event loop
        # happens to schedule things.
        await asyncio.wait_for(started.wait(), timeout=5.0)
        # `release` is deliberately never set: the preference reviewer is
        # provably still blocked here, so cancelling now can only resolve
        # it through cancellation, never through natural completion.
        task.cancel()
        return await task

    result = asyncio.run(cancel_once_preference_reviewer_is_blocked())

    assert result.status == WorkflowStatus.CANCELLED
    statuses = {task.reviewer: task.status for task in result.tasks}
    # Budget has no internal await point, so by the time the blocking
    # reviewer has signalled `started`, budget has already run to
    # completion on its own -- this assertion is the authoritative proof,
    # not an assumption.
    assert statuses[ReviewerRole.BUDGET] == TaskStatus.COMPLETED
    assert statuses[ReviewerRole.PREFERENCE] == TaskStatus.CANCELLED
    assert [finding.finding_type for finding in result.findings] == [
        FindingType.BUDGET_OVERRUN
    ]
    assert result.directives == []


class FlakyPreferenceReviewer:
    role = ReviewerRole.PREFERENCE

    def __init__(self):
        self.calls = 0

    async def run(self, context):
        self.calls += 1
        if self.calls == 1:
            raise RetryableReviewerError("temporary model queue failure")
        return ReviewerReport(reviewer=self.role, task_id=context.task_id)


def test_retryable_failure_retries_within_workflow_budget():
    reviewer = FlakyPreferenceReviewer()
    orchestrator = WorkflowOrchestrator(
        reviewers=[reviewer],
        config=ReviewWorkflowConfig(
            workflow_timeout_seconds=0.2,
            task_timeout_seconds=0.1,
            max_attempts=2,
        ),
    )

    result = orchestrator.run_sync(review_state())

    assert result.status == WorkflowStatus.COMPLETED
    assert reviewer.calls == 2
    assert result.tasks[0].attempts == 2


def test_semantic_analyzer_is_injectable_but_not_required_for_offline_ci():
    async def semantic_analyzer(context: PreferenceReviewContext):
        assert context.candidate_plan is not None
        return [
            ReviewFinding(
                finding_id="semantic-finding",
                task_id=context.task_id,
                reviewer=ReviewerRole.PREFERENCE,
                rule_id="preference.semantic_alignment",
                finding_type=FindingType.TRAVEL_STYLE_PREFERENCE_VIOLATION,
                basis=FindingBasis.LLM_SEMANTIC,
                verdict=FindingVerdict.PLAUSIBLE,
                severity=FindingSeverity.LOW,
                summary="The plan may be too dense for the user's wording.",
                evidence=[
                    EvidenceRef(
                        source_type=EvidenceSourceType.PLAN_ITEM,
                        source_id=context.candidate_plan.plan_item_id,
                        field="poi_style",
                        observed=context.candidate_plan.poi_style,
                        expected="less dense",
                    )
                ],
                affected_plan_item_ids=[context.candidate_plan.plan_item_id],
            )
        ]

    orchestrator = WorkflowOrchestrator(
        reviewers=[PreferenceReviewer(semantic_analyzer=semantic_analyzer)]
    )
    result = orchestrator.run_sync(review_state(total_cost=6000))

    assert result.status == WorkflowStatus.COMPLETED
    assert result.findings[0].basis == FindingBasis.LLM_SEMANTIC
    assert result.directives == []
    assert result.tasks[0].output is not None
    assert "preference.semantic_alignment" in result.tasks[0].output.checked_rule_ids


def test_feature_flagged_runtime_repairs_budget_then_keeps_final_validator_gate():
    state = AgentState(thread_id="feature-flag")

    baseline = TravelAgentRuntime().handle_user_message(
        state,
        "I want a 5-day Tokyo trip under 7000 SGD.",
    )
    reviewed = TravelAgentRuntime(enable_review_workflow=True).handle_user_message(
        state,
        "I want a 5-day Tokyo trip under 7000 SGD.",
    )

    assert baseline.state.itinerary is not None
    assert baseline.state.itinerary.total_cost == 7300
    assert "cost_breakdown" not in baseline.state.tool_outputs
    assert baseline.validation_errors
    assert reviewed.state.itinerary is not None
    assert reviewed.state.itinerary.total_cost == 5800
    assert reviewed.validation_errors == []
    assert any(
        event.event == "review_workflow_finished" and event.reason == "completed"
        for event in reviewed.state.execution_trace
    )
    assert reviewed.state.execution_trace[-1].event == "validation_finished"


def test_review_workflow_does_not_run_for_unactionable_message_with_existing_plan():
    runtime = TravelAgentRuntime(enable_review_workflow=True)
    planned = runtime.handle_user_message(
        AgentState(thread_id="clarification"),
        "I want a 5-day Tokyo trip under 7000 SGD.",
    )
    workflow_count = sum(
        event.event == "review_workflow_started"
        for event in planned.state.execution_trace
    )

    clarified = runtime.handle_user_message(planned.state, "Tell me something unrelated.")

    assert clarified.state.blockers == ["Could not identify actionable travel constraints."]
    assert sum(
        event.event == "review_workflow_started"
        for event in clarified.state.execution_trace
    ) == workflow_count


def test_review_agent_version_is_opt_in_and_baseline_version_is_preserved():
    registry = build_default_registry()
    versions = {descriptor.version for descriptor in registry.list_agents()}

    assert versions == {"0.3.0", "0.5.0"}
    reviewed = registry.resolve("travel-agent", "0.5.0").handle_user_message(
        AgentState(thread_id="versioned-review"),
        "I want a 5-day Tokyo trip under 7000 SGD.",
    )
    assert reviewed.state.itinerary is not None
    assert reviewed.state.itinerary.total_cost == 5800
