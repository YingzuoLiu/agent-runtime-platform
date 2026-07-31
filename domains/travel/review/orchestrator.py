from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from uuid import uuid4

from agent.contracts import utc_now

from ..state import AgentState

from .evidence import ContextProjector, PlanEvidenceBuilder
from .models import (
    ReviewContext,
    SubtaskResult,
    TaskStatus,
    WorkflowReviewResult,
    WorkflowStatus,
)
from .reducer import FindingReducer
from .reviewers import (
    BudgetChecker,
    PreferenceReviewer,
    RetryableReviewerError,
    Reviewer,
    ReviewerBlocked,
)


@dataclass(frozen=True)
class ReviewWorkflowConfig:
    max_concurrency: int = 2
    workflow_timeout_seconds: float = 5.0
    task_timeout_seconds: float = 2.0
    max_attempts: int = 2

    def __post_init__(self) -> None:
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        if self.workflow_timeout_seconds <= 0:
            raise ValueError("workflow_timeout_seconds must be positive")
        if self.task_timeout_seconds <= 0:
            raise ValueError("task_timeout_seconds must be positive")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")


class WorkflowOrchestrator:
    """Run isolated reviewers concurrently within one absolute workflow deadline."""

    def __init__(
        self,
        *,
        reviewers: list[Reviewer] | None = None,
        config: ReviewWorkflowConfig | None = None,
        evidence_builder: PlanEvidenceBuilder | None = None,
        context_projector: ContextProjector | None = None,
        reducer: FindingReducer | None = None,
    ) -> None:
        self.reviewers = reviewers or [BudgetChecker(), PreferenceReviewer()]
        self.config = config or ReviewWorkflowConfig()
        self.evidence_builder = evidence_builder or PlanEvidenceBuilder()
        self.context_projector = context_projector or ContextProjector()
        self.reducer = reducer or FindingReducer()

        roles = [reviewer.role for reviewer in self.reviewers]
        if len(roles) != len(set(roles)):
            raise ValueError("Only one reviewer per role is allowed in a workflow")

    async def run(self, state: AgentState) -> WorkflowReviewResult:
        loop = asyncio.get_running_loop()
        started_monotonic = loop.time()
        started_at = utc_now()
        deadline = started_monotonic + self.config.workflow_timeout_seconds
        workflow_run_id = f"workflow_{uuid4().hex}"
        evidence = self.evidence_builder.build(state)
        semaphore = asyncio.Semaphore(self.config.max_concurrency)

        pending: list[asyncio.Task[SubtaskResult]] = []
        for reviewer in self.reviewers:
            task_id = f"review_task_{uuid4().hex}"
            context = self.context_projector.project(
                evidence,
                reviewer.role,
                workflow_run_id=workflow_run_id,
                task_id=task_id,
            )
            pending.append(
                asyncio.create_task(
                    self._run_reviewer(
                        reviewer,
                        context,
                        deadline=deadline,
                        semaphore=semaphore,
                    )
                )
            )

        try:
            tasks = await asyncio.gather(*pending)
        except asyncio.CancelledError:
            for task in pending:
                if not task.done():
                    task.cancel()
            settled = await asyncio.gather(*pending, return_exceptions=True)
            tasks = [result for result in settled if isinstance(result, SubtaskResult)]
            completed_reports = [
                task.output
                for task in tasks
                if task.status == TaskStatus.COMPLETED and task.output is not None
            ]
            reduced = self.reducer.reduce(completed_reports)
            finished_at = utc_now()
            return WorkflowReviewResult(
                workflow_run_id=workflow_run_id,
                status=WorkflowStatus.CANCELLED,
                tasks=tasks,
                evidence_issues=evidence.evidence_issues,
                findings=reduced.findings,
                directives=[],
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=(loop.time() - started_monotonic) * 1000,
            )

        completed_reports = [
            task.output
            for task in tasks
            if task.status == TaskStatus.COMPLETED and task.output is not None
        ]
        reduced = self.reducer.reduce(completed_reports)
        status = self._workflow_status(tasks)
        return WorkflowReviewResult(
            workflow_run_id=workflow_run_id,
            status=status,
            tasks=tasks,
            evidence_issues=evidence.evidence_issues,
            findings=reduced.findings,
            directives=reduced.directives,
            started_at=started_at,
            finished_at=utc_now(),
            duration_ms=(loop.time() - started_monotonic) * 1000,
        )

    def run_sync(self, state: AgentState) -> WorkflowReviewResult:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.run(state))
        raise RuntimeError("run_sync cannot be called from an active event loop; await run instead")

    async def _run_reviewer(
        self,
        reviewer: Reviewer,
        context: ReviewContext,
        *,
        deadline: float,
        semaphore: asyncio.Semaphore,
    ) -> SubtaskResult:
        loop = asyncio.get_running_loop()
        started_monotonic = loop.time()
        started_at = utc_now()
        attempts = 0

        while attempts < self.config.max_attempts:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return self._result(
                    context,
                    TaskStatus.TIMED_OUT,
                    attempts,
                    started_at,
                    started_monotonic,
                    error_code="workflow_deadline_exceeded",
                    error_message="No workflow time remained before the reviewer could run.",
                )
            attempts += 1

            acquired = False
            try:
                await asyncio.wait_for(semaphore.acquire(), timeout=remaining)
                acquired = True
                remaining = deadline - loop.time()
                timeout = min(self.config.task_timeout_seconds, remaining)
                if timeout <= 0:
                    raise TimeoutError
                report = await asyncio.wait_for(reviewer.run(context), timeout=timeout)
                if report.reviewer != reviewer.role or report.task_id != context.task_id:
                    raise ValueError("Reviewer report identity does not match its task context")
                return self._result(
                    context,
                    TaskStatus.COMPLETED,
                    attempts,
                    started_at,
                    started_monotonic,
                    output=report,
                )
            except ReviewerBlocked as exc:
                return self._result(
                    context,
                    TaskStatus.BLOCKED,
                    attempts,
                    started_at,
                    started_monotonic,
                    error_code="missing_required_evidence",
                    error_message=str(exc),
                )
            except (TimeoutError, asyncio.TimeoutError):
                if attempts >= self.config.max_attempts or deadline - loop.time() <= 0:
                    return self._result(
                        context,
                        TaskStatus.TIMED_OUT,
                        attempts,
                        started_at,
                        started_monotonic,
                        error_code="reviewer_timeout",
                        error_message="Reviewer did not finish within the available deadline.",
                    )
            except RetryableReviewerError as exc:
                if attempts >= self.config.max_attempts or deadline - loop.time() <= 0:
                    return self._result(
                        context,
                        TaskStatus.FAILED,
                        attempts,
                        started_at,
                        started_monotonic,
                        error_code="retryable_failure_exhausted",
                        error_message=str(exc),
                    )
            except asyncio.CancelledError:
                return self._result(
                    context,
                    TaskStatus.CANCELLED,
                    attempts,
                    started_at,
                    started_monotonic,
                    error_code="reviewer_cancelled",
                    error_message="Reviewer task was cancelled.",
                )
            except Exception as exc:
                return self._result(
                    context,
                    TaskStatus.FAILED,
                    attempts,
                    started_at,
                    started_monotonic,
                    error_code=type(exc).__name__,
                    error_message=str(exc),
                )
            finally:
                if acquired:
                    semaphore.release()

        raise AssertionError("reviewer attempt loop exited unexpectedly")

    @staticmethod
    def _result(
        context: ReviewContext,
        status: TaskStatus,
        attempts: int,
        started_at: str,
        started_monotonic: float,
        *,
        output=None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> SubtaskResult:
        return SubtaskResult(
            task_id=context.task_id,
            reviewer=context.reviewer,
            status=status,
            attempts=attempts,
            output=output,
            error_code=error_code,
            error_message=error_message,
            started_at=started_at,
            finished_at=utc_now(),
            duration_ms=(time.monotonic() - started_monotonic) * 1000,
        )

    @staticmethod
    def _workflow_status(tasks: list[SubtaskResult]) -> WorkflowStatus:
        if tasks and all(task.status == TaskStatus.CANCELLED for task in tasks):
            return WorkflowStatus.CANCELLED
        completed = [task for task in tasks if task.status == TaskStatus.COMPLETED]
        if not completed:
            return WorkflowStatus.FAILED
        has_incomplete_task = len(completed) != len(tasks)
        has_required_coverage_gap = any(
            task.output is not None and task.output.has_required_coverage_gap
            for task in completed
        )
        if has_incomplete_task or has_required_coverage_gap:
            return WorkflowStatus.COMPLETED_PARTIAL
        return WorkflowStatus.COMPLETED
