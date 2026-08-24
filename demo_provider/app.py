from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import time
from collections.abc import Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal, cast

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from runtime_service.external_actions import ExternalActionProviderResult, ExternalActionRequest


Scenario = Literal["idempotent", "unsafe", "known_success", "known_failure"]
_SCENARIOS: frozenset[str] = frozenset(
    {"idempotent", "unsafe", "known_success", "known_failure"}
)


class ProviderProofEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    event_sequence: int
    event_type: str


class ProviderProofState(BaseModel):
    """Sanitized sidecar evidence exposed only by the loopback proof service."""

    model_config = ConfigDict(extra="forbid", strict=True)

    action_id: str
    scenario: Scenario
    attempt_count: int
    effect_count: int
    request_identity_count: int
    idempotency_identity_count: int
    provider_reference: str | None
    waiting_for_release: bool
    events: list[ProviderProofEvent]


@dataclass(frozen=True)
class DispatchDecision:
    attempt_sequence: int
    provider_reference: str | None = None
    wait_for_release: bool = False
    key_conflict: bool = False
    definitive_failure: bool = False


class ProviderProofLedger:
    """Provider-owned effect and event ledger for the local recovery proof.

    Only hashes of request/key material are stored. The proof API projects
    counts, references, and provider-owned event order; it never returns the
    request body, either idempotency key, tenant/subject identity, or credentials.
    """

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connection() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = FULL;

                CREATE TABLE IF NOT EXISTS provider_events (
                    event_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    scenario TEXT NOT NULL,
                    event_type TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS provider_effects (
                    effect_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    scenario TEXT NOT NULL,
                    key_digest TEXT NOT NULL,
                    request_digest TEXT NOT NULL,
                    provider_reference TEXT NOT NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS provider_idempotent_effect
                    ON provider_effects (scenario, key_digest)
                    WHERE scenario = 'idempotent';

                CREATE TABLE IF NOT EXISTS provider_attempts (
                    attempt_sequence INTEGER PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    request_digest TEXT NOT NULL,
                    key_digest TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS held_attempts (
                    run_id TEXT NOT NULL,
                    attempt_sequence INTEGER NOT NULL,
                    release_requested INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (run_id, attempt_sequence)
                );
                """
            )

    @staticmethod
    def _digest(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    @staticmethod
    def _request_digest(provider_request: ExternalActionRequest) -> str:
        canonical = json.dumps(
            provider_request.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return ProviderProofLedger._digest(canonical)

    @staticmethod
    def _record_event(
        connection: sqlite3.Connection,
        *,
        run_id: str,
        scenario: Scenario,
        event_type: str,
    ) -> int:
        cursor = connection.execute(
            """
            INSERT INTO provider_events (run_id, scenario, event_type)
            VALUES (?, ?, ?)
            """,
            (run_id, scenario, event_type),
        )
        if cursor.lastrowid is None:  # pragma: no cover - SQLite guarantees it here
            raise RuntimeError("provider event sequence was not assigned")
        return int(cursor.lastrowid)

    def begin_dispatch(
        self,
        *,
        scenario: Scenario,
        provider_request: ExternalActionRequest,
    ) -> DispatchDecision:
        if scenario not in _SCENARIOS:  # pragma: no cover - typed routes constrain this
            raise ValueError("unsupported proof scenario")
        key_digest = self._digest(provider_request.idempotency_key)
        request_digest = self._request_digest(provider_request)
        run_id = provider_request.run_id

        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            attempt_sequence = self._record_event(
                connection,
                run_id=run_id,
                scenario=scenario,
                event_type="attempt.received",
            )
            connection.execute(
                """
                INSERT INTO provider_attempts (
                    attempt_sequence, run_id, request_digest, key_digest
                ) VALUES (?, ?, ?, ?)
                """,
                (attempt_sequence, run_id, request_digest, key_digest),
            )
            if scenario == "idempotent":
                existing = connection.execute(
                    """
                    SELECT request_digest, provider_reference
                    FROM provider_effects
                    WHERE scenario = ? AND key_digest = ?
                    """,
                    (scenario, key_digest),
                ).fetchone()
                if existing is not None:
                    if existing["request_digest"] != request_digest:
                        self._record_event(
                            connection,
                            run_id=run_id,
                            scenario=scenario,
                            event_type="request.conflict",
                        )
                        connection.commit()
                        return DispatchDecision(
                            attempt_sequence=attempt_sequence,
                            key_conflict=True,
                        )
                    self._record_event(
                        connection,
                        run_id=run_id,
                        scenario=scenario,
                        event_type="receipt.replayed",
                    )
                    connection.commit()
                    return DispatchDecision(
                        attempt_sequence=attempt_sequence,
                        provider_reference=str(existing["provider_reference"]),
                    )

            if scenario == "known_failure":
                self._record_event(
                    connection,
                    run_id=run_id,
                    scenario=scenario,
                    event_type="failure.definitive",
                )
                connection.commit()
                return DispatchDecision(
                    attempt_sequence=attempt_sequence,
                    definitive_failure=True,
                )

            provider_reference = f"delivery_{secrets.token_hex(8)}"
            connection.execute(
                """
                INSERT INTO provider_effects (
                    run_id,
                    scenario,
                    key_digest,
                    request_digest,
                    provider_reference
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    scenario,
                    key_digest,
                    request_digest,
                    provider_reference,
                ),
            )
            self._record_event(
                connection,
                run_id=run_id,
                scenario=scenario,
                event_type="effect.committed",
            )
            if scenario == "known_success":
                self._record_event(
                    connection,
                    run_id=run_id,
                    scenario=scenario,
                    event_type="response.success",
                )
                connection.commit()
                return DispatchDecision(
                    attempt_sequence=attempt_sequence,
                    provider_reference=provider_reference,
                )
            connection.execute(
                """
                INSERT INTO held_attempts (run_id, attempt_sequence, release_requested)
                VALUES (?, ?, 0)
                """,
                (run_id, attempt_sequence),
            )
            connection.commit()
            return DispatchDecision(
                attempt_sequence=attempt_sequence,
                provider_reference=provider_reference,
                wait_for_release=True,
            )

    def release(self, run_id: str) -> bool:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            held = connection.execute(
                """
                SELECT attempt_sequence
                FROM held_attempts
                WHERE run_id = ? AND release_requested = 0
                ORDER BY attempt_sequence DESC
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
            if held is None:
                connection.rollback()
                return False
            scenario_row = connection.execute(
                """
                SELECT scenario
                FROM provider_events
                WHERE run_id = ? AND event_sequence = ?
                """,
                (run_id, int(held["attempt_sequence"])),
            ).fetchone()
            if scenario_row is None:  # pragma: no cover - protected by ledger writes
                connection.rollback()
                raise RuntimeError("held attempt has no provider event")
            scenario = str(scenario_row["scenario"])
            if scenario not in _SCENARIOS:  # pragma: no cover - writer constrains it
                connection.rollback()
                raise RuntimeError("held attempt has an unknown scenario")
            connection.execute(
                """
                UPDATE held_attempts
                SET release_requested = 1
                WHERE run_id = ? AND attempt_sequence = ?
                """,
                (run_id, int(held["attempt_sequence"])),
            )
            self._record_event(
                connection,
                run_id=run_id,
                scenario=cast(Scenario, scenario),
                event_type="fault.release_requested",
            )
            connection.commit()
            return True

    def wait_for_release(
        self,
        *,
        run_id: str,
        scenario: Scenario,
        attempt_sequence: int,
        timeout_seconds: float = 45,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        released = False
        while time.monotonic() < deadline:
            with self._connection() as connection:
                row = connection.execute(
                    """
                    SELECT release_requested
                    FROM held_attempts
                    WHERE run_id = ? AND attempt_sequence = ?
                    """,
                    (run_id, attempt_sequence),
                ).fetchone()
            if row is not None and int(row["release_requested"]) == 1:
                released = True
                break
            time.sleep(0.02)

        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                DELETE FROM held_attempts
                WHERE run_id = ? AND attempt_sequence = ?
                """,
                (run_id, attempt_sequence),
            )
            self._record_event(
                connection,
                run_id=run_id,
                scenario=scenario,
                event_type=("response.ambiguous" if released else "fault.release_timeout"),
            )
            connection.commit()

    def snapshot(self, run_id: str) -> ProviderProofState | None:
        with self._connection() as connection:
            scenario_rows = connection.execute(
                """
                SELECT DISTINCT scenario
                FROM provider_events
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchall()
            if not scenario_rows:
                return None
            if len(scenario_rows) != 1:
                raise RuntimeError("one proof action crossed provider scenarios")
            scenario = str(scenario_rows[0]["scenario"])
            if scenario not in _SCENARIOS:  # pragma: no cover - writer constrains it
                raise RuntimeError("provider ledger contains an unknown scenario")
            attempts = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM provider_events
                WHERE run_id = ? AND event_type = 'attempt.received'
                """,
                (run_id,),
            ).fetchone()
            identities = connection.execute(
                """
                SELECT
                    COUNT(DISTINCT request_digest) AS request_count,
                    COUNT(DISTINCT key_digest) AS key_count
                FROM provider_attempts
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            effects = connection.execute(
                """
                SELECT COUNT(*) AS count, MIN(provider_reference) AS provider_reference
                FROM provider_effects
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            waiting = connection.execute(
                """
                SELECT 1
                FROM held_attempts
                WHERE run_id = ? AND release_requested = 0
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
            event_rows = connection.execute(
                """
                SELECT event_sequence, event_type
                FROM provider_events
                WHERE run_id = ?
                ORDER BY event_sequence
                """,
                (run_id,),
            ).fetchall()

        if (
            attempts is None or effects is None or identities is None
        ):  # pragma: no cover - aggregate rows always exist
            raise RuntimeError("provider proof aggregates are missing")
        return ProviderProofState(
            action_id=run_id,
            scenario=cast(Scenario, scenario),
            attempt_count=int(attempts["count"]),
            effect_count=int(effects["count"]),
            request_identity_count=int(identities["request_count"]),
            idempotency_identity_count=int(identities["key_count"]),
            provider_reference=(
                str(effects["provider_reference"])
                if effects["provider_reference"] is not None
                else None
            ),
            waiting_for_release=waiting is not None,
            events=[
                ProviderProofEvent(
                    event_sequence=int(row["event_sequence"]),
                    event_type=str(row["event_type"]),
                )
                for row in event_rows
            ],
        )


def create_demo_provider_app(database_path: str | Path | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(provider_app: FastAPI):
        resolved_path = (
            database_path
            if database_path is not None
            else os.getenv("DEMO_PROVIDER_DB_PATH", "provider_data/provider.db")
        )
        provider_app.state.proof_ledger = ProviderProofLedger(resolved_path)
        yield

    provider_app = FastAPI(
        title="Action Gateway Demo Provider",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    @provider_app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    def dispatch(
        *,
        scenario: Scenario,
        provider_request: ExternalActionRequest,
        header_key: str,
        request: Request,
    ) -> JSONResponse:
        if header_key != provider_request.idempotency_key:
            raise HTTPException(
                status_code=400,
                detail="Idempotency-Key must match the signed provider envelope.",
            )
        ledger: ProviderProofLedger = request.app.state.proof_ledger
        decision = ledger.begin_dispatch(
            scenario=scenario,
            provider_request=provider_request,
        )
        if decision.key_conflict:
            return JSONResponse(
                status_code=409,
                content={"error": {"code": "provider_idempotency_conflict"}},
            )
        if decision.definitive_failure:
            return JSONResponse(
                status_code=422,
                content={"error": {"code": "injected_definitive_failure"}},
            )
        if decision.wait_for_release:
            ledger.wait_for_release(
                run_id=provider_request.run_id,
                scenario=scenario,
                attempt_sequence=decision.attempt_sequence,
            )
            return JSONResponse(
                status_code=503,
                content={"error": {"code": "injected_ambiguous_result"}},
            )
        if decision.provider_reference is None:  # pragma: no cover - decision invariant
            raise RuntimeError("provider receipt decision has no reference")
        result = ExternalActionProviderResult(
            provider_reference=decision.provider_reference,
            result={},
        )
        return JSONResponse(status_code=200, content=result.model_dump(mode="json"))

    @provider_app.post("/actions/idempotent")
    def idempotent_action(
        provider_request: ExternalActionRequest,
        request: Request,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> JSONResponse:
        return dispatch(
            scenario="idempotent",
            provider_request=provider_request,
            header_key=idempotency_key,
            request=request,
        )

    @provider_app.post("/actions/unsafe")
    def unsafe_action(
        provider_request: ExternalActionRequest,
        request: Request,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> JSONResponse:
        return dispatch(
            scenario="unsafe",
            provider_request=provider_request,
            header_key=idempotency_key,
            request=request,
        )

    @provider_app.post("/actions/known-success")
    def known_success_action(
        provider_request: ExternalActionRequest,
        request: Request,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> JSONResponse:
        return dispatch(
            scenario="known_success",
            provider_request=provider_request,
            header_key=idempotency_key,
            request=request,
        )

    @provider_app.post("/actions/known-failure")
    def known_failure_action(
        provider_request: ExternalActionRequest,
        request: Request,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> JSONResponse:
        return dispatch(
            scenario="known_failure",
            provider_request=provider_request,
            header_key=idempotency_key,
            request=request,
        )

    @provider_app.get(
        "/proof/actions/{action_id}",
        response_model=ProviderProofState,
    )
    def proof_state(action_id: str, request: Request) -> ProviderProofState:
        ledger: ProviderProofLedger = request.app.state.proof_ledger
        state = ledger.snapshot(action_id)
        if state is None:
            raise HTTPException(status_code=404, detail="Proof action not found.")
        return state

    @provider_app.post("/proof/actions/{action_id}/release")
    def release_fault(action_id: str, request: Request) -> dict[str, bool]:
        ledger: ProviderProofLedger = request.app.state.proof_ledger
        if not ledger.release(action_id):
            raise HTTPException(status_code=404, detail="Held proof action not found.")
        return {"released": True}

    return provider_app


app = create_demo_provider_app()
