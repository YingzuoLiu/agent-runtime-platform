from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any
from uuid import uuid4

from agent.contracts import utc_now

from .memory import (
    MemoryEvent,
    MemoryKind,
    MemoryMutationAction,
    MemoryMutationResult,
    MemoryRecord,
    MemorySnapshot,
    MemoryStatus,
    MemoryWrite,
    RetrievedMemory,
)
from .postgres_schema import (
    initialize_postgres_memory_schema,
    open_postgres_connection,
    validate_postgres_schema_name,
)
from .store import RunLeaseLostError


Row = Mapping[str, Any]


class PostgresMemoryStore:
    """PostgreSQL implementation of the governed Memory contract.

    Run identity and the current unexpired lease are locked and validated in
    the same transaction as every managed mutation or snapshot. A scoped
    transaction advisory lock serializes the first-write case where no active
    Memory row exists yet; the partial unique index remains an independent
    database backstop for the one-active-version invariant.
    """

    def __init__(
        self,
        dsn: str,
        *,
        schema: str = "agent_runtime",
        lease_clock_ms: Callable[[], int] | None = None,
        connect_timeout_seconds: float = 30,
        statement_timeout_seconds: float | None = None,
        lock_timeout_seconds: float | None = None,
        idle_in_transaction_session_timeout_seconds: float = 5.0,
        initialize: bool = True,
    ) -> None:
        if connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be positive")
        if idle_in_transaction_session_timeout_seconds <= 0:
            raise ValueError(
                "idle_in_transaction_session_timeout_seconds must be positive"
            )
        self._dsn = dsn
        self.schema = validate_postgres_schema_name(schema)
        self._lease_clock_ms = lease_clock_ms
        self._connect_timeout_seconds = connect_timeout_seconds
        self._statement_timeout_seconds = statement_timeout_seconds
        self._lock_timeout_seconds = lock_timeout_seconds
        self._idle_in_transaction_session_timeout_seconds = (
            idle_in_transaction_session_timeout_seconds
        )
        if initialize:
            initialize_postgres_memory_schema(
                dsn,
                schema=self.schema,
                connect_timeout_seconds=connect_timeout_seconds,
                statement_timeout_seconds=statement_timeout_seconds,
                lock_timeout_seconds=lock_timeout_seconds,
                idle_in_transaction_session_timeout_seconds=(
                    idle_in_transaction_session_timeout_seconds
                ),
            )

    def _connect(self):
        return open_postgres_connection(
            self._dsn,
            schema=self.schema,
            connect_timeout_seconds=self._connect_timeout_seconds,
            statement_timeout_seconds=self._statement_timeout_seconds,
            lock_timeout_seconds=self._lock_timeout_seconds,
            idle_in_transaction_session_timeout_seconds=(
                self._idle_in_transaction_session_timeout_seconds
            ),
        )

    def ping(self) -> None:
        connection = self._connect()
        try:
            connection.execute("SELECT 1").fetchone()
        finally:
            connection.close()

    def upsert(
        self,
        *,
        tenant_id: str,
        subject_id: str,
        domain_id: str,
        write: MemoryWrite,
        source_run_id: str,
        source_thread_id: str,
        actor_subject_id: str,
    ) -> MemoryMutationResult:
        """Apply an explicit administrative/test memory mutation."""

        return self._upsert(
            tenant_id=tenant_id,
            subject_id=subject_id,
            domain_id=domain_id,
            write=write,
            source_run_id=source_run_id,
            source_thread_id=source_thread_id,
            actor_subject_id=actor_subject_id,
            lease_token=None,
        )

    def upsert_from_run(
        self,
        *,
        lease_token: str,
        tenant_id: str,
        subject_id: str,
        domain_id: str,
        write: MemoryWrite,
        source_run_id: str,
        source_thread_id: str,
        actor_subject_id: str,
    ) -> MemoryMutationResult:
        """Apply a mutation owned by the current Run attempt."""

        if not lease_token:
            raise ValueError("lease_token must not be empty")
        return self._upsert(
            tenant_id=tenant_id,
            subject_id=subject_id,
            domain_id=domain_id,
            write=write,
            source_run_id=source_run_id,
            source_thread_id=source_thread_id,
            actor_subject_id=actor_subject_id,
            lease_token=lease_token,
        )

    def _upsert(
        self,
        *,
        tenant_id: str,
        subject_id: str,
        domain_id: str,
        write: MemoryWrite,
        source_run_id: str,
        source_thread_id: str,
        actor_subject_id: str,
        lease_token: str | None,
    ) -> MemoryMutationResult:
        self._validate_identity(tenant_id, subject_id, domain_id)
        encoded_value = self._encode_json(write.value)
        now = utc_now()
        connection = self._connect()
        try:
            with connection.transaction():
                if lease_token is not None:
                    self._assert_run_identity(
                        connection,
                        run_id=source_run_id,
                        tenant_id=tenant_id,
                        subject_id=subject_id,
                        domain_id=domain_id,
                    )
                    self._assert_current_run_lease(
                        connection,
                        source_run_id,
                        lease_token=lease_token,
                    )
                self._lock_memory_key_with_connection(
                    connection,
                    tenant_id=tenant_id,
                    subject_id=subject_id,
                    domain_id=domain_id,
                    kind=write.kind,
                    key=write.key,
                )
                active_row = connection.execute(
                    """
                    SELECT * FROM memory_records
                    WHERE tenant_id = %s AND subject_id = %s AND domain_id = %s
                        AND kind = %s AND memory_key = %s AND status = %s
                    FOR UPDATE
                    """,
                    (
                        tenant_id,
                        subject_id,
                        domain_id,
                        write.kind.value,
                        write.key,
                        MemoryStatus.ACTIVE.value,
                    ),
                ).fetchone()
                if (
                    active_row is not None
                    and active_row["value_json"] == encoded_value
                    and active_row["expires_at"] == write.expires_at
                ):
                    return MemoryMutationResult(
                        action=MemoryMutationAction.UNCHANGED,
                        record=self._row_to_record(active_row),
                    )

                superseded: MemoryRecord | None = None
                if active_row is not None:
                    connection.execute(
                        """
                        UPDATE memory_records SET status = %s, updated_at = %s
                        WHERE memory_id = %s
                        """,
                        (
                            MemoryStatus.SUPERSEDED.value,
                            now,
                            active_row["memory_id"],
                        ),
                    )
                    superseded = self._row_to_record(active_row).model_copy(
                        update={
                            "status": MemoryStatus.SUPERSEDED,
                            "updated_at": now,
                        }
                    )

                version_row = connection.execute(
                    """
                    SELECT COALESCE(MAX(version), 0) + 1 AS version
                    FROM memory_records
                    WHERE tenant_id = %s AND subject_id = %s AND domain_id = %s
                        AND kind = %s AND memory_key = %s
                    """,
                    (
                        tenant_id,
                        subject_id,
                        domain_id,
                        write.kind.value,
                        write.key,
                    ),
                ).fetchone()
                assert version_row is not None
                record = MemoryRecord(
                    memory_id=f"mem_{uuid4().hex}",
                    tenant_id=tenant_id,
                    subject_id=subject_id,
                    domain_id=domain_id,
                    kind=write.kind,
                    key=write.key,
                    value=write.value,
                    status=MemoryStatus.ACTIVE,
                    source_run_id=source_run_id,
                    source_thread_id=source_thread_id,
                    confidence=write.confidence,
                    version=int(version_row["version"]),
                    created_at=now,
                    updated_at=now,
                    expires_at=write.expires_at,
                )
                connection.execute(
                    """
                    INSERT INTO memory_records (
                        memory_id, tenant_id, subject_id, domain_id, kind, memory_key,
                        value_json, status, source_run_id, source_thread_id, confidence,
                        version, created_at, updated_at, expires_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        record.memory_id,
                        record.tenant_id,
                        record.subject_id,
                        record.domain_id,
                        record.kind.value,
                        record.key,
                        encoded_value,
                        record.status.value,
                        record.source_run_id,
                        record.source_thread_id,
                        record.confidence,
                        record.version,
                        record.created_at,
                        record.updated_at,
                        record.expires_at,
                    ),
                )
                if superseded is not None:
                    self._append_event_with_connection(
                        connection,
                        MemoryEvent(
                            tenant_id=tenant_id,
                            subject_id=subject_id,
                            event_type="memory.superseded",
                            memory_id=superseded.memory_id,
                            actor_subject_id=actor_subject_id,
                            source_run_id=source_run_id,
                            payload={
                                "domain_id": domain_id,
                                "kind": write.kind.value,
                                "key": write.key,
                                "version": superseded.version,
                                "replacement_memory_id": record.memory_id,
                                "replacement_version": record.version,
                            },
                        ),
                    )
                self._append_event_with_connection(
                    connection,
                    MemoryEvent(
                        tenant_id=tenant_id,
                        subject_id=subject_id,
                        event_type="memory.created",
                        memory_id=record.memory_id,
                        actor_subject_id=actor_subject_id,
                        source_run_id=source_run_id,
                        payload={
                            "domain_id": domain_id,
                            "kind": write.kind.value,
                            "key": write.key,
                            "version": record.version,
                        },
                    ),
                )
            return MemoryMutationResult(
                action=(
                    MemoryMutationAction.SUPERSEDED
                    if superseded is not None
                    else MemoryMutationAction.CREATED
                ),
                record=record,
                superseded_record=superseded,
            )
        finally:
            connection.close()

    def list_memories(
        self,
        *,
        tenant_id: str,
        subject_id: str,
        domain_id: str | None = None,
        kind: MemoryKind | None = None,
        include_inactive: bool = False,
    ) -> list[MemoryRecord]:
        self._validate_identity(tenant_id, subject_id)
        clauses = ["tenant_id = %s", "subject_id = %s"]
        parameters: list[Any] = [tenant_id, subject_id]
        if domain_id is not None:
            clauses.append("domain_id = %s")
            parameters.append(domain_id)
        if kind is not None:
            clauses.append("kind = %s")
            parameters.append(kind.value)
        if not include_inactive:
            clauses.extend(
                [
                    "status = %s",
                    "(expires_at IS NULL OR expires_at > %s)",
                ]
            )
            parameters.extend([MemoryStatus.ACTIVE.value, utc_now()])
        query = (
            "SELECT * FROM memory_records WHERE "
            + " AND ".join(clauses)
            + " ORDER BY domain_id, kind, memory_key, version DESC"
        )
        connection = self._connect()
        try:
            rows = connection.execute(query, parameters).fetchall()
        finally:
            connection.close()
        return [self._row_to_record(row) for row in rows]

    def get_memory_for_subject(
        self,
        memory_id: str,
        *,
        tenant_id: str,
        subject_id: str,
    ) -> MemoryRecord | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT * FROM memory_records
                WHERE memory_id = %s AND tenant_id = %s AND subject_id = %s
                """,
                (memory_id, tenant_id, subject_id),
            ).fetchone()
        finally:
            connection.close()
        return self._row_to_record(row) if row is not None else None

    def forget_memory(
        self,
        memory_id: str,
        *,
        tenant_id: str,
        subject_id: str,
        actor_subject_id: str,
    ) -> MemoryRecord:
        now = utc_now()
        connection = self._connect()
        try:
            with connection.transaction():
                initial = connection.execute(
                    """
                    SELECT * FROM memory_records
                    WHERE memory_id = %s AND tenant_id = %s AND subject_id = %s
                    """,
                    (memory_id, tenant_id, subject_id),
                ).fetchone()
                if initial is None:
                    raise KeyError("Memory not found")
                self._lock_memory_key_with_connection(
                    connection,
                    tenant_id=tenant_id,
                    subject_id=subject_id,
                    domain_id=str(initial["domain_id"]),
                    kind=MemoryKind(str(initial["kind"])),
                    key=str(initial["memory_key"]),
                )
                target = connection.execute(
                    """
                    SELECT * FROM memory_records
                    WHERE memory_id = %s AND tenant_id = %s AND subject_id = %s
                    FOR UPDATE
                    """,
                    (memory_id, tenant_id, subject_id),
                ).fetchone()
                if target is None:  # pragma: no cover - rows are tombstoned, never removed
                    raise KeyError("Memory not found")
                affected = connection.execute(
                    """
                    SELECT memory_id FROM memory_records
                    WHERE tenant_id = %s AND subject_id = %s AND domain_id = %s
                        AND kind = %s AND memory_key = %s AND status != %s
                    FOR UPDATE
                    """,
                    (
                        tenant_id,
                        subject_id,
                        target["domain_id"],
                        target["kind"],
                        target["memory_key"],
                        MemoryStatus.DELETED.value,
                    ),
                ).fetchall()
                if affected:
                    connection.execute(
                        """
                        UPDATE memory_records
                        SET status = %s, value_json = 'null', updated_at = %s
                        WHERE tenant_id = %s AND subject_id = %s AND domain_id = %s
                            AND kind = %s AND memory_key = %s AND status != %s
                        """,
                        (
                            MemoryStatus.DELETED.value,
                            now,
                            tenant_id,
                            subject_id,
                            target["domain_id"],
                            target["kind"],
                            target["memory_key"],
                            MemoryStatus.DELETED.value,
                        ),
                    )
                    self._append_event_with_connection(
                        connection,
                        MemoryEvent(
                            tenant_id=tenant_id,
                            subject_id=subject_id,
                            event_type="memory.deleted",
                            memory_id=memory_id,
                            actor_subject_id=actor_subject_id,
                            source_run_id=None,
                            payload={
                                "domain_id": target["domain_id"],
                                "kind": target["kind"],
                                "key": target["memory_key"],
                                "deleted_version_count": len(affected),
                            },
                        ),
                    )
                refreshed = connection.execute(
                    "SELECT * FROM memory_records WHERE memory_id = %s",
                    (memory_id,),
                ).fetchone()
            if refreshed is None:  # pragma: no cover - protected by the transaction
                raise RuntimeError("Deleted memory disappeared")
            return self._row_to_record(refreshed)
        finally:
            connection.close()

    def get_or_create_run_snapshot(
        self,
        *,
        run_id: str,
        tenant_id: str,
        subject_id: str,
        domain_id: str,
        allowed_keys: tuple[str, ...] | None = None,
    ) -> MemorySnapshot:
        """Seal a snapshot through the explicit administrative/test API."""

        return self._get_or_create_run_snapshot(
            run_id=run_id,
            tenant_id=tenant_id,
            subject_id=subject_id,
            domain_id=domain_id,
            allowed_keys=allowed_keys,
            lease_token=None,
        )

    def get_or_create_run_snapshot_for_run(
        self,
        *,
        lease_token: str,
        run_id: str,
        tenant_id: str,
        subject_id: str,
        domain_id: str,
        allowed_keys: tuple[str, ...] | None = None,
    ) -> MemorySnapshot:
        """Seal or reuse a snapshot for the current Run attempt."""

        if not lease_token:
            raise ValueError("lease_token must not be empty")
        return self._get_or_create_run_snapshot(
            run_id=run_id,
            tenant_id=tenant_id,
            subject_id=subject_id,
            domain_id=domain_id,
            allowed_keys=allowed_keys,
            lease_token=lease_token,
        )

    def _get_or_create_run_snapshot(
        self,
        *,
        run_id: str,
        tenant_id: str,
        subject_id: str,
        domain_id: str,
        allowed_keys: tuple[str, ...] | None,
        lease_token: str | None,
    ) -> MemorySnapshot:
        self._validate_identity(tenant_id, subject_id, domain_id)
        connection = self._connect()
        try:
            with connection.transaction():
                self._assert_run_identity(
                    connection,
                    run_id=run_id,
                    tenant_id=tenant_id,
                    subject_id=subject_id,
                    domain_id=domain_id,
                )
                if lease_token is not None:
                    self._assert_current_run_lease(
                        connection,
                        run_id,
                        lease_token=lease_token,
                    )
                existing = connection.execute(
                    "SELECT * FROM run_memory_snapshots WHERE run_id = %s",
                    (run_id,),
                ).fetchone()
                if existing is not None:
                    return self._row_to_snapshot(
                        existing,
                        expected_identity=(tenant_id, subject_id, domain_id),
                    )

                clauses = [
                    "tenant_id = %s",
                    "subject_id = %s",
                    "domain_id = %s",
                    "status = %s",
                    "(expires_at IS NULL OR expires_at > %s)",
                ]
                now = utc_now()
                parameters: list[Any] = [
                    tenant_id,
                    subject_id,
                    domain_id,
                    MemoryStatus.ACTIVE.value,
                    now,
                ]
                if allowed_keys is not None:
                    if allowed_keys:
                        placeholders = ", ".join("%s" for _ in allowed_keys)
                        clauses.append(f"memory_key IN ({placeholders})")
                        parameters.extend(allowed_keys)
                        rows = connection.execute(
                            "SELECT * FROM memory_records WHERE "
                            + " AND ".join(clauses)
                            + " ORDER BY kind, memory_key, version",
                            parameters,
                        ).fetchall()
                    else:
                        rows = []
                else:
                    rows = connection.execute(
                        "SELECT * FROM memory_records WHERE "
                        + " AND ".join(clauses)
                        + " ORDER BY kind, memory_key, version",
                        parameters,
                    ).fetchall()
                memories = tuple(
                    RetrievedMemory.from_record(self._row_to_record(row)) for row in rows
                )
                snapshot = MemorySnapshot(
                    run_id=run_id,
                    tenant_id=tenant_id,
                    subject_id=subject_id,
                    domain_id=domain_id,
                    memories=memories,
                    created_at=now,
                )
                connection.execute(
                    """
                    INSERT INTO run_memory_snapshots (
                        run_id, tenant_id, subject_id, domain_id, memories_json, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        snapshot.run_id,
                        snapshot.tenant_id,
                        snapshot.subject_id,
                        snapshot.domain_id,
                        json.dumps(
                            [
                                memory.model_dump(mode="json")
                                for memory in snapshot.memories
                            ],
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        snapshot.created_at,
                    ),
                )
            return snapshot
        finally:
            connection.close()

    def get_run_snapshot(self, run_id: str) -> MemorySnapshot | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM run_memory_snapshots WHERE run_id = %s",
                (run_id,),
            ).fetchone()
        finally:
            connection.close()
        return self._row_to_snapshot(row) if row is not None else None

    def list_events_for_subject(
        self,
        *,
        tenant_id: str,
        subject_id: str,
    ) -> list[MemoryEvent]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT * FROM memory_events
                WHERE tenant_id = %s AND subject_id = %s
                ORDER BY event_id
                """,
                (tenant_id, subject_id),
            ).fetchall()
        finally:
            connection.close()
        return [self._row_to_event(row) for row in rows]

    def list_events_for_run(
        self,
        run_id: str,
        *,
        tenant_id: str,
        subject_id: str,
    ) -> list[MemoryEvent]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT * FROM memory_events
                WHERE source_run_id = %s AND tenant_id = %s AND subject_id = %s
                ORDER BY event_id
                """,
                (run_id, tenant_id, subject_id),
            ).fetchall()
        finally:
            connection.close()
        return [self._row_to_event(row) for row in rows]

    @staticmethod
    def _append_event_with_connection(connection, event: MemoryEvent) -> MemoryEvent:
        row = connection.execute(
            """
            INSERT INTO memory_events (
                tenant_id, subject_id, event_type, memory_id, actor_subject_id,
                source_run_id, payload_json, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING event_id
            """,
            (
                event.tenant_id,
                event.subject_id,
                event.event_type,
                event.memory_id,
                event.actor_subject_id,
                event.source_run_id,
                json.dumps(event.payload, sort_keys=True, separators=(",", ":")),
                event.created_at,
            ),
        ).fetchone()
        if row is None:
            raise RuntimeError("PostgreSQL did not return a memory event id")
        event.event_id = int(row["event_id"])
        return event

    def _lock_memory_key_with_connection(
        self,
        connection,
        *,
        tenant_id: str,
        subject_id: str,
        domain_id: str,
        kind: MemoryKind,
        key: str,
    ) -> None:
        identity = json.dumps(
            [self.schema, tenant_id, subject_id, domain_id, kind.value, key],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"agent-runtime-memory-key:{identity}",),
        )

    @staticmethod
    def _row_to_record(row: Row) -> MemoryRecord:
        return MemoryRecord(
            memory_id=str(row["memory_id"]),
            tenant_id=str(row["tenant_id"]),
            subject_id=str(row["subject_id"]),
            domain_id=str(row["domain_id"]),
            kind=MemoryKind(str(row["kind"])),
            key=str(row["memory_key"]),
            value=json.loads(str(row["value_json"])),
            status=MemoryStatus(str(row["status"])),
            source_run_id=(
                str(row["source_run_id"])
                if row["source_run_id"] is not None
                else None
            ),
            source_thread_id=(
                str(row["source_thread_id"])
                if row["source_thread_id"] is not None
                else None
            ),
            confidence=float(row["confidence"]),
            version=int(row["version"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            expires_at=(str(row["expires_at"]) if row["expires_at"] is not None else None),
        )

    @staticmethod
    def _row_to_event(row: Row) -> MemoryEvent:
        return MemoryEvent(
            event_id=int(row["event_id"]),
            tenant_id=str(row["tenant_id"]),
            subject_id=str(row["subject_id"]),
            event_type=str(row["event_type"]),
            memory_id=(str(row["memory_id"]) if row["memory_id"] is not None else None),
            actor_subject_id=str(row["actor_subject_id"]),
            source_run_id=(
                str(row["source_run_id"])
                if row["source_run_id"] is not None
                else None
            ),
            payload=json.loads(str(row["payload_json"])),
            created_at=str(row["created_at"]),
        )

    @staticmethod
    def _row_to_snapshot(
        row: Row,
        *,
        expected_identity: tuple[str, str, str] | None = None,
    ) -> MemorySnapshot:
        identity = (
            str(row["tenant_id"]),
            str(row["subject_id"]),
            str(row["domain_id"]),
        )
        if expected_identity is not None and identity != expected_identity:
            raise ValueError("Persisted memory snapshot identity does not match run authority")
        memories = tuple(
            RetrievedMemory.model_validate(payload)
            for payload in json.loads(str(row["memories_json"]))
        )
        return MemorySnapshot(
            run_id=str(row["run_id"]),
            tenant_id=identity[0],
            subject_id=identity[1],
            domain_id=identity[2],
            memories=memories,
            created_at=str(row["created_at"]),
        )

    @staticmethod
    def _encode_json(value: Any) -> str:
        try:
            return json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("Memory value must be JSON serializable") from exc

    @staticmethod
    def _validate_identity(*values: str) -> None:
        if any(not value or len(value) > 200 for value in values):
            raise ValueError("Memory identity fields must contain 1 to 200 characters")

    @staticmethod
    def _assert_run_identity(
        connection,
        *,
        run_id: str,
        tenant_id: str,
        subject_id: str,
        domain_id: str,
    ) -> None:
        row = connection.execute(
            """
            SELECT tenant_id, domain_id, execution_authority_json
            FROM runs WHERE run_id = %s
            FOR UPDATE
            """,
            (run_id,),
        ).fetchone()
        if row is None or row["tenant_id"] != tenant_id or row["domain_id"] != domain_id:
            raise ValueError("Run identity does not match memory snapshot authority")
        authority_json = row["execution_authority_json"]
        if not authority_json:
            raise ValueError("Run has no trusted authority for a memory snapshot")
        authority = json.loads(str(authority_json))
        if authority.get("subject_id") != subject_id:
            raise ValueError("Run identity does not match memory snapshot authority")

    def _lease_now_ms(self, connection) -> int:
        if self._lease_clock_ms is not None:
            return self._lease_clock_ms()
        row = connection.execute(
            """
            SELECT floor(extract(epoch FROM transaction_timestamp()) * 1000)::bigint AS now_ms
            """
        ).fetchone()
        assert row is not None
        return int(row["now_ms"])

    def _assert_current_run_lease(
        self,
        connection,
        run_id: str,
        *,
        lease_token: str,
    ) -> None:
        now_ms = self._lease_now_ms(connection)
        row = connection.execute(
            """
            SELECT 1 FROM runs
            WHERE run_id = %s AND status = 'running' AND lease_token = %s
                AND lease_expires_at > %s
            FOR UPDATE
            """,
            (run_id, lease_token, now_ms),
        ).fetchone()
        if row is None:
            raise RunLeaseLostError(f"Run lease is no longer current: {run_id}")
