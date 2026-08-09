from __future__ import annotations

import json
import sqlite3
import threading
from enum import Enum
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from agent.contracts import RuntimeExecutionContext, RuntimeExecutionError, utc_now


MEMORY_READ_PERMISSION = "memory:read"
MEMORY_WRITE_PERMISSION = "memory:write"


class MemoryKind(str, Enum):
    PREFERENCE = "preference"
    FACT = "fact"


class MemoryStatus(str, Enum):
    ACTIVE = "active"
    SUPERSEDED = "superseded"
    DELETED = "deleted"


class MemoryMutationAction(str, Enum):
    CREATED = "created"
    SUPERSEDED = "superseded"
    UNCHANGED = "unchanged"


class MemoryWrite(BaseModel):
    """A domain-approved, explicit candidate for long-term memory."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: MemoryKind
    key: str = Field(min_length=1, max_length=200, pattern=r"^[a-z][a-z0-9_.-]*$")
    value: Any
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    expires_at: str | None = None


class MemoryRecord(BaseModel):
    """One versioned subject-level memory record."""

    model_config = ConfigDict(extra="forbid")

    memory_id: str = Field(min_length=1, max_length=200)
    tenant_id: str = Field(min_length=1, max_length=200)
    subject_id: str = Field(min_length=1, max_length=200)
    domain_id: str = Field(min_length=1, max_length=200)
    kind: MemoryKind
    key: str = Field(min_length=1, max_length=200, pattern=r"^[a-z][a-z0-9_.-]*$")
    value: Any
    status: MemoryStatus
    source_run_id: str | None = None
    source_thread_id: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    version: int = Field(ge=1)
    created_at: str
    updated_at: str
    expires_at: str | None = None


class RetrievedMemory(BaseModel):
    """Immutable typed value copied into a run-scoped retrieval snapshot."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    memory_id: str = Field(min_length=1, max_length=200)
    kind: MemoryKind
    key: str = Field(min_length=1, max_length=200, pattern=r"^[a-z][a-z0-9_.-]*$")
    value: Any
    source_run_id: str | None = None
    source_thread_id: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    version: int = Field(ge=1)
    created_at: str
    expires_at: str | None = None

    @classmethod
    def from_record(cls, record: MemoryRecord) -> "RetrievedMemory":
        return cls(
            memory_id=record.memory_id,
            kind=record.kind,
            key=record.key,
            value=record.value,
            source_run_id=record.source_run_id,
            source_thread_id=record.source_thread_id,
            confidence=record.confidence,
            version=record.version,
            created_at=record.created_at,
            expires_at=record.expires_at,
        )


class MemorySnapshot(BaseModel):
    """The sealed memory context used by one durable run and all its retries."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: str = Field(min_length=1, max_length=200)
    tenant_id: str = Field(min_length=1, max_length=200)
    subject_id: str = Field(min_length=1, max_length=200)
    domain_id: str = Field(min_length=1, max_length=200)
    memories: tuple[RetrievedMemory, ...] = ()
    created_at: str


class MemoryMutationResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: MemoryMutationAction
    record: MemoryRecord
    superseded_record: MemoryRecord | None = None


class MemoryEvent(BaseModel):
    """Append-only audit event for memory mutations."""

    model_config = ConfigDict(extra="forbid")

    event_id: int | None = None
    tenant_id: str = Field(min_length=1, max_length=200)
    subject_id: str = Field(min_length=1, max_length=200)
    event_type: str
    memory_id: str | None = None
    actor_subject_id: str = Field(min_length=1, max_length=200)
    source_run_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now)


class RunEventSink(Protocol):
    def append_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        ...

    def list_events(self, run_id: str, *, after_sequence: int = 0) -> list[Any]:
        ...


class MemoryStore(Protocol):
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
        ...

    def get_or_create_run_snapshot(
        self,
        *,
        run_id: str,
        tenant_id: str,
        subject_id: str,
        domain_id: str,
        allowed_keys: tuple[str, ...] | None = None,
    ) -> MemorySnapshot:
        ...

    def list_events_for_run(
        self,
        run_id: str,
        *,
        tenant_id: str,
        subject_id: str,
    ) -> list[MemoryEvent]:
        ...


class SQLiteMemoryStore:
    """SQLite-backed, tenant-and-subject-scoped long-term memory storage."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = str(database_path)
        self._lock = threading.RLock()
        Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def initialize(self) -> None:
        with self._lock, self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS memory_records (
                    memory_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    subject_id TEXT NOT NULL,
                    domain_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    memory_key TEXT NOT NULL,
                    value_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    source_run_id TEXT,
                    source_thread_id TEXT,
                    confidence REAL NOT NULL,
                    version INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    expires_at TEXT
                );

                CREATE TABLE IF NOT EXISTS memory_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    subject_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    memory_id TEXT,
                    actor_subject_id TEXT NOT NULL,
                    source_run_id TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS run_memory_snapshots (
                    run_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    subject_id TEXT NOT NULL,
                    domain_id TEXT NOT NULL,
                    memories_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_records_active_key
                    ON memory_records(tenant_id, subject_id, domain_id, kind, memory_key)
                    WHERE status = 'active';
                CREATE INDEX IF NOT EXISTS idx_memory_records_subject
                    ON memory_records(tenant_id, subject_id, domain_id, status);
                CREATE INDEX IF NOT EXISTS idx_memory_events_subject
                    ON memory_events(tenant_id, subject_id, event_id);
                CREATE INDEX IF NOT EXISTS idx_memory_events_source_run
                    ON memory_events(source_run_id, event_id)
                    WHERE source_run_id IS NOT NULL;
                """
            )

    def ping(self) -> None:
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()

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
        self._validate_identity(tenant_id, subject_id, domain_id)
        encoded_value = self._encode_json(write.value)
        now = utc_now()

        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            active_row = connection.execute(
                """
                SELECT * FROM memory_records
                WHERE tenant_id = ? AND subject_id = ? AND domain_id = ?
                    AND kind = ? AND memory_key = ? AND status = ?
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
                    "UPDATE memory_records SET status = ?, updated_at = ? WHERE memory_id = ?",
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

            version = int(
                connection.execute(
                    """
                    SELECT COALESCE(MAX(version), 0) + 1 FROM memory_records
                    WHERE tenant_id = ? AND subject_id = ? AND domain_id = ?
                        AND kind = ? AND memory_key = ?
                    """,
                    (
                        tenant_id,
                        subject_id,
                        domain_id,
                        write.kind.value,
                        write.key,
                    ),
                ).fetchone()[0]
            )
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
                version=version,
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        clauses = ["tenant_id = ?", "subject_id = ?"]
        parameters: list[Any] = [tenant_id, subject_id]
        if domain_id is not None:
            clauses.append("domain_id = ?")
            parameters.append(domain_id)
        if kind is not None:
            clauses.append("kind = ?")
            parameters.append(kind.value)
        if not include_inactive:
            clauses.extend(
                [
                    "status = ?",
                    "(expires_at IS NULL OR expires_at > ?)",
                ]
            )
            parameters.extend([MemoryStatus.ACTIVE.value, utc_now()])

        query = (
            "SELECT * FROM memory_records WHERE "
            + " AND ".join(clauses)
            + " ORDER BY domain_id, kind, memory_key, version DESC"
        )
        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [self._row_to_record(row) for row in rows]

    def get_memory_for_subject(
        self,
        memory_id: str,
        *,
        tenant_id: str,
        subject_id: str,
    ) -> MemoryRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM memory_records
                WHERE memory_id = ? AND tenant_id = ? AND subject_id = ?
                """,
                (memory_id, tenant_id, subject_id),
            ).fetchone()
        return self._row_to_record(row) if row is not None else None

    def forget_memory(
        self,
        memory_id: str,
        *,
        tenant_id: str,
        subject_id: str,
        actor_subject_id: str,
    ) -> MemoryRecord:
        """Tombstone every stored version of one logical key.

        Sealed run snapshots remain immutable execution evidence. They are not
        consulted by future runs after the active record is forgotten.
        """

        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            target = connection.execute(
                """
                SELECT * FROM memory_records
                WHERE memory_id = ? AND tenant_id = ? AND subject_id = ?
                """,
                (memory_id, tenant_id, subject_id),
            ).fetchone()
            if target is None:
                raise KeyError("Memory not found")

            affected = connection.execute(
                """
                SELECT memory_id FROM memory_records
                WHERE tenant_id = ? AND subject_id = ? AND domain_id = ?
                    AND kind = ? AND memory_key = ? AND status != ?
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
                    SET status = ?, value_json = 'null', updated_at = ?
                    WHERE tenant_id = ? AND subject_id = ? AND domain_id = ?
                        AND kind = ? AND memory_key = ? AND status != ?
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
                "SELECT * FROM memory_records WHERE memory_id = ?",
                (memory_id,),
            ).fetchone()
        if refreshed is None:  # pragma: no cover - protected by the transaction
            raise RuntimeError("Deleted memory disappeared")
        return self._row_to_record(refreshed)

    def get_or_create_run_snapshot(
        self,
        *,
        run_id: str,
        tenant_id: str,
        subject_id: str,
        domain_id: str,
        allowed_keys: tuple[str, ...] | None = None,
    ) -> MemorySnapshot:
        """Seal the first retrieval, including an empty result, for restart safety."""

        self._validate_identity(tenant_id, subject_id, domain_id)
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_run_identity(
                connection,
                run_id=run_id,
                tenant_id=tenant_id,
                subject_id=subject_id,
                domain_id=domain_id,
            )
            existing = connection.execute(
                "SELECT * FROM run_memory_snapshots WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if existing is not None:
                return self._row_to_snapshot(
                    existing,
                    expected_identity=(tenant_id, subject_id, domain_id),
                )

            clauses = [
                "tenant_id = ?",
                "subject_id = ?",
                "domain_id = ?",
                "status = ?",
                "(expires_at IS NULL OR expires_at > ?)",
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
                    placeholders = ", ".join("?" for _ in allowed_keys)
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
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.run_id,
                    snapshot.tenant_id,
                    snapshot.subject_id,
                    snapshot.domain_id,
                    json.dumps(
                        [memory.model_dump(mode="json") for memory in snapshot.memories],
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    snapshot.created_at,
                ),
            )
        return snapshot

    def get_run_snapshot(self, run_id: str) -> MemorySnapshot | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM run_memory_snapshots WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        return self._row_to_snapshot(row) if row is not None else None

    def list_events_for_subject(
        self,
        *,
        tenant_id: str,
        subject_id: str,
    ) -> list[MemoryEvent]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM memory_events
                WHERE tenant_id = ? AND subject_id = ?
                ORDER BY event_id
                """,
                (tenant_id, subject_id),
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def list_events_for_run(
        self,
        run_id: str,
        *,
        tenant_id: str,
        subject_id: str,
    ) -> list[MemoryEvent]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM memory_events
                WHERE source_run_id = ? AND tenant_id = ? AND subject_id = ?
                ORDER BY event_id
                """,
                (run_id, tenant_id, subject_id),
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    @staticmethod
    def _append_event_with_connection(
        connection: sqlite3.Connection,
        event: MemoryEvent,
    ) -> MemoryEvent:
        cursor = connection.execute(
            """
            INSERT INTO memory_events (
                tenant_id, subject_id, event_type, memory_id, actor_subject_id,
                source_run_id, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
        )
        if cursor.lastrowid is None:
            raise RuntimeError("SQLite did not return a memory event id")
        event.event_id = int(cursor.lastrowid)
        return event

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> MemoryRecord:
        return MemoryRecord(
            memory_id=row["memory_id"],
            tenant_id=row["tenant_id"],
            subject_id=row["subject_id"],
            domain_id=row["domain_id"],
            kind=MemoryKind(row["kind"]),
            key=row["memory_key"],
            value=json.loads(row["value_json"]),
            status=MemoryStatus(row["status"]),
            source_run_id=row["source_run_id"],
            source_thread_id=row["source_thread_id"],
            confidence=float(row["confidence"]),
            version=int(row["version"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            expires_at=row["expires_at"],
        )

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> MemoryEvent:
        return MemoryEvent(
            event_id=int(row["event_id"]),
            tenant_id=row["tenant_id"],
            subject_id=row["subject_id"],
            event_type=row["event_type"],
            memory_id=row["memory_id"],
            actor_subject_id=row["actor_subject_id"],
            source_run_id=row["source_run_id"],
            payload=json.loads(row["payload_json"]),
            created_at=row["created_at"],
        )

    @staticmethod
    def _row_to_snapshot(
        row: sqlite3.Row,
        *,
        expected_identity: tuple[str, str, str] | None = None,
    ) -> MemorySnapshot:
        identity = (row["tenant_id"], row["subject_id"], row["domain_id"])
        if expected_identity is not None and identity != expected_identity:
            raise ValueError("Persisted memory snapshot identity does not match run authority")
        memories = tuple(
            RetrievedMemory.model_validate(payload)
            for payload in json.loads(row["memories_json"])
        )
        return MemorySnapshot(
            run_id=row["run_id"],
            tenant_id=row["tenant_id"],
            subject_id=row["subject_id"],
            domain_id=row["domain_id"],
            memories=memories,
            created_at=row["created_at"],
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
        connection: sqlite3.Connection,
        *,
        run_id: str,
        tenant_id: str,
        subject_id: str,
        domain_id: str,
    ) -> None:
        row = connection.execute(
            """
            SELECT tenant_id, domain_id, execution_authority_json
            FROM runs WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
        if row is None or row["tenant_id"] != tenant_id or row["domain_id"] != domain_id:
            raise ValueError("Run identity does not match memory snapshot authority")
        authority_json = row["execution_authority_json"]
        if not authority_json:
            raise ValueError("Run has no trusted authority for a memory snapshot")
        authority = json.loads(authority_json)
        if authority.get("subject_id") != subject_id:
            raise ValueError("Run identity does not match memory snapshot authority")


class GovernedMemory:
    """Run-facing memory boundary with permission checks and durable evidence."""

    def __init__(self, store: MemoryStore, run_event_sink: RunEventSink) -> None:
        self.store = store
        self.run_event_sink = run_event_sink

    def retrieve(
        self,
        context: RuntimeExecutionContext,
        *,
        domain_id: str,
        allowed_keys: tuple[str, ...],
    ) -> MemorySnapshot:
        if not context.authority.allows(MEMORY_READ_PERMISSION):
            raise RuntimeExecutionError(
                "memory_permission_denied",
                "Memory read denied by persisted execution authority.",
            )
        snapshot = self.store.get_or_create_run_snapshot(
            run_id=context.run_id,
            tenant_id=context.authority.tenant_id,
            subject_id=context.authority.subject_id,
            domain_id=domain_id,
            allowed_keys=tuple(sorted(set(allowed_keys))),
        )
        self._ensure_run_event(
            context.run_id,
            "memory.retrieved",
            {
                "snapshot_id": snapshot.run_id,
                "domain_id": snapshot.domain_id,
                "created_at": snapshot.created_at,
                "count": len(snapshot.memories),
                "memories": [
                    {
                        "memory_id": memory.memory_id,
                        "kind": memory.kind.value,
                        "key": memory.key,
                        "version": memory.version,
                    }
                    for memory in snapshot.memories
                ],
            },
            identity_key="snapshot_id",
            identity_value=snapshot.run_id,
        )
        return snapshot

    def remember(
        self,
        context: RuntimeExecutionContext,
        *,
        domain_id: str,
        source_thread_id: str,
        writes: tuple[MemoryWrite, ...],
    ) -> None:
        if not writes:
            return
        if not context.authority.allows(MEMORY_WRITE_PERMISSION):
            raise RuntimeExecutionError(
                "memory_permission_denied",
                "Memory write denied by persisted execution authority.",
            )
        for write in writes:
            self.store.upsert(
                tenant_id=context.authority.tenant_id,
                subject_id=context.authority.subject_id,
                domain_id=domain_id,
                write=write,
                source_run_id=context.run_id,
                source_thread_id=source_thread_id,
                actor_subject_id=context.authority.subject_id,
            )
        self._mirror_mutation_events(context)

    def _mirror_mutation_events(self, context: RuntimeExecutionContext) -> None:
        existing_audit_events = {
            (event.event_type, event.payload.get("audit_event_id"))
            for event in self.run_event_sink.list_events(context.run_id)
            if event.payload.get("audit_event_id") is not None
        }
        for event in self.store.list_events_for_run(
            context.run_id,
            tenant_id=context.authority.tenant_id,
            subject_id=context.authority.subject_id,
        ):
            if event.event_id is None:  # pragma: no cover - persisted events always have ids
                continue
            payload = {
                "audit_event_id": event.event_id,
                "memory_id": event.memory_id,
                **event.payload,
            }
            identity = (event.event_type, event.event_id)
            if identity in existing_audit_events:
                continue
            self.run_event_sink.append_event(context.run_id, event.event_type, payload)
            existing_audit_events.add(identity)

    def _ensure_run_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        identity_key: str,
        identity_value: Any,
    ) -> None:
        for event in self.run_event_sink.list_events(run_id):
            if (
                event.event_type == event_type
                and event.payload.get(identity_key) == identity_value
            ):
                return
        self.run_event_sink.append_event(run_id, event_type, payload)
