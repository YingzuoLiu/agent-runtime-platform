from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from pathlib import Path
from uuid import UUID, uuid4

from pydantic import ValidationError

from runtime_service.external_actions import (
    DefinitiveExternalActionError,
    ExternalActionProviderResult,
    ExternalActionRequest,
)

from .models import CreateTripHoldInput


class SQLiteTripHoldProvider:
    """Deterministic provider-side idempotency ledger for the Travel reference action.

    This is deliberately a provider test double, not live inventory.  Its ledger is
    separate from the runtime action ledger so tests can prove that replaying the
    same provider idempotency key does not create a second semantic hold.
    """

    supports_idempotency = True

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = str(database_path)
        self._lock = threading.RLock()
        Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
        self._provider_identity = self._initialize()

    @property
    def provider_identity(self) -> str:
        return self._provider_identity

    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        if request.tool_name != "create_trip_hold":
            raise DefinitiveExternalActionError(
                "Trip hold provider received an unsupported tool"
            )

        try:
            arguments = CreateTripHoldInput.model_validate(request.arguments).model_dump(
                mode="json"
            )
        except ValidationError:
            raise DefinitiveExternalActionError(
                "Trip hold provider received invalid arguments"
            ) from None
        canonical_payload = json.dumps(
            arguments,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        payload_hash = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()

        with self._lock, self._connect() as connection:
            # Serialize provider-side key ownership across independent provider
            # instances and processes, not only threads sharing this object.
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM synthetic_trip_holds WHERE idempotency_key = ?",
                (request.idempotency_key,),
            ).fetchone()
            if existing is not None:
                if existing["payload_hash"] != payload_hash:
                    raise DefinitiveExternalActionError(
                        "Trip hold idempotency key was reused with different arguments"
                    )
                result = json.loads(existing["result_json"])
                if not isinstance(result, dict):  # pragma: no cover - guarded on write
                    raise RuntimeError("Stored trip hold result is invalid")
                return ExternalActionProviderResult(
                    provider_reference=existing["provider_reference"],
                    result=result,
                )

            provider_reference = (
                "hold_"
                + hashlib.sha256(request.idempotency_key.encode("utf-8")).hexdigest()[:16]
            )
            result = {
                "status": "held",
                "provider_reference": provider_reference,
                **arguments,
            }
            connection.execute(
                """
                INSERT INTO synthetic_trip_holds (
                    idempotency_key, payload_hash, payload_json,
                    provider_reference, result_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    request.idempotency_key,
                    payload_hash,
                    canonical_payload,
                    provider_reference,
                    json.dumps(
                        result,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ),
                ),
            )
            return ExternalActionProviderResult(
                provider_reference=provider_reference,
                result=result,
            )

    def count_holds(self) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) FROM synthetic_trip_holds").fetchone()
        assert row is not None
        return int(row[0])

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        deadline = time.monotonic() + 30
        while True:
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                return connection
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or time.monotonic() >= deadline:
                    connection.close()
                    raise
                # SQLite may return SQLITE_BUSY immediately while another first-open
                # connection is switching the same file into WAL mode. Retrying here
                # preserves the cross-instance BEGIN IMMEDIATE contract below.
                time.sleep(0.01)

    def _initialize(self) -> str:
        with self._lock, self._connect() as connection:
            # The identity and provider ledger are one durable unit. Serialize
            # first-open initialization across provider instances so a restart
            # cannot accidentally mint a second idempotency domain for the
            # same database file.
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS synthetic_trip_hold_provider_metadata (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    provider_identity TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS synthetic_trip_holds (
                    idempotency_key TEXT PRIMARY KEY,
                    payload_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    provider_reference TEXT NOT NULL UNIQUE,
                    result_json TEXT NOT NULL
                )
                """
            )
            row = connection.execute(
                """
                SELECT provider_identity
                FROM synthetic_trip_hold_provider_metadata
                WHERE singleton = 1
                """
            ).fetchone()
            if row is None:
                provider_identity = str(uuid4())
                connection.execute(
                    """
                    INSERT INTO synthetic_trip_hold_provider_metadata (
                        singleton, provider_identity
                    ) VALUES (1, ?)
                    """,
                    (provider_identity,),
                )
                return provider_identity

            provider_identity = row["provider_identity"]
            try:
                canonical_identity = str(UUID(provider_identity))
            except (AttributeError, TypeError, ValueError):
                raise RuntimeError(
                    "Trip hold provider metadata has an invalid identity"
                ) from None
            if canonical_identity != provider_identity:
                raise RuntimeError(
                    "Trip hold provider metadata has an invalid identity"
                )
            return provider_identity
