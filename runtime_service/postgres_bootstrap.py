from __future__ import annotations

import argparse
import json
import sys

from .postgres_schema import (
    POSTGRES_MEMORY_SCHEMA_VERSION,
    POSTGRES_SCHEMA_VERSION,
    PostgresSchemaError,
    bootstrap_postgres_application_schema,
    inspect_postgres_application_schema,
    validate_postgres_application_schema,
)
from .storage import resolve_runtime_storage_config


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect or bootstrap the Agent Runtime PostgreSQL application schema. "
            "The DSN is read only from RUNTIME_POSTGRES_DSN and is never printed."
        )
    )
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--dry-run", action="store_true")
    action.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--schema",
        help="Override RUNTIME_POSTGRES_SCHEMA (default: agent_runtime).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = resolve_runtime_storage_config(
            backend="postgres",
            postgres_schema=args.schema,
        )
        assert config.postgres_dsn is not None
        assert config.postgres_schema is not None
        assert config.connect_timeout_seconds is not None
        assert config.statement_timeout_seconds is not None
        assert config.lock_timeout_seconds is not None

        if args.dry_run:
            status = inspect_postgres_application_schema(
                config.postgres_dsn,
                schema=config.postgres_schema,
                connect_timeout_seconds=config.connect_timeout_seconds,
            )
            for component, accepted in (
                ("execution-plane", POSTGRES_SCHEMA_VERSION),
                ("memory", POSTGRES_MEMORY_SCHEMA_VERSION),
            ):
                observed = status.components.get(component)
                if observed is not None and observed != accepted:
                    raise PostgresSchemaError(
                        f"PostgreSQL {component} schema version is incompatible"
                    )
            action = "no_change"
            if not status.compatible:
                action = "would_bootstrap_or_validate"
            result = {
                "action": action,
                "schema": status.schema,
                "schema_exists": status.schema_exists,
                "metadata_exists": status.metadata_exists,
                "components": status.components,
            }
        else:
            versions = bootstrap_postgres_application_schema(
                config.postgres_dsn,
                schema=config.postgres_schema,
                connect_timeout_seconds=config.connect_timeout_seconds,
                statement_timeout_seconds=config.statement_timeout_seconds,
                lock_timeout_seconds=config.lock_timeout_seconds,
            )
            validated = validate_postgres_application_schema(
                config.postgres_dsn,
                schema=config.postgres_schema,
                connect_timeout_seconds=config.connect_timeout_seconds,
                statement_timeout_seconds=config.statement_timeout_seconds,
                lock_timeout_seconds=config.lock_timeout_seconds,
            )
            if validated != versions:
                raise PostgresSchemaError(
                    "PostgreSQL bootstrap postcondition changed during validation"
                )
            result = {
                "action": "applied_and_validated",
                "schema": config.postgres_schema,
                "components": validated,
            }
    except (PostgresSchemaError, ValueError) as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, sort_keys=True))
        return 2

    print(json.dumps({"status": "ok", **result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
