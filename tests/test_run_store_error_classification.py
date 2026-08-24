from __future__ import annotations

import sqlite3

from runtime_service.run_store import (
    is_run_store_contention_error,
    is_run_store_error,
    is_run_store_integrity_error,
    is_run_store_retryable_error,
)


class _BusyOperationalError(sqlite3.OperationalError):
    sqlite_errorcode = sqlite3.SQLITE_BUSY


class _SqlstateError(RuntimeError):
    def __init__(self, sqlstate: str) -> None:
        super().__init__("sanitized PostgreSQL failure")
        self.sqlstate = sqlstate


def _driver_error(class_name: str, sqlstate: str | None) -> Exception:
    """Build a Psycopg-shaped error without importing a concrete driver.

    Classification keys on the module name and the exception class name, so a
    faithful stand-in must control both. Psycopg raises a SQLSTATE-specific
    subclass when the server reports one and a bare ``OperationalError``/
    ``InterfaceError`` when the failure is client-side, so tests must be able
    to produce either shape for the same state.
    """

    error_type = type(
        class_name,
        (Exception,),
        {"__module__": "psycopg.errors", "sqlstate": sqlstate},
    )
    return error_type("sanitized PostgreSQL failure")


def _raise_bug_with_implicit_context(context_error: BaseException) -> BaseException:
    try:
        raise context_error
    except BaseException:
        try:
            raise AttributeError("programming bug after handled store error")
        except AttributeError as exc:
            return exc


def _wrap_with_explicit_cause(cause: BaseException) -> BaseException:
    try:
        raise cause
    except BaseException as exc:
        try:
            raise RuntimeError("store boundary wrapper") from exc
        except RuntimeError as wrapped:
            return wrapped


def test_implicit_sqlite_context_does_not_classify_programming_bug_as_store_error() -> None:
    bug = _raise_bug_with_implicit_context(sqlite3.OperationalError("handled database error"))

    assert bug.__cause__ is None
    assert isinstance(bug.__context__, sqlite3.OperationalError)
    assert not is_run_store_error(bug)
    assert not is_run_store_retryable_error(bug)


def test_implicit_integrity_context_does_not_enter_idempotent_submit_path() -> None:
    bug = _raise_bug_with_implicit_context(sqlite3.IntegrityError("handled integrity error"))

    assert bug.__cause__ is None
    assert isinstance(bug.__context__, sqlite3.IntegrityError)
    assert not is_run_store_integrity_error(bug)


def test_implicit_busy_context_does_not_hide_bug_as_contention() -> None:
    bug = _raise_bug_with_implicit_context(_BusyOperationalError("handled busy error"))

    assert bug.__cause__ is None
    assert isinstance(bug.__context__, _BusyOperationalError)
    assert not is_run_store_contention_error(bug)


def test_explicit_sqlite_causes_remain_classified() -> None:
    retryable = _wrap_with_explicit_cause(sqlite3.OperationalError("database unavailable"))
    integrity = _wrap_with_explicit_cause(sqlite3.IntegrityError("duplicate"))
    contention = _wrap_with_explicit_cause(_BusyOperationalError("database busy"))

    assert is_run_store_error(retryable)
    assert is_run_store_retryable_error(retryable)
    assert is_run_store_integrity_error(integrity)
    assert is_run_store_contention_error(contention)


def test_admin_shutdown_is_retryable_only_in_side_effect_free_store_polling() -> None:
    terminated_connection = _SqlstateError("57P01")

    assert is_run_store_error(terminated_connection)
    assert is_run_store_retryable_error(terminated_connection)
    assert not is_run_store_contention_error(terminated_connection)


def test_admin_shutdown_is_retryable_under_either_driver_class_shape() -> None:
    """The 57P01 decision must not depend on how Psycopg names the class."""

    assert is_run_store_retryable_error(_driver_error("AdminShutdown", "57P01"))
    assert is_run_store_retryable_error(_driver_error("OperationalError", "57P01"))


def test_other_operator_intervention_is_not_broadly_made_retryable() -> None:
    assert not is_run_store_retryable_error(_SqlstateError("57P02"))
    assert not is_run_store_retryable_error(_driver_error("CrashShutdown", "57P02"))


def test_server_reported_sqlstate_outranks_the_driver_class_fallback() -> None:
    """A named driver class must not readmit an excluded SQLSTATE.

    Psycopg reports most server conditions through a SQLSTATE-specific
    subclass, but a wrapper or pool may surface the same state as a bare
    ``OperationalError``. If the class-name fallback outranked the SQLSTATE,
    the narrow 57P01 allowlist would be unenforceable and every
    operator-intervention and resource-limit state would silently retry.
    """

    for excluded_state in ("57P02", "57P03", "53300", "53400"):
        bare_driver_error = _driver_error("OperationalError", excluded_state)

        assert is_run_store_error(bare_driver_error)
        assert not is_run_store_retryable_error(bare_driver_error)


def test_driver_connectivity_failure_without_sqlstate_remains_retryable() -> None:
    """Client-side connectivity loss carries no SQLSTATE and still retries."""

    for class_name in ("OperationalError", "InterfaceError"):
        connectivity_failure = _driver_error(class_name, None)

        assert is_run_store_error(connectivity_failure)
        assert is_run_store_retryable_error(connectivity_failure)


def test_connection_exception_class_stays_retryable_for_either_shape() -> None:
    assert is_run_store_retryable_error(_driver_error("OperationalError", "08006"))
    assert is_run_store_retryable_error(_driver_error("ConnectionFailure", "08006"))
