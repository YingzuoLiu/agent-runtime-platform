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


def test_other_operator_intervention_is_not_broadly_made_retryable() -> None:
    assert not is_run_store_retryable_error(_SqlstateError("57P02"))
