from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence

import uvicorn

from .deployment import (
    RuntimeDeploymentConfigurationError,
    resolve_runtime_deployment_config,
)
from .structured_logging import uvicorn_json_log_config


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run or validate the portable Agent Runtime server process."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate bounded process inputs and print only non-secret metadata.",
    )
    return parser


def main(
    argv: Sequence[str] = (),
    *,
    environment: Mapping[str, str] | None = None,
) -> int:
    args = _parser().parse_args(list(argv))
    values = os.environ if environment is None else environment
    try:
        config = resolve_runtime_deployment_config(values)
    except RuntimeDeploymentConfigurationError as exc:
        print(
            json.dumps(
                {"status": "failed", "error": str(exc)},
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 2
    if args.check:
        print(
            json.dumps(
                {"status": "ok", "deployment": config.public_dict()},
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        workers=1,
        limit_concurrency=config.http_concurrency_limit,
        timeout_graceful_shutdown=config.server_graceful_shutdown_seconds,
        log_level=config.log_level,
        log_config=uvicorn_json_log_config(values, log_level=config.log_level),
        access_log=True,
        proxy_headers=False,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
