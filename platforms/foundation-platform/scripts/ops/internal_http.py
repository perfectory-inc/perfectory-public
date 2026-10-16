"""The one bounded retry the host's daily jobs use for the services they call (root ADR-0102's shape).

A daily job that asks DataHub GMS or Trino one question and stops on the first `503` turns a
blip into a failed day: foundation-data-quality.service failed on 2026-10-15 at 18:06 on one
`HTTP Error 503` while both services were healthy minutes later. Every such call goes through
here instead of growing a retry of its own:

- `call(operation, transient=...)` runs `operation()` again after a failure `transient` accepts,
  with equal-jitter exponential backoff (half the ceiling fixed, half random; the ceiling doubles
  from `min_wait` up to `max_wait`), and gives up after `attempts`.
- `open_json(request)` is urllib's request with that retry: a `5xx` that can pass (`502`, `503`,
  `504`), `429`, a refused or reset connection and a timeout are asked again; any other answer is
  final, so a wrong request still fails at once.
- `trino_transient(stderr)` names the Trino CLI failures that mean the server was not answering,
  never a query error.
"""

from __future__ import annotations

import json
import random
import socket
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, TypeVar

T = TypeVar("T")

RETRYABLE_STATUS = frozenset({429, 502, 503, 504})
# What the CLI (or `docker exec`) prints when no server answered; a query error prints none of these.
TRINO_TRANSIENT = (
    "statusCode=502", "statusCode=503", "statusCode=504", "SERVER_STARTING_UP",
    "Connection refused", "ConnectException", "Connection reset", "SocketTimeoutException",
    "Failed to connect", "is restarting, wait until the container is running", "is not running",
)


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int
    min_wait: float
    max_wait: float

    def delay(self, attempt: int, unit: float) -> float:
        """Seconds to wait after failed attempt `attempt` (1-based); `unit` in [0, 1) is the random part."""
        ceiling = min(self.max_wait, self.min_wait * (2 ** max(0, attempt - 1)))
        return ceiling / 2 + (ceiling / 2) * min(max(unit, 0.0), 1.0)


# About a minute of patience for a service on the same host: 1 s doubling to 30 s, six tries.
DAILY_JOB_POLICY = RetryPolicy(attempts=6, min_wait=1.0, max_wait=30.0)


def call(
    operation: Callable[[], T],
    *,
    transient: Callable[[BaseException], bool],
    describe: str,
    policy: RetryPolicy = DAILY_JOB_POLICY,
    sleep: Callable[[float], None] | None = None,
    unit: Callable[[], float] = random.random,
) -> T:
    """`operation()`, asked again after a transient failure; any other failure is raised at once."""
    sleep = sleep or time.sleep
    for attempt in range(1, policy.attempts + 1):
        try:
            return operation()
        except Exception as error:  # noqa: BLE001 - `transient` decides; the rest is raised as is
            if attempt == policy.attempts or not transient(error):
                raise
            wait = policy.delay(attempt, unit())
            print(f"internal-http: {describe} failed ({type(error).__name__}: {error}); "
                  f"attempt {attempt} of {policy.attempts}, again in {wait:.1f}s", file=sys.stderr, flush=True)
            sleep(wait)
    raise AssertionError("unreachable: the last attempt returns or raises")


def http_transient(error: BaseException) -> bool:
    if isinstance(error, urllib.error.HTTPError):
        return error.code in RETRYABLE_STATUS
    return isinstance(error, (urllib.error.URLError, ConnectionError, TimeoutError, socket.timeout))


def open_json(request: urllib.request.Request, timeout: float, **options: Any) -> Any:
    """The JSON a request answers, with the daily jobs' retry."""
    def once() -> Any:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)

    return call(once, transient=http_transient, describe=f"{request.get_method()} {request.full_url}", **options)


class TrinoUnavailable(RuntimeError):
    """The Trino CLI got no answer from the server. Not a `LookupError`: a Trino that stays down
    fails the run instead of making every table look absent."""


def trino_transient(stderr: str) -> bool:
    return any(marker in stderr for marker in TRINO_TRANSIENT)


def retry_trino(run_once: Callable[[], T], **options: Any) -> T:
    """`run_once()` asked again while it raises `TrinoUnavailable`."""
    return call(run_once, transient=lambda error: isinstance(error, TrinoUnavailable), describe="trino", **options)
