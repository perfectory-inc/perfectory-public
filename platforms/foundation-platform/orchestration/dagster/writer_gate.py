"""The writable Dagster webserver, answering only what its two callers may send (root ADR-0565 §8).

    writer_gate.py <dagster-webserver arguments>     serve, as `dagster-webserver` would
    writer_gate.py ask ready|reload|location-errors  the deploy's three questions, over the host's socket
    writer_gate.py ask pools < {"<pool>": <slots>}  sets Dagster's pool limits to the job list's (root ADR-0605)

Dagster OSS's webserver has no sign-in, and its GraphQL launches, terminates and stops. This runs the
same webserver (its own CLI, so every option and the workspace are Dagster's) and puts a gate in front
of each of its two listeners, from dagster_writes.py:

- TCP on the compose network, for Airflow: a proxied job's `run` task's documents (`airflow_allowed`).
- A unix socket (`WRITER_SOCKET`) in a directory only root and the container's uid open, for the host:
  the deploy's and an operator's run-now (`HOST`), and `GET /server_info` the deploy waits on.

A request the gate refuses is answered 403 with the reason, and Dagster never sees it; the UI, the
websocket and every other route are refused alike. Staff and every reader use the read-only webserver.
Copied into the image (orchestration/dagster/Dockerfile) with dagster_writes.py: a gate change is an
image change, so compose recreates the container that serves it.
"""

import asyncio
import json
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
for folder in (HERE, HERE.parent / "dags"):
    if folder.is_dir() and str(folder) not in sys.path:
        sys.path.insert(0, str(folder))

import dagster_writes  # noqa: E402


class Gate:
    """An ASGI app passing on only the requests `allowed` holds; `ready` also lets `GET /server_info` by."""

    def __init__(self, app, allowed, name, ready=False):
        self.app, self.allowed, self.name, self.ready = app, allowed, name, ready

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            await self.app(scope, receive, send)
            return
        if scope["type"] != "http":
            await send({"type": "websocket.close", "code": 1008})
            return
        body, more = b"", True
        while more:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body += message.get("body", b"")
            more = message.get("more_body", False)
            if len(body) > dagster_writes.MAX_BODY_BYTES:
                break
        reason = dagster_writes.refused(self.allowed, scope["method"], scope["path"], body, self.ready)
        if reason is not None:
            print(f"writer-gate: refused from {self.name}: {reason}", file=sys.stderr, flush=True)
            answer = json.dumps({"errors": [{"message": f"refused by the writer gate: {reason}"}]}).encode()
            await send({"type": "http.response.start", "status": 403,
                        "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(answer)).encode())]})
            await send({"type": "http.response.body", "body": answer})
            return
        replayed = False

        async def replay():
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


def serve(app, host, port, log_level, socket_path=None):
    """Both listeners in one event loop; when either stops (a signal reaches the last one installed),
    the other is told to stop too, so the container stops in its grace period."""
    import uvicorn  # noqa: PLC0415

    airflow = uvicorn.Server(uvicorn.Config(Gate(app, dagster_writes.airflow_allowed(), "airflow"),
                                            host=host, port=port, log_level=log_level))
    host_side = uvicorn.Server(uvicorn.Config(Gate(app, dagster_writes.HOST, "host", ready=True),
                                              uds=socket_path or dagster_writes.WRITER_SOCKET,
                                              log_level=log_level, lifespan="off"))

    async def both():
        tasks = [asyncio.ensure_future(airflow.serve()), asyncio.ensure_future(host_side.serve())]
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        airflow.should_exit = host_side.should_exit = True
        await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(both())


def run_webserver(argv):
    """Dagster's own `dagster-webserver` command, its server swapped for the two gated listeners."""
    import uvicorn  # noqa: PLC0415
    from dagster_webserver import cli  # noqa: PLC0415

    def gated(app, host, port, log_level, **_):
        serve(app, host, port, log_level)

    uvicorn.run = gated
    sys.argv = ["dagster-webserver", *argv]
    cli.main()


ASKS = {"reload": dagster_writes.RELOAD_LOCATION, "location-errors": dagster_writes.LOCATION_ERRORS}


def ask(question):
    """One of the deploy's questions over the host's socket; exit 0 when answered as hoped."""
    try:
        if question == "ready":
            status, _ = dagster_writes.ask("GET", "/server_info", timeout=10)
            return 0 if status == 200 else 1
        status, body = dagster_writes.ask("POST", "/graphql", {"query": ASKS[question], "variables": None})
        data = json.loads(body).get("data") or {}
    except (OSError, ValueError) as error:
        print(f"writer-gate: {question}: {error}", file=sys.stderr)
        return 1
    if question == "reload":
        return 0 if status == 200 and (data.get("reloadRepositoryLocation") or {}).get("__typename") == "WorkspaceLocationEntry" else 1
    entries = ((data.get("workspaceOrError") or {}).get("locationEntries")) or []
    if status != 200 or not entries:
        print(f"writer-gate: location-errors: answered {status} with no location", file=sys.stderr)
        return 1
    print("; ".join(entry["name"] + ": " + entry["locationOrLoadError"]["message"].splitlines()[0] for entry in entries
                    if (entry.get("locationOrLoadError") or {}).get("__typename") == "PythonError"))
    return 0


def graphql(document, variables=None):
    """One host document over the writer socket: its `data`, or OSError/ValueError with what went wrong."""
    status, body = dagster_writes.ask("POST", "/graphql", {"query": document, "variables": variables})
    answer = json.loads(body)
    if status != 200 or answer.get("errors") or not answer.get("data"):
        raise ValueError(f"answered {status}: {str(answer.get('errors') or body[:200])[:300]}")
    return answer["data"]


def pool_limits():
    """Pool name -> the limit set on it, None where Dagster took its default."""
    found = graphql(dagster_writes.POOL_LIMITS)["instance"]["concurrencyLimits"]
    return {entry["concurrencyKey"]: None if entry.get("usingDefaultLimit") else entry.get("limit") for entry in found}


def pools(declared):
    """Makes Dagster's pool limits the job list's `declared` ({pool: slots}): each pool set to its slots, a
    pool the list does not name deleted, then read back. Exit 0 when Dagster holds exactly the list's."""
    try:
        if not isinstance(declared, dict) or not all(
                isinstance(name, str) and dagster_writes.POOL.fullmatch(name) and type(slots) is int
                and 1 <= slots <= dagster_writes.MAX_POOL_LIMIT for name, slots in declared.items()):
            raise ValueError(f"not a job list's pools: {str(declared)[:200]}")
        held = pool_limits()
        for name, slots in sorted(declared.items()):
            if held.get(name) != slots:
                graphql(dagster_writes.SET_POOL_LIMIT, {"concurrencyKey": name, "limit": slots})
        for name in sorted(set(held) - set(declared)):
            graphql(dagster_writes.DELETE_POOL_LIMIT, {"concurrencyKey": name})
        now = pool_limits()
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"writer-gate: pools: {error}", file=sys.stderr)
        return 1
    if now != declared:
        print(f"writer-gate: pools: Dagster holds {now}, the job list says {declared}", file=sys.stderr)
        return 1
    print("pools: " + " ".join(f"{name}={slots}" for name, slots in sorted(now.items())))
    return 0


def main(argv):
    if argv == ["ask", "pools"]:
        try:
            declared = json.load(sys.stdin)
        except ValueError as error:
            print(f"writer-gate: pools: stdin is not JSON: {error}", file=sys.stderr)
            return 64
        return pools(declared)
    if argv[:1] == ["ask"]:
        if len(argv) != 2 or argv[1] not in ("ready", *ASKS):
            print(__doc__.split("\n\n")[1], file=sys.stderr)
            return 64
        return ask(argv[1])
    run_webserver(argv)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
