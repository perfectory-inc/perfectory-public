"""Dagster runs a job as its systemd unit, through the host's one forced command (root ADR-0417).

systemd stays the executor and the security boundary (root ADR-0122 §2, ADR-0270): Dagster holds no
right on the host beyond the SSH key whose only command is scripts/ops/start-scheduled-job.sh, which
reads one job id, starts the job's service, streams that invocation's journal back and ends with the
run's `foundation-job-outcome` line and an exit status. Dagster Pipes (dagster.io/docs, "Dagster Pipes
details and customization": a custom message reader) is the client side of that stream:

- Every streamed line is the step's log as it arrives. A line that is a Pipes message (a JSON object with
  `__dagster_pipes_version`) is kept, and `JournalMessageReader` hands it to Dagster as the run reported
  it: a materialization, an asset check, a log. The session's `opened`/`closed` are the reader's own,
  never the stream's, so a job cannot end the session early or speak for another run.
- The last outcome line decides the run as it does for Airflow today (job_specs.job_outcome, root
  ADR-0260 §2): `changed` materializes the job's assets it did not report itself; `unchanged`,
  `waiting` and `skipped` materialize nothing, so nothing downstream starts (job_specs.WITHHOLDS_EVENTS);
  `failed`, or an exit status other than 0, fails the step with the line's reason.

So a job keeps its outcome line and gains Pipes reports one line at a time; nothing on the host changes.
"""

import json
import subprocess
from collections.abc import Iterator, Sequence
from contextlib import contextmanager

from dagster import Failure, PipesClient, PipesContextInjector, PipesMessageReader
from dagster._core.pipes.utils import open_pipes_session
from dagster_pipes import PIPES_PROTOCOL_VERSION, PIPES_PROTOCOL_VERSION_FIELD

import observe_assets

job_specs = observe_assets.job_specs

# What a job may report through its journal; the session's framing belongs to the reader.
REPORTED_METHODS = ("report_asset_materialization", "report_asset_check", "log", "report_custom_message")


def _message(method, params):
    return {PIPES_PROTOCOL_VERSION_FIELD: PIPES_PROTOCOL_VERSION, "method": method, "params": params}


def pipes_message(line):
    """The Pipes message `line` holds, or None when it is a log line. A message with a method the
    stream may not send is refused, not passed on as a log."""
    text = line.strip()
    if not text.startswith("{"):
        return None
    try:
        message = json.loads(text)
    except ValueError:
        return None
    if not isinstance(message, dict) or PIPES_PROTOCOL_VERSION_FIELD not in message:
        return None
    if message.get("method") not in REPORTED_METHODS:
        raise Failure(f"the job's journal sent Pipes method {message.get('method')!r}; it may send {REPORTED_METHODS}")
    return message


class NoContextInjector(PipesContextInjector):
    """The forced command reads a job id and nothing else: no Dagster context reaches the unit."""

    @contextmanager
    def inject_context(self, context_data) -> Iterator[dict]:
        yield {}

    def no_messages_debug_text(self) -> str:
        return "The host's forced command passes no context to the unit (root ADR-0417)."


class JournalMessageReader(PipesMessageReader):
    """Replays the Pipes messages one run's journal carried into the session, framed by the reader."""

    def __init__(self, messages):
        self.messages = list(messages)

    @contextmanager
    def read_messages(self, handler) -> Iterator[dict]:
        handler.handle_message(_message("opened", {}))
        try:
            yield {}
        finally:
            for message in self.messages:
                handler.handle_message(message)
            handler.handle_message(_message("closed", {}))

    def messages_are_readable(self, params) -> bool:
        return True

    def no_messages_debug_text(self) -> str:
        return "The run's journal, streamed by scripts/ops/start-scheduled-job.sh, carried no Pipes message."


def outcome_of(outcome_line, exit_status):
    """(outcome word, reason) of a run: its last outcome line; a non-zero exit is a failure."""
    word = job_specs.job_outcome(outcome_line)
    reason = outcome_line.split(" ", 2)[2] if outcome_line and outcome_line.count(" ") >= 2 else ""
    if exit_status != 0 and word != "failed":
        return "failed", reason or f"the forced command exited {exit_status}"
    return word, reason


class SystemdRun:
    """One finished run: its outcome, the Pipes messages its journal carried, and for the asset
    reconciler the runs it found owed (job_specs.owed_runs, what Airflow's `select` reads)."""

    def __init__(self, context, job_id, outcome, reason, messages, extras, owed=()):
        self.context, self.job_id, self.outcome, self.reason = context, job_id, outcome, reason
        self.messages, self.extras, self.owed = messages, extras, list(owed)

    def get_results(self):
        """What the asset yields. A run that withholds its events (`unchanged`, `waiting`, `skipped`)
        yields nothing: its skippable assets are not materialized and nothing downstream starts. A
        Pipes session is opened only for a `changed` run, since a session requires every selected
        asset to be reported; the run's own reports keep their metadata, the rest are implicit."""
        if self.outcome in job_specs.WITHHOLDS_EVENTS:
            return []
        with open_pipes_session(context=self.context, context_injector=NoContextInjector(),
                                message_reader=JournalMessageReader(self.messages),
                                extras=self.extras) as session:
            pass
        return list(session.get_results(implicit_materializations=True,
                                        metadata={"outcome": self.outcome, "job": self.job_id}))

    def materializations(self, asset_keys):
        """What an op that runs the job logs (run_jobs.py): one AssetMaterialization per output of a
        `changed` run, none for a run that withholds its events, as get_results does for an asset.

        An op is not an asset, so no Pipes session frames these: a job's outputs may be another job's
        too (a collector and its Silver refresh), which no asset definition can share. A materialization
        the journal reported for one of the outputs keeps its metadata (raw values, as Dagster infers
        them); a report for any other key is refused, so a run never speaks for a dataset it does not write.
        """
        from dagster import AssetKey, AssetMaterialization

        if self.outcome in job_specs.WITHHOLDS_EVENTS:
            return []
        keys = {AssetKey(list(key.path)): {} for key in asset_keys}
        for message in self.messages:
            params = message.get("params") or {}
            if message.get("method") == "report_asset_materialization":
                key = AssetKey.from_user_string(params.get("asset_key") or "")
                if key not in keys:
                    raise Failure(f"{self.job_id} reported a materialization of {key.to_user_string()}, which it does not write")
                keys[key].update({name: (entry or {}).get("raw_value") for name, entry in (params.get("metadata") or {}).items()})
            elif message.get("method") == "log":
                self.context.log.info(str(params.get("message", "")))
        return [AssetMaterialization(key, metadata={**reported, "outcome": self.outcome, "job": self.job_id})
                for key, reported in keys.items()]


class SubprocessForcedCommand:
    """The forced command as a local argv, its request the last argument: the tests' stand-in for
    the host (run_jobs.ForcedCommand reaches the real one over SSH)."""

    def __init__(self, command: Sequence[str]):
        self.command = list(command)

    def start(self, request):
        """(the run's output lines, a function that waits for it and returns its exit status)."""
        process = subprocess.Popen([*self.command, request], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, encoding="utf-8", errors="replace")
        return process.stdout, process.wait


class SystemdUnitPipesClient(PipesClient):
    """Starts a job through the host's forced command and reads its run.

    The forced command is reached through `transport` (its `start(request)`), or a local argv
    `command` that receives the request as its last argument. The request is the job id and its
    kind, as Airflow's SSHOperator sends them: nothing for a clock start, `triggered`, or
    `requested <id>` (job_specs.RUN_KIND). Every line is the step's log as it arrives; a Pipes
    message line is kept for the session, and the reconciler's owed lines for SystemdRun.owed.
    """

    def __init__(self, command: Sequence[str] | None = None, *, transport=None):
        self.transport = transport if transport is not None else SubprocessForcedCommand(command)

    def run(self, *, context, job_id, triggered=True, extras=None, kind=None) -> SystemdRun:
        jobs = observe_assets.contracts()[0]
        listed = {job["id"]: job for job in jobs["jobs"]}
        if job_id not in listed:
            raise Failure(f"{job_id} is not a job of jobs.v1.json")
        kind = ("triggered" if triggered else "") if kind is None else kind
        if not job_specs.RUN_KIND.fullmatch(kind):
            raise Failure(f"{job_id}: {kind[:40]!r} is not a run kind the forced command reads")
        messages, outcome_line, owed = [], None, []
        lines, wait = self.transport.start(job_id + (f" {kind}" if kind else ""))
        for line in lines:
            message = pipes_message(line)
            if message is not None:
                messages.append(message)
                continue
            outcome_line = job_specs.outcome_line(line) or outcome_line
            if job_specs.OWED_LINE.match(line.rstrip("\n")):
                owed.append(line)
            context.log.info(line.rstrip("\n"))
        word, reason = outcome_of(outcome_line, wait())
        if word == "failed":
            raise Failure(f"{job_id} failed: {reason}", metadata={"outcome": word, "reason": reason})
        context.log.info(f"{job_id} ended {word}{(': ' + reason) if reason else ''}")
        return SystemdRun(context, job_id, word, reason, messages, extras,
                          job_specs.owed_runs("".join(owed), tuple(listed[job_id].get("reconciles", ()))
                                              + job_specs.requeued(jobs, listed[job_id])))
