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
    """One finished run: its outcome and the Pipes messages its journal carried."""

    def __init__(self, context, job_id, outcome, reason, messages, extras):
        self.context, self.job_id, self.outcome, self.reason = context, job_id, outcome, reason
        self.messages, self.extras = messages, extras

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


class SystemdUnitPipesClient(PipesClient):
    """Starts a job through the host's forced command and reads its run.

    `command` is the argv that reaches the forced command (`ssh -i <key> foundation-scheduler@<host>`);
    the job id and its kind are its last argument, as Airflow's SSHOperator sends them. Every line is
    the step's log as it arrives; a Pipes message line is kept for the session.
    """

    def __init__(self, command: Sequence[str]):
        self.command = list(command)

    def run(self, *, context, job_id, triggered=True, extras=None) -> SystemdRun:
        if job_id not in {job["id"] for job in observe_assets.contracts()[0]["jobs"]}:
            raise Failure(f"{job_id} is not a job of jobs.v1.json")
        messages, outcome_line = [], None
        process = subprocess.Popen([*self.command, job_id + (" triggered" if triggered else "")],
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                   encoding="utf-8", errors="replace")
        for line in process.stdout:
            message = pipes_message(line)
            if message is not None:
                messages.append(message)
                continue
            outcome_line = job_specs.outcome_line(line) or outcome_line
            context.log.info(line.rstrip("\n"))
        word, reason = outcome_of(outcome_line, process.wait())
        if word == "failed":
            raise Failure(f"{job_id} failed: {reason}", metadata={"outcome": word, "reason": reason})
        context.log.info(f"{job_id} ended {word}{(': ' + reason) if reason else ''}")
        return SystemdRun(context, job_id, word, reason, messages, extras)
