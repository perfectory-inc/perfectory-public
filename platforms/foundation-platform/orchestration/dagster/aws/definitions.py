"""The AWS code location (root ADR-0616 A1): Dagster runs on ECS here before any job does.

It holds no job yet, only the alarms that report a failed run or a stale asset of this location
(alerts.py). It is served by its own gRPC code server (infra/aws/code-servers.tf). The ai-server location
(../definitions.py) observes Airflow and starts host units through the forced command; none of that is
ported (root ADR-0616 §0), so it is never loaded here. Collectors move in one at a time from A2, each as
assets of this location.
"""

import pathlib
import sys

from dagster import Definitions

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import alerts  # noqa: E402

defs = Definitions.merge(Definitions(), alerts.definitions())
