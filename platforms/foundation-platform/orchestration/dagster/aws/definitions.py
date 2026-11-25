"""The AWS code location (root ADR-0616 A1): Dagster runs on ECS here before any job does.

It holds nothing yet. The ai-server location (../definitions.py) observes Airflow and starts host units
through the forced command; none of that is ported (root ADR-0616 §0), so it is never loaded here.
Collectors move in one at a time from A2, each as assets of this location.
"""

from dagster import Definitions

defs = Definitions()
