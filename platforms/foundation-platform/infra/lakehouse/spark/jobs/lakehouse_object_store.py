#!/usr/bin/env python3
"""Let `spark.read` open an object in R2 that is not part of an Iceberg table.

Iceberg reaches R2 through its own storage layer, configured per catalog. A plain
`spark.read.json("s3a://...")` does not: it goes through a Hadoop filesystem, which is a
different implementation with its own settings and its own credentials. That is why a job can
write an Iceberg table into a bucket and still be unable to read a JSONL object out of the same
bucket — measured 2026-08-31, the Spark image carries 252 jars and not one of them is aws, s3,
or hadoop-cloud.

The settings live here rather than in each job because the catalog settings did not, and eight
copies of those had already drifted into three different key sets by the time anyone counted.

No PySpark import at module scope: the lane that runs `infra/lakehouse/spark/tests` has no
PySpark install, and a module-level import would make every check that touches this file skip
itself — which reports the same green as passing.
"""

from __future__ import annotations

import os
from typing import Any

ENDPOINT_ENV = "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT"
ACCESS_KEY_ENV = "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID"
SECRET_KEY_ENV = "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY"

# The reader credentials, not the writer ones. A job reads its input through this filesystem
# and writes its output through Iceberg, so a writer key here would grant the read path a
# power it never uses — and the bucket it would grant it over is the one holding every table.
CREDENTIAL_ENVS: tuple[str, ...] = (ENDPOINT_ENV, ACCESS_KEY_ENV, SECRET_KEY_ENV)

S3A_SCHEME = "s3a://"


def is_object_store_path(path: str) -> bool:
    """Return whether a job input names an object rather than a file."""
    return path.startswith(S3A_SCHEME)


def object_store_settings(lookup: Any = os.getenv) -> dict[str, str]:
    """Return the Hadoop settings that make `s3a://` resolve against R2.

    Raises when a credential is missing rather than letting Spark fail later with an access
    error that names neither the variable nor the bucket.
    """
    values = {}
    for name in CREDENTIAL_ENVS:
        value = lookup(name)
        if value is None or not str(value).strip():
            raise ValueError(
                f"{name} is required to read an {S3A_SCHEME} input; "
                "the R2 endpoint and reader credentials all come from the environment"
            )
        values[name] = str(value).strip()

    return {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": values[ENDPOINT_ENV],
        "spark.hadoop.fs.s3a.access.key": values[ACCESS_KEY_ENV],
        "spark.hadoop.fs.s3a.secret.key": values[SECRET_KEY_ENV],
        # R2 addresses buckets by path, not by subdomain. Left at the S3 default, every
        # request goes to a host that does not exist and the error is a DNS failure.
        "spark.hadoop.fs.s3a.path.style.access": "true",
        # R2 has one region and rejects a signature computed for another. The value is not
        # read from the environment because there is nothing to choose.
        "spark.hadoop.fs.s3a.endpoint.region": "auto",
    }


def apply_object_store_settings(builder: Any, lookup: Any = os.getenv) -> Any:
    """Apply the settings to a `SparkSession.builder` and return it."""
    for key, value in object_store_settings(lookup).items():
        builder = builder.config(key, value)
    return builder
