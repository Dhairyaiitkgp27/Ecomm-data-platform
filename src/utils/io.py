"""S3 / MinIO helpers plus a small run-metrics container.

Uses boto3 against the MinIO endpoint. Kept deliberately thin: ingestion and
Spark both use these helpers so there is one place that knows how to talk to the
object store, and one place that formats the "records in/out/rejected" metrics
that every stage logs.
"""

from __future__ import annotations

import io
import time
from dataclasses import dataclass, field
from typing import Iterable

from src.utils.config import get_settings
from src.utils.logging_config import get_logger

log = get_logger("utils.io")


def s3_client():
    """Return a boto3 S3 client pointed at the configured (MinIO) endpoint."""
    import boto3  # imported lazily so non-ingestion code needn't have it

    s = get_settings()
    return boto3.client(
        "s3",
        endpoint_url=s.s3_endpoint,
        aws_access_key_id=s.s3_access_key,
        aws_secret_access_key=s.s3_secret_key,
        region_name=s.s3_region,
    )


def ensure_bucket(client, bucket: str) -> None:
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
        log.info("created bucket %s", bucket)


def put_bytes(client, bucket: str, key: str, data: bytes) -> None:
    ensure_bucket(client, bucket)
    client.put_object(Bucket=bucket, Key=key, Body=data)


def list_keys(client, bucket: str, prefix: str = "") -> list[str]:
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def object_exists(client, bucket: str, key: str) -> bool:
    from botocore.exceptions import ClientError

    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


@dataclass
class RunMetrics:
    """Uniform metrics every pipeline stage emits, for interview-friendly logs."""

    stage: str
    files_discovered: int = 0
    records_processed: int = 0
    records_rejected: int = 0
    records_quarantined: int = 0
    records_loaded: int = 0
    _t0: float = field(default_factory=time.perf_counter)

    def summary(self) -> str:
        dur = time.perf_counter() - self._t0
        return (
            f"[{self.stage}] files={self.files_discovered} "
            f"processed={self.records_processed} rejected={self.records_rejected} "
            f"quarantined={self.records_quarantined} loaded={self.records_loaded} "
            f"duration={dur:.2f}s"
        )
