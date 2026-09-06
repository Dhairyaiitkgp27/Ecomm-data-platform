"""Central, environment-driven configuration.

Nothing in the codebase hardcodes credentials or absolute paths. Everything is
read from environment variables (which docker-compose / .env supply), with
sensible local-dev defaults so the code also runs outside Docker.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class Settings:
    # --- object store (MinIO / S3) ---------------------------------------- #
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://localhost:9000"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", "minioadmin"))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", "minioadmin"))
    s3_region: str = field(default_factory=lambda: _env("S3_REGION", "us-east-1"))
    bucket_bronze: str = field(default_factory=lambda: _env("BUCKET_BRONZE", "bronze"))
    bucket_silver: str = field(default_factory=lambda: _env("BUCKET_SILVER", "silver"))
    bucket_quarantine: str = field(default_factory=lambda: _env("BUCKET_QUARANTINE", "quarantine"))

    # --- warehouse (PostgreSQL) ------------------------------------------- #
    pg_host: str = field(default_factory=lambda: _env("POSTGRES_HOST", "localhost"))
    pg_port: int = field(default_factory=lambda: _env_int("POSTGRES_PORT", 5432))
    pg_db: str = field(default_factory=lambda: _env("POSTGRES_DB", "ecommerce"))
    pg_user: str = field(default_factory=lambda: _env("POSTGRES_USER", "warehouse"))
    pg_password: str = field(default_factory=lambda: _env("POSTGRES_PASSWORD", "warehouse"))
    pg_staging_schema: str = field(default_factory=lambda: _env("PG_STAGING_SCHEMA", "staging"))

    # --- paths (relative, resolved from PROJECT_ROOT) --------------------- #
    project_root: str = field(default_factory=lambda: _env("PROJECT_ROOT", os.getcwd()))
    raw_data_dir: str = field(default_factory=lambda: _env("RAW_DATA_DIR", "data/raw"))

    # --- run behaviour ----------------------------------------------------- #
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))
    dq_fail_fast: bool = field(default_factory=lambda: _env("DQ_FAIL_FAST", "true").lower() == "true")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}"

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
