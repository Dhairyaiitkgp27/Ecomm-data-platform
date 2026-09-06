"""Tests for env-driven settings."""
from __future__ import annotations

import importlib

import src.utils.config as cfg


def test_defaults(monkeypatch):
    for k in ["POSTGRES_HOST", "S3_ENDPOINT", "POSTGRES_PORT"]:
        monkeypatch.delenv(k, raising=False)
    importlib.reload(cfg)
    s = cfg.Settings()
    assert s.pg_host == "localhost"
    assert s.pg_port == 5432
    assert s.jdbc_url.startswith("jdbc:postgresql://")


def test_env_override(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "db.internal")
    monkeypatch.setenv("POSTGRES_PORT", "6000")
    importlib.reload(cfg)
    s = cfg.Settings()
    assert s.pg_host == "db.internal"
    assert s.pg_port == 6000
    assert "db.internal:6000" in s.jdbc_url
