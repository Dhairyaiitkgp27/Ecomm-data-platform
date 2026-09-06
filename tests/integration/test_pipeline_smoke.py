"""
Integration smoke test: generate -> write CSV -> run the data-quality gate.

Exercises the generator, the schema layer and the full quality suite together
(no Spark / Postgres required), which is the part of the pipeline that runs
purely in Python. Confirms the gate produces a report, quarantines rows and
returns a pass/fail decision without raising.
"""
from __future__ import annotations

from generator.config import GeneratorConfig
from generator.generate_data import generate, write_csv
from src.quality.run_quality import run as run_gate


def test_generate_and_quality_gate(tmp_path):
    cfg = GeneratorConfig().scaled(0.003)
    cfg.n_orders = 600
    cfg.seed = 11
    tables = generate(cfg)
    raw_dir = tmp_path / "raw"
    write_csv(tables, str(raw_dir))

    # all eight files exist
    for name in tables:
        assert (raw_dir / f"{name}.csv").exists()

    quarantine = tmp_path / "quarantine"
    ok = run_gate(raw_dir=str(raw_dir), quarantine_dir=str(quarantine), fail_fast=False)
    assert ok is True  # report-only mode never fails

    # the gate should have quarantined at least the orphan order_items
    assert (quarantine / "order_items_quarantine.csv").exists()
