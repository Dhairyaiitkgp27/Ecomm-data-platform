"""Shared pytest fixtures."""
from __future__ import annotations

import pandas as pd
import pytest

from generator.config import GeneratorConfig
from generator.generate_data import generate


@pytest.fixture(scope="session")
def small_dataset() -> dict[str, pd.DataFrame]:
    """A tiny, deterministic dataset generated once per test session."""
    cfg = GeneratorConfig().scaled(0.004)
    cfg.n_orders = 800
    cfg.seed = 7
    return generate(cfg)
