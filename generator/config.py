"""Configuration for the synthetic data generator.

Defaults produce a laptop-friendly full dataset (~200k orders). The committed
sample in data/sample is generated at a much smaller scale via CLI overrides so
the repository stays small. Every number here is overridable from the command
line (see generate_data.py) or from configs/data_generation.yml when PyYAML is
installed.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class GeneratorConfig:
    # --- volumes (full run defaults) --------------------------------------- #
    n_orders: int = 200_000
    n_customers: int = 80_000
    n_products: int = 15_000
    n_sellers: int = 2_000

    # --- time window ------------------------------------------------------- #
    start_date: str = "2023-01-01"
    end_date: str = "2024-12-31"

    # --- behaviour --------------------------------------------------------- #
    max_items_per_order: int = 6
    repeat_customer_zipf_a: float = 1.6   # >1; higher => more concentrated repeats
    festival_uplift: float = 2.8          # multiplier during festival windows
    weekend_uplift: float = 1.25

    # --- data-quality defect rates (fraction of affected rows) ------------- #
    dq_duplicate_orders: float = 0.005
    dq_duplicate_customers: float = 0.003
    dq_null_city: float = 0.02
    dq_null_payment_value: float = 0.01
    dq_null_delivered_ts: float = 0.02
    dq_dirty_city_names: float = 0.06
    dq_dirty_categories: float = 0.04
    dq_dirty_payment_status: float = 0.10
    dq_invalid_dates: float = 0.01
    dq_bad_quantity: float = 0.005        # zero / negative quantity
    dq_negative_price: float = 0.002
    dq_orphan_order_items: float = 0.003  # order_item pointing at missing order

    # --- io ---------------------------------------------------------------- #
    output_dir: str = "data/raw"
    seed: int = 42

    def scaled(self, factor: float) -> "GeneratorConfig":
        """Return a copy scaled down/up by a factor (customers/products/sellers
        scale sub-linearly so ratios stay realistic)."""
        c = GeneratorConfig(**asdict(self))
        c.n_orders = max(200, int(self.n_orders * factor))
        c.n_customers = max(100, int(self.n_customers * factor ** 0.9))
        c.n_products = max(50, int(self.n_products * factor ** 0.8))
        c.n_sellers = max(20, int(self.n_sellers * factor ** 0.7))
        return c


def load_yaml_overrides(path: str | Path, base: GeneratorConfig) -> GeneratorConfig:
    """Overlay a YAML file onto the base config if PyYAML is available."""
    try:
        import yaml  # type: ignore
    except ImportError:
        return base
    p = Path(path)
    if not p.exists():
        return base
    data = yaml.safe_load(p.read_text()) or {}
    merged = {**asdict(base), **{k: v for k, v in data.items() if k in asdict(base)}}
    return GeneratorConfig(**merged)
