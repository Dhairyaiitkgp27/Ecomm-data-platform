"""Structured, consistent logging used across ingestion, quality and Spark."""

from __future__ import annotations

import logging
import os
import sys

_CONFIGURED = False


def get_logger(name: str) -> logging.Logger:
    global _CONFIGURED
    if not _CONFIGURED:
        level = os.environ.get("LOG_LEVEL", "INFO").upper()
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        root = logging.getLogger()
        root.setLevel(level)
        # avoid duplicate handlers if libraries also configure logging
        if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
            root.addHandler(handler)
        _CONFIGURED = True
    return logging.getLogger(name)
