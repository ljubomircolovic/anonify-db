# -*- coding: utf-8 -*-
"""Constants shared by Source configuration logic and UI."""

from __future__ import annotations

from typing import Final

DOMAIN_OPTIONS: Final[tuple[str, ...]] = (
    "Customer Data",
    "Financial Records",
    "E-commerce",
    "Healthcare",
    "Custom",
    "Other",
)

SOURCE_TYPES: Final[tuple[str, ...]] = ("Database", "File", "API")
