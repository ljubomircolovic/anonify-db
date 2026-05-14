# -*- coding: utf-8 -*-
"""String normalization helpers for user-provided identifiers."""

from __future__ import annotations


def normalize_name_fragment(raw_value: str) -> str:
    """Return a filesystem- and SQL-friendly slug derived from ``raw_value``.

    The result contains only ASCII letters, digits, and underscores. Any
    other character is replaced with ``"_"``; repeated underscores are
    collapsed and leading or trailing underscores are stripped.

    Parameters
    ----------
    raw_value:
        Arbitrary user text (plan fragment, table nickname, etc.).

    Returns
    -------
    str
        Sanitized fragment safe to embed in generated database names.
    """
    sanitized = "".join(
        ch if (ch.isalnum() or ch == "_") else "_" for ch in str(raw_value or "").strip().lower()
    )
    while "__" in sanitized:
        sanitized = sanitized.replace("__", "_")
    return sanitized.strip("_")
