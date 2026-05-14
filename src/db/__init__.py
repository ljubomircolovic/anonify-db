# -*- coding: utf-8 -*-
"""Database access package.

Re-exports :class:`DBManager` from ``src.database``. Read-heavy SQL lives under
``src.db.queries`` and is composed by ``DBManager`` for a thinner orchestration
layer (ongoing refactor).
"""

from __future__ import annotations

from src.database.db_manager import DBManager

__all__ = ["DBManager"]
