# -*- coding: utf-8 -*-
"""Read-oriented SQL helpers used by :class:`src.database.db_manager.DBManager`.

Subpackages:
    ``schema_queries`` — schemas, tables, ``SELECT`` previews.
    ``data_discovery`` — foreign keys, PKs, execution order, samples.
    ``metadata_reads`` — audit log reads and connectivity probes.
"""

from __future__ import annotations

__all__ = ["data_discovery", "metadata_reads", "schema_queries"]
