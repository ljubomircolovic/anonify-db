# -*- coding: utf-8 -*-
"""Build a logical "mirrored" SQL string for the Source vs Anon preview.

Uses ``sqlglot`` to rewrite schema-qualified tables and equality / ``IN`` literals
for columns governed by non-``keep`` strategies, delegating value transforms to
``DBManager.apply_anonymization_rules`` so preview SQL stays aligned with the
in-memory anonymization pipeline.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Mapping, MutableMapping, Optional

import pandas as pd

from src.db.services.ddl_manager import DdlManager

logger = logging.getLogger(__name__)

MIRROR_SQL_ROW_CAP = 10


def _append_outer_limit_if_missing(sql: str, limit_n: int = MIRROR_SQL_ROW_CAP) -> str:
    """Ensure the mirrored SQL string ends with ``LIMIT n`` when the outer query has none."""
    s = str(sql or "").strip().rstrip(";")
    if re.search(r"\bLIMIT\s+\d+\s*$", s, flags=re.I | re.S):
        return str(sql).strip()
    return s + f" LIMIT {int(limit_n)}"


try:
    import sqlglot
    from sqlglot import exp
except ImportError:  # pragma: no cover - enforced via requirements.txt
    sqlglot = None  # type: ignore[assignment]
    exp = None  # type: ignore[assignment]


def _table_plan_cache_key(schema_name: str, table_name: str) -> str:
    return f"{schema_name}.{table_name}"


def _mirror_plan_rows_for_table(
    session: Mapping[str, Any],
    db: Any,
    schema_name: str,
    table_name: str,
) -> list[dict[str, Any]]:
    """Session intent (editor + active_plan) merged over saved plan — same rules as the mirror tab."""
    skey = _table_plan_cache_key(schema_name, table_name)

    session_rows: list[dict[str, Any]] | None = None
    cpd = session.get("current_plan_data") or {}
    if isinstance(cpd, dict):
        block = cpd.get(skey)
        if isinstance(block, dict) and isinstance(block.get("plan"), list) and block["plan"]:
            session_rows = [dict(r) for r in block["plan"] if isinstance(r, dict)]

    if session_rows is None:
        active_plan = session.get("active_plan") or {}
        if isinstance(active_plan, dict):
            block = active_plan.get(table_name)
            if isinstance(block, dict) and isinstance(block.get("mappings"), list) and block["mappings"]:
                session_rows = [dict(r) for r in block["mappings"] if isinstance(r, dict)]
            else:
                block = active_plan.get(skey)
                if isinstance(block, dict) and isinstance(block.get("mappings"), list) and block["mappings"]:
                    session_rows = [dict(r) for r in block["mappings"] if isinstance(r, dict)]

    if session_rows is None:
        legacy = (session.get("active_plan_by_table") or {}).get(skey)
        if isinstance(legacy, list) and legacy:
            session_rows = [dict(r) for r in legacy if isinstance(r, dict)]

    saved = db.get_saved_plan(schema_name, table_name)
    saved_rows: list[dict[str, Any]] = []
    if saved and isinstance(saved.get("plan"), list):
        saved_rows = [dict(r) for r in saved["plan"] if isinstance(r, dict)]

    if not session_rows:
        return saved_rows
    if not saved_rows:
        return list(session_rows)

    by_col: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for r in saved_rows:
        col = str(r.get("column", "")).strip()
        if not col:
            continue
        by_col[col] = dict(r)
        order.append(col)
    for r in session_rows:
        col = str(r.get("column", "")).strip()
        if not col:
            continue
        if col in by_col:
            by_col[col] = {**by_col[col], **r}
        else:
            by_col[col] = dict(r)
            order.append(col)
    return [by_col[c] for c in order]


def resolve_export_target_identifier(session: Mapping[str, Any], db: Any) -> str:
    """Identifier shown on Export dashboard (bound plan target DB name / path segment)."""
    from urllib.parse import urlparse

    plan_name = str(session.get("active_plan_db_name", "None") or "None").strip()
    connected_plan = str(session.get("connected_plan_db_name", "None") or "None").strip()
    target_url = str(getattr(db, "target_db_url", "") or "").strip()
    source_url = str(getattr(db, "source_db_url", "") or "").strip()
    target_bound = (
        plan_name not in ("", "None")
        and connected_plan == plan_name
        and bool(target_url)
        and target_url != source_url
    )
    if not target_bound:
        return ""
    target_parsed = urlparse(target_url) if target_url else None
    target_db_display = (
        target_parsed.path.lstrip("/")
        if target_parsed and target_parsed.path
        else (plan_name if plan_name not in ("", "None") else "")
    )
    return str(target_db_display or "").strip()


def resolve_anonymized_target_schema(
    session: Mapping[str, Any],
    destination_mode: str,
    source_schema: str,
    db: Any,
) -> str:
    """Logical namespace for mirrored SQL — aligned with Export UI / plan target (no hardcoded ``anon``)."""
    if destination_mode != "database":
        return "__mirror_preview__"
    for key in (
        session.get("mirror_sql_target_schema"),
        session.get("export_target"),
        session.get("export_target_schema"),
        session.get("anon_write_schema"),
        (session.get("plan_metadata") or {}).get("anon_target_schema"),
        (session.get("plan_metadata") or {}).get("target_anon_schema"),
        (session.get("plan_metadata") or {}).get("anonymized_schema"),
    ):
        if isinstance(key, str) and key.strip():
            return key.strip()
    resolved = resolve_export_target_identifier(session, db)
    if resolved:
        return resolved
    logger.warning(
        "query_mirror: no export target in session and plan not bound; using logical preview namespace"
    )
    return "__mirror_preview__"


def _ident_name(node: Any) -> str:
    if node is None:
        return ""
    if isinstance(node, str):
        return node.strip('"').strip()
    name = getattr(node, "name", None)
    if isinstance(name, str):
        return name.strip('"').strip()
    this = getattr(node, "this", None)
    if isinstance(this, str):
        return this.strip('"').strip()
    return str(node).strip('"').strip()


def _should_attempt_literal_mirror(row: dict[str, Any]) -> bool:
    """Mirror WHERE literals for any non-``keep`` strategy (aligned with plan execution)."""
    return str(row.get("strategy", "keep")).lower().strip() != "keep"


def _strategy_is_faker(strategy: str) -> bool:
    return strategy.lower().strip() in {"faker_name", "faker_email", "faker_phone"}


def _deterministic_faker_value(strategy: str, column: str, raw_val: Any, salt: str) -> str:
    import hashlib

    from faker import Faker

    seed_bytes = hashlib.sha256(f"{strategy}|{column}|{raw_val!s}|{salt}".encode()).digest()
    seed_int = int.from_bytes(seed_bytes[:8], "big", signed=False)
    fake = Faker()
    fake.seed_instance(seed_int)
    if strategy == "faker_name":
        return str(fake.name())
    if strategy == "faker_email":
        return str(fake.email())
    if strategy == "faker_phone":
        return str(fake.phone_number())
    return str(raw_val)


def _deterministic_noise_value(raw_val: Any, column: str, salt: str) -> Any:
    import hashlib
    import random

    seed = int.from_bytes(
        hashlib.sha256(f"{column}|{raw_val!s}|{salt}".encode()).digest()[:8],
        "big",
        signed=False,
    )
    rng = random.Random(seed)
    try:
        fv = float(raw_val)
        return fv + fv * rng.uniform(-0.1, 0.1)
    except (TypeError, ValueError):
        return raw_val


def _deterministic_date_shift_value(raw_val: Any, column: str, salt: str) -> Any:
    import hashlib
    from datetime import timedelta

    if raw_val is None or (isinstance(raw_val, float) and pd.isna(raw_val)):
        return raw_val
    seed = int.from_bytes(
        hashlib.sha256(f"{column}|{raw_val!s}|{salt}".encode()).digest()[:8],
        "big",
        signed=False,
    )
    days = (seed % 61) - 30
    try:
        ts = pd.Timestamp(raw_val)
        return ts + timedelta(days=int(days))
    except Exception:
        return raw_val


def _column_sql_name(col: exp.Column) -> str:
    an = getattr(col, "alias_or_name", None)
    s_an = _ident_name(an) if an is not None else ""
    if s_an:
        return s_an
    nm = getattr(col, "name", None)
    s_nm = _ident_name(nm) if nm is not None else ""
    if s_nm:
        return s_nm
    return _ident_name(col.args.get("this"))


def _owner_select_for_node(node: Any) -> Any:
    """Nearest ancestor ``Select`` for an expression (sqlglot sets ``parent`` after parse)."""
    p = getattr(node, "parent", None)
    while p is not None:
        if isinstance(p, exp.Select):
            return p
        p = getattr(p, "parent", None)
    return None


def _as_column_ref(node: Any) -> exp.Column | None:
    """Normalize ``Column`` or qualified ``Dot`` (e.g. ``t1.first_name``) to ``exp.Column``."""
    if isinstance(node, exp.Column):
        return node
    if isinstance(node, exp.Dot):
        tbl = _ident_name(node.this)
        col = _ident_name(node.expression)
        if not tbl or not col:
            return None
        return exp.Column(this=exp.Identifier(this=col, quoted=False), table=exp.Identifier(this=tbl, quoted=False))
    return None


def _in_list_expressions(inn: exp.In) -> list[exp.Expression]:
    raw = inn.args.get("expressions")
    if isinstance(raw, list):
        return list(raw)
    if isinstance(raw, exp.Tuple):
        return list(raw.expressions)
    if isinstance(raw, exp.Paren) and raw.this:
        inner = raw.this
        if isinstance(inner, exp.Tuple):
            return list(inner.expressions)
    if raw is None:
        return []
    return [raw]


def _policy_for_column(
    session: Mapping[str, Any],
    db: Any,
    schema: str,
    physical_table: str,
    column: str,
) -> dict[str, Any] | None:
    for r in _mirror_plan_rows_for_table(session, db, schema, physical_table):
        if not isinstance(r, dict):
            continue
        if str(r.get("column", "")).strip().lower() == column.lower():
            return r
    return None


def _resolve_physical_table(
    alias_map: dict[str, tuple[str, str]],
    default_schema: str,
    col: exp.Column,
    single_table: tuple[str, str] | None,
) -> tuple[str, str] | None:
    tbl = col.table
    if tbl:
        key = _ident_name(tbl).lower()
        return alias_map.get(key)
    if single_table:
        return single_table
    if len(alias_map) == 1:
        return next(iter(alias_map.values()))
    return None


def _register_table(
    t: exp.Table,
    default_schema: str,
    alias_map: MutableMapping[str, tuple[str, str]],
) -> None:
    schema = _ident_name(t.args.get("catalog")) or _ident_name(t.args.get("db")) or default_schema
    table_name = _ident_name(t.name)
    if not table_name:
        return
    alias_obj = t.args.get("alias")
    alias_name = ""
    if alias_obj is not None:
        if isinstance(alias_obj, exp.TableAlias):
            alias_name = _ident_name(alias_obj.this)
        else:
            alias_name = _ident_name(alias_obj)
    keys = [k for k in (alias_name, table_name) if k]
    for k in keys:
        alias_map[k.lower()] = (schema, table_name)


def _collect_alias_map(select: exp.Select, default_schema: str) -> dict[str, tuple[str, str]]:
    alias_map: dict[str, tuple[str, str]] = {}
    frm = select.args.get("from")
    if not isinstance(frm, exp.From):
        return alias_map
    if frm.this:
        node = frm.this
        if isinstance(node, exp.Table):
            _register_table(node, default_schema, alias_map)
        elif isinstance(node, exp.Subquery) and isinstance(node.this, exp.Select):
            # Skip registering subquery alias as physical table for literals v1
            pass
    for j in frm.args.get("joins") or []:
        if isinstance(j, exp.Join) and isinstance(j.this, exp.Table):
            _register_table(j.this, default_schema, alias_map)
    return alias_map


def _single_implicit_table(alias_map: dict[str, tuple[str, str]]) -> tuple[str, str] | None:
    physical = {v for v in alias_map.values()}
    if len(physical) == 1:
        return next(iter(physical))
    return None


def _anonymize_literal_with_plan(
    db: Any,
    plan_row: dict[str, Any],
    raw_py_value: Any,
    salt: str,
) -> tuple[Any, str | None]:
    """Return (python_value_for_sql, optional_note). Uses plan salt for stable literals."""
    strat = str(plan_row.get("strategy", "keep")).lower().strip()
    col = str(plan_row.get("column", "")).strip()
    if not col:
        return raw_py_value, None

    if _strategy_is_faker(strat):
        return _deterministic_faker_value(strat, col, raw_py_value, salt), None

    if strat == "noise":
        return _deterministic_noise_value(raw_py_value, col, salt), None

    if strat == "date_shift":
        return _deterministic_date_shift_value(raw_py_value, col, salt), None

    try:
        df = pd.DataFrame({col: [raw_py_value]})
        out = db.apply_anonymization_rules(df, [plan_row], salt=salt)
        return out[col].iloc[0], None
    except Exception as exc:  # noqa: BLE001
        logger.warning("query_mirror: literal anonymization failed for %s: %s", col, exc)
        return raw_py_value, None


def _literal_to_sqlglot(val: Any) -> exp.Expression:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return exp.Null()
    if isinstance(val, pd.Timestamp):
        return exp.Literal.string(val.isoformat())
    try:
        from datetime import date, datetime

        if isinstance(val, (datetime, date)):
            return exp.Literal.string(val.isoformat())
    except Exception:
        pass
    if isinstance(val, bool):
        try:
            return exp.Boolean(this=val)
        except Exception:
            return exp.Literal.string("TRUE" if val else "FALSE")
    if isinstance(val, (int,)) and not isinstance(val, bool):
        return exp.Literal.number(int(val))
    if isinstance(val, float):
        return exp.Literal.number(float(val))
    return exp.Literal.string(str(val))


def _extract_python_literal(lit: exp.Literal) -> Any:
    if lit.is_string:
        return str(lit.this)
    raw = lit.this
    if raw is None:
        return None
    s = str(raw).lower()
    if s in ("true", "false"):
        return s == "true"
    try:
        if "." in str(raw):
            return float(raw)
        return int(raw)
    except (TypeError, ValueError):
        return raw


def _rewrite_literals_in_expression(
    expr: exp.Expression,
    *,
    session: Mapping[str, Any],
    db: Any,
    default_schema: str,
    salt: str,
    notes: list[str],
) -> int:
    """Rewrite EQ / IN literals under ``expr`` (one ``Select``). Returns number of literals changed."""
    if exp is None:
        return 0
    changed = 0
    alias_map: dict[str, tuple[str, str]] = {}
    single_tbl = None
    if isinstance(expr, exp.Select):
        alias_map = _collect_alias_map(expr, default_schema)
        single_tbl = _single_implicit_table(alias_map)

    for eq in list(expr.find_all(exp.EQ)):
        if isinstance(expr, exp.Select):
            owner = _owner_select_for_node(eq)
            if owner is not None and owner is not expr:
                continue
        lit_side: exp.Literal | None = None
        col_side: exp.Column | None = None
        left_c = _as_column_ref(eq.left)
        right_c = _as_column_ref(eq.right)
        if left_c is not None and isinstance(eq.right, exp.Literal):
            col_side, lit_side = left_c, eq.right
        elif right_c is not None and isinstance(eq.left, exp.Literal):
            col_side, lit_side = right_c, eq.left
        if lit_side is None or col_side is None:
            continue
        phys = _resolve_physical_table(alias_map, default_schema, col_side, single_tbl)
        if not phys:
            continue
        schema, table = phys
        col_name = _column_sql_name(col_side)
        policy = _policy_for_column(session, db, schema, table, col_name)
        if not policy or not _should_attempt_literal_mirror(policy):
            continue
        raw_val = _extract_python_literal(lit_side)
        new_val, note = _anonymize_literal_with_plan(db, policy, raw_val, salt)
        if note:
            notes.append(note)
        if new_val == raw_val:
            continue
        replacement = _literal_to_sqlglot(new_val)
        if isinstance(eq.right, exp.Literal):
            eq.set("right", replacement)
        else:
            eq.set("left", replacement)
        changed += 1

    for inn in list(expr.find_all(exp.In)):
        if isinstance(expr, exp.Select):
            owner = _owner_select_for_node(inn)
            if owner is not None and owner is not expr:
                continue
        col_side = _as_column_ref(inn.this)
        if col_side is None:
            continue
        rhs = _in_list_expressions(inn)
        if not rhs:
            continue
        phys = _resolve_physical_table(alias_map, default_schema, col_side, single_tbl)
        if not phys:
            continue
        schema, table = phys
        col_name = _column_sql_name(col_side)
        policy = _policy_for_column(session, db, schema, table, col_name)
        if not policy or not _should_attempt_literal_mirror(policy):
            continue
        new_exprs: list[exp.Expression] = []
        any_change = False
        for el in rhs:
            if not isinstance(el, exp.Literal):
                new_exprs.append(el)
                continue
            raw_val = _extract_python_literal(el)
            new_val, note = _anonymize_literal_with_plan(db, policy, raw_val, salt)
            if note:
                notes.append(note)
            if new_val == raw_val:
                new_exprs.append(el)
                continue
            any_change = True
            new_exprs.append(_literal_to_sqlglot(new_val))
        if any_change:
            inn.set("expressions", new_exprs)
            changed += 1

    return changed


def build_source_to_target_map(
    session: Mapping[str, Any],
    db: Any,
    source_schema: str,
    resolved_target: str,
) -> dict[str, Any]:
    """Extensible mapping from source names to export/mirror targets (schemas, tables, columns)."""
    schemas: dict[str, str] = {str(source_schema).strip().lower(): str(resolved_target).strip()}
    extra = session.get("mirror_schema_map")
    if isinstance(extra, dict):
        for k, v in extra.items():
            if isinstance(k, str) and isinstance(v, str) and k.strip() and v.strip():
                schemas[k.strip().lower()] = v.strip()
    tables = session.get("mirror_table_map") if isinstance(session.get("mirror_table_map"), dict) else {}
    columns = session.get("mirror_column_map") if isinstance(session.get("mirror_column_map"), dict) else {}
    return {"schemas": schemas, "tables": dict(tables), "columns": dict(columns)}


def _pretty_sql_postgres(sql: str) -> str:
    if not sql or sqlglot is None:
        return str(sql or "")
    try:
        parts = sqlglot.transpile(sql, read="postgres", write="postgres", pretty=True)
        return parts[0].strip() if parts else str(sql).strip()
    except Exception as exc:  # noqa: BLE001
        logger.info("query_mirror: pretty transpile skipped: %s", exc)
        return str(sql).strip()


def _normalize_cell_key(val: Any) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "__null__"
    return str(val)


def _lookup_anon_for_column_literal(df_s: pd.DataFrame, df_a: pd.DataFrame, col: str, lit: Any) -> Any | None:
    """First preview row where ``df_s[col]`` matches ``lit``; return ``df_a`` value at that row."""
    if col not in df_s.columns or col not in df_a.columns:
        return None
    if len(df_s) == 0 or len(df_a) == 0:
        return None
    n = min(len(df_s), len(df_a))
    series = df_s[col].iloc[:n]
    if lit is None or (isinstance(lit, float) and pd.isna(lit)):
        match = series.isna()
    else:
        match = series.eq(lit) | (series.astype(str) == str(lit))
    if not match.any():
        return None
    try:
        pos = int(match.to_numpy().nonzero()[0][0])
    except (IndexError, TypeError, ValueError):
        return None
    return df_a[col].iloc[pos]


def _apply_preview_aligned_literals(
    expr: exp.Expression,
    *,
    df_source: pd.DataFrame,
    df_anon: pd.DataFrame,
) -> int:
    """Replace WHERE literals with values from the same row index in anon preview dataframes."""
    if exp is None:
        return 0
    changed = 0

    for eq in list(expr.find_all(exp.EQ)):
        if isinstance(expr, exp.Select):
            owner = _owner_select_for_node(eq)
            if owner is not None and owner is not expr:
                continue
        left_c = _as_column_ref(eq.left)
        right_c = _as_column_ref(eq.right)
        lit_side: exp.Literal | None = None
        col_side: exp.Column | None = None
        if left_c is not None and isinstance(eq.right, exp.Literal):
            col_side, lit_side = left_c, eq.right
        elif right_c is not None and isinstance(eq.left, exp.Literal):
            col_side, lit_side = right_c, eq.left
        if lit_side is None or col_side is None:
            continue
        col_name = _column_sql_name(col_side)
        if col_name not in df_source.columns or col_name not in df_anon.columns:
            continue
        raw_val = _extract_python_literal(lit_side)
        new_val = _lookup_anon_for_column_literal(df_source, df_anon, col_name, raw_val)
        if new_val is None or new_val == raw_val:
            continue
        replacement = _literal_to_sqlglot(new_val)
        if isinstance(eq.right, exp.Literal):
            eq.set("right", replacement)
        else:
            eq.set("left", replacement)
        changed += 1

    for inn in list(expr.find_all(exp.In)):
        if isinstance(expr, exp.Select):
            owner = _owner_select_for_node(inn)
            if owner is not None and owner is not expr:
                continue
        col_side = _as_column_ref(inn.this)
        if col_side is None:
            continue
        col_name = _column_sql_name(col_side)
        if col_name not in df_source.columns or col_name not in df_anon.columns:
            continue
        rhs = _in_list_expressions(inn)
        if not rhs:
            continue
        new_exprs: list[exp.Expression] = []
        any_change = False
        for el in rhs:
            if not isinstance(el, exp.Literal):
                new_exprs.append(el)
                continue
            raw_val = _extract_python_literal(el)
            new_val = _lookup_anon_for_column_literal(df_source, df_anon, col_name, raw_val)
            if new_val is None or new_val == raw_val:
                new_exprs.append(el)
                continue
            any_change = True
            new_exprs.append(_literal_to_sqlglot(new_val))
        if any_change:
            inn.set("expressions", new_exprs)
            changed += 1

    return changed


def _unwrap_select(tree: exp.Expression) -> exp.Select | None:
    if isinstance(tree, exp.With):
        return _unwrap_select(tree.this)
    if isinstance(tree, exp.Select):
        return tree
    return None


def build_mirrored_sql(
    sql: str,
    *,
    source_schema: str,
    destination_mode: str,
    session: Mapping[str, Any],
    db: Any,
    plan_salt: str,
    df_preview_source: Optional[pd.DataFrame] = None,
    df_preview_anon: Optional[pd.DataFrame] = None,
) -> tuple[str, bool]:
    """Return ``(mirrored_sql, any_transformation_applied)`` for UI preview."""
    raw = str(sql or "").strip()
    target = resolve_anonymized_target_schema(session, destination_mode, source_schema, db)
    source_to_target_map = build_source_to_target_map(session, db, source_schema, target)
    export_label = session.get("export_target")
    logger.debug(
        "mirror build: export_target=%r resolved_target=%r",
        export_label,
        target,
    )
    if not raw:
        return "", False

    notes: list[str] = []
    body_sql = raw
    schema_changed = False
    literal_changes = 0
    preview_changes = 0

    if sqlglot is not None and exp is not None:
        try:
            parsed = sqlglot.parse_one(raw, read="postgres")
            outer = _unwrap_select(parsed)
            if outer is not None:
                for sel in list(parsed.find_all(exp.Select)):
                    literal_changes += _rewrite_literals_in_expression(
                        sel,
                        session=session,
                        db=db,
                        default_schema=source_schema,
                        salt=plan_salt,
                        notes=notes,
                    )
                if (
                    df_preview_source is not None
                    and df_preview_anon is not None
                    and not df_preview_source.empty
                    and len(df_preview_anon) > 0
                ):
                    for sel in list(parsed.find_all(exp.Select)):
                        preview_changes += _apply_preview_aligned_literals(
                            sel,
                            df_source=df_preview_source,
                            df_anon=df_preview_anon,
                        )
                if preview_changes:
                    notes.append(
                        "Preview-aligned WHERE literals use the same row values as the Source vs Anon tables below."
                    )
                literal_changes += preview_changes
                schemas = source_to_target_map.get("schemas") or {}
                for tbl in parsed.find_all(exp.Table):
                    db_ident = tbl.args.get("db")
                    cur = _ident_name(db_ident) if db_ident else ""
                    key = cur.lower() if cur else ""
                    mapped = schemas.get(key)
                    if mapped:
                        tbl.set("db", exp.Identifier(this=mapped, quoted=True))
                        schema_changed = True
                body_sql = parsed.sql(dialect="postgres", pretty=False)
            else:
                body_sql = DdlManager.rewrite_schema_references(raw, source_schema, target)
                schema_changed = body_sql.strip() != raw.strip()
        except Exception as exc:  # noqa: BLE001
            logger.info("query_mirror: sqlglot transform failed, falling back to string rewrite: %s", exc)
            body_sql = DdlManager.rewrite_schema_references(raw, source_schema, target)
            schema_changed = body_sql.strip() != raw.strip()
    else:
        body_sql = DdlManager.rewrite_schema_references(raw, source_schema, target)
        schema_changed = body_sql.strip() != raw.strip()

    body_sql = _append_outer_limit_if_missing(body_sql, MIRROR_SQL_ROW_CAP)
    body_sql = _pretty_sql_postgres(body_sql)

    header_lines: list[str] = []
    if destination_mode != "database":
        header_lines.append(
            f'-- In-memory mirror: logical read from "{target}" '
            "(session-backed preview; row-level anonymization still applied in Python)."
        )
    if notes:
        dedup = list(dict.fromkeys(notes))
        for n in dedup[:12]:
            header_lines.append(f"-- Note: {n}")
    if literal_changes:
        header_lines.append(f"-- Mirror: rewritten {literal_changes} filter literal(s) using active plan rules.")

    if header_lines:
        body_sql = "\n".join(header_lines) + "\n" + body_sql

    transformed = schema_changed or bool(literal_changes) or bool(notes)
    return body_sql, transformed
