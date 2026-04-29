# -*- coding: utf-8 -*-
import logging
from src.agents.privacy_agent import PrivacyAgent

# Logger initialization
logger = logging.getLogger(__name__)

def _enforce_id_strategy(column_name, strategy, is_pk=False, is_fk=False, sql_type=""):
    """
    Enforces RI-safe strategy for identifier columns.
    - ID-like columns must never use 'mask' because it breaks joins/lineage.
    - PK/FK numeric identifiers (BIGINT/INTEGER family) must remain 'keep'
      to avoid type conflicts and preserve strict referential integrity.
    """
    safe_strategy = (strategy or "keep").lower().strip()
    col_l = str(column_name or "").lower()
    is_id_like = any(token in col_l for token in ["id", "pk", "fk"])
    type_l = str(sql_type or "").lower()
    is_numeric_id_type = any(token in type_l for token in ["bigint", "integer", "smallint", "int"])

    if (is_pk or is_fk) and is_numeric_id_type:
        return "keep"
    if is_id_like and safe_strategy == "mask":
        return "hash"
    return safe_strategy

class AnonymizationPlanner:
    def __init__(self, db_manager):
        self.db = db_manager
        self.agent = PrivacyAgent()

    def generate_suggestion_plan(self, schema, table_name, allow_sampling=True, sample_limit=5):
        """
        Main method: fetches a DB sample and asks Azure AI for recommendations.
        This method is thread-safe (no st.* calls inside).
        """
        try:
            logger.info(f"🔍 Analyzing table: {schema}.{table_name}...")

            metadata_package = []

            # 1. Conditionally fetch a sample
            if allow_sampling:
                # Fetch data directly through db_manager
                sample_data = self.db.get_table_sample(schema, table_name, limit=sample_limit)

                if sample_data:
                    # Build metadata package with real value samples
                    all_columns = list(sample_data[0].keys())
                    for col in all_columns:
                        col_samples = [row.get(col) for row in sample_data]
                        metadata_package.append({
                            "column": col,
                            "sample": col_samples
                        })
                else:
                    logger.warning(f"Table {table_name} is empty. AI will use metadata only.")
                    allow_sampling = False

            # 2. Fallback when sampling is disabled or table is empty
            if not allow_sampling:
                all_columns = self.db.get_columns(table_name, schema)
                metadata_package = [{"column": col, "sample": []} for col in all_columns]

            # 3. Load metadata needed for strict strategy enforcement
            col_details = self.db.get_column_details(table_name, schema)
            pk_columns = set(self.db.get_primary_keys(schema, table_name))
            all_relations = self.db.get_all_foreign_keys(schema)
            fk_columns = {rel[1] for rel in all_relations if rel[0] == table_name}

            # 4. Build audit info
            audit_info = {
                "target_table": f"{schema}.{table_name}",
                "sampling_enabled": allow_sampling,
                "rows_sent": sample_limit if allow_sampling else 0,
                "payload": metadata_package,
                "policy": "Azure OpenAI Enterprise (No Training)"
            }

            # 5. Call Azure AI Agent via LangChain
            # Spinner is controlled in the UI layer (tabs_content.py)
            analysis = self.agent.analyze_metadata(metadata_package)

            if analysis and analysis.plan:
                # Convert Pydantic objects to dictionaries for stable thread transfer
                final_plan = []
                for p in analysis.plan:
                    col_type = col_details.get(p.column, {}).get("type", "")
                    corrected_strategy = _enforce_id_strategy(
                        p.column,
                        p.strategy,
                        is_pk=(p.column in pk_columns),
                        is_fk=(p.column in fk_columns),
                        sql_type=col_type,
                    )
                    final_plan.append({
                        "column": p.column,
                        "is_pii": p.is_pii,
                        "strategy": corrected_strategy,
                        "reason": p.reason
                    })

                logger.info(f"✅ Successfully generated plan for {table_name}")
                return final_plan, audit_info

            return None, None

        except Exception as e:
            logger.error(f"❌ Planner error for {table_name}: {e}")
            return None, None

def analyze_tables_parallel(db_manager, tables, schema="public", allow_sampling=True, sample_limit=5):
    """
    Unified batch analysis across all selected tables in one Azure AI request.
    Returns per-table results to preserve table-level planning UX.
    """
    planner = AnonymizationPlanner(db_manager)
    results = {}
    selected_tables = list(tables or [])
    if not selected_tables:
        return results

    strict_sample_limit = min(int(sample_limit or 5), 5)
    scan_signature = (
        str(schema),
        tuple(sorted(str(t) for t in selected_tables)),
        bool(allow_sampling),
        int(strict_sample_limit),
    )

    logger.info(f"[AI_SCAN] Collecting metadata and 5-row samples for {len(selected_tables)} tables....")
    unified_packages = []
    table_runtime_context = {}
    all_relations = db_manager.get_all_foreign_keys(schema)
    cached_signature = getattr(db_manager, "_last_unified_scan_signature", None)
    cached_rows = getattr(db_manager, "_last_unified_payload_rows", None)
    if cached_signature == scan_signature and isinstance(cached_rows, list):
        unified_payload_rows = cached_rows
        logger.info("[AI_SCAN] Reusing prepared table payload from current session cache.")
    else:
        unified_payload_rows = db_manager.get_unified_ai_scan_payload(
            schema=schema,
            tables=selected_tables,
            sample_limit=strict_sample_limit if allow_sampling else 0,
        )
        db_manager._last_unified_scan_signature = scan_signature
        db_manager._last_unified_payload_rows = unified_payload_rows

    for table_obj in unified_payload_rows:
        table_name = table_obj.get("table")
        if not table_name:
            continue
        try:
            col_details = db_manager.get_column_details(table_name, schema)
            columns = [{"name": c.get("column", ""), "type": c.get("type", "")} for c in table_obj.get("columns", [])]
            rows = (table_obj.get("sample_rows", []) or [])[:5]

            unified_packages.append({
                "table_name": table_name,
                "columns": columns,
                "sample_rows": rows,
            })
            table_runtime_context[table_name] = {
                "col_details": col_details,
                "pk_columns": set(db_manager.get_primary_keys(schema, table_name)),
                "fk_columns": {rel[1] for rel in all_relations if rel[0] == table_name},
                "audit": {
                    "target_table": f"{schema}.{table_name}",
                    "sampling_enabled": bool(allow_sampling),
                    "rows_sent": len(rows),
                    "payload": {"columns": columns, "sample_rows": rows},
                    "policy": "Azure OpenAI Enterprise (No Training)",
                    "request_mode": "unified_batch",
                }
            }
        except Exception as e:
            logger.error(f"❌ [AI_SCAN] Preparation failed for {table_name}: {e}")
            results[table_name] = {"error": f"Preparation failed: {e}"}

    full_prompt = planner.agent.build_unified_prompt(schema, unified_packages)
    estimated_tokens = int(len(full_prompt) / 4)
    logger.info(f"[AI_SCAN] Sending unified payload to Azure AI (Estimated tokens: ~{int(estimated_tokens)})...")

    try:
        unified_response = planner.agent.analyze_unified_tables(schema, unified_packages)
    except Exception as e:
        logger.error(f"❌ [AI_SCAN] Unified request failed: {e}")
        return {table_name: {"error": f"Unified request failed: {e}"} for table_name in selected_tables}

    logger.info("[AI_SCAN] Parsing unified response and mapping per-table results...")
    returned_tables = unified_response.get("table_results", {})
    parsing_warnings = unified_response.get("warnings", [])

    for warning in parsing_warnings:
        logger.warning(f"⚠️ [AI_SCAN] {warning}")

    for table_name in selected_tables:
        if table_name in results and results[table_name].get("error"):
            continue

        ctx = table_runtime_context.get(table_name, {})
        col_details = ctx.get("col_details", {})
        pk_columns = ctx.get("pk_columns", set())
        fk_columns = ctx.get("fk_columns", set())
        audit_info = ctx.get("audit", {})

        table_plan = returned_tables.get(table_name)
        if not isinstance(table_plan, list):
            logger.warning(f"⚠️ [AI_SCAN] Malformed or missing AI output for table '{table_name}'.")
            results[table_name] = {
                "error": "Malformed AI response for this table in unified batch.",
                "audit": audit_info,
            }
            continue

        final_plan = []
        pii_count = 0
        for p in table_plan:
            try:
                col_name = p.get("column", "")
                if not col_name:
                    continue
                col_type = col_details.get(col_name, {}).get("type", "")
                corrected_strategy = _enforce_id_strategy(
                    col_name,
                    p.get("strategy", "keep"),
                    is_pk=(col_name in pk_columns),
                    is_fk=(col_name in fk_columns),
                    sql_type=col_type,
                )
                is_pii = bool(p.get("is_pii", False))
                if is_pii:
                    pii_count += 1
                final_plan.append({
                    "column": col_name,
                    "is_pii": is_pii,
                    "strategy": corrected_strategy,
                    "reason": p.get("reason", ""),
                })
            except Exception as parse_err:
                logger.warning(f"⚠️ [AI_SCAN] Table '{table_name}' plan row skipped: {parse_err}")

        if final_plan:
            results[table_name] = {"plan": final_plan, "audit": audit_info}
            if pii_count > 0:
                logger.info(f"[AI_SCAN] Table '{table_name}': {pii_count} PII columns flagged.")
            else:
                logger.info(f"[AI_SCAN] Table '{table_name}': No sensitive data found.")
        else:
            logger.warning(f"⚠️ [AI_SCAN] Table '{table_name}' returned no usable plan rows.")
            results[table_name] = {"error": "No usable plan rows from unified response.", "audit": audit_info}

    try:
        db_manager.log_unified_ai_scan(
            user="system",
            schema=schema,
            tables=selected_tables,
            status="UNIFIED_AI_SCAN_SUCCESS",
            score=len(selected_tables),
            estimated_tokens=estimated_tokens,
        )
    except Exception as log_err:
        logger.warning(f"⚠️ [AI_SCAN] Failed to record unified audit log event: {log_err}")

    logger.info("[AI_SCAN] ✅ Unified analysis complete. Results mapped to planning session.")
    return results