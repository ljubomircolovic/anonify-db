# -*- coding: utf-8 -*-
import pandas as pd
import logging
from src.agents.privacy_agent import PrivacyAgent
import concurrent.futures
import time

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
    Runs AI analysis for multiple tables in parallel with throttling.
    Safe for Streamlit usage.
    """
    planner = AnonymizationPlanner(db_manager)
    results = {}
    future_to_table = {}

    # Use ThreadPoolExecutor for I/O-bound tasks (API calls)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:

        for table in tables:
            # Submit task to thread pool
            future = executor.submit(
                planner.generate_suggestion_plan,
                schema, table, allow_sampling, sample_limit
            )
            future_to_table[future] = table

            # Throttling: prevent Azure 429 errors
            time.sleep(0.5)
            logger.info(f"📡 Submitted task for table: {table}, waiting 0.5s before next...")

        # Collect results
        for future in concurrent.futures.as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                plan, audit = future.result()
                if plan:
                    results[table_name] = {"plan": plan, "audit": audit}
                    logger.info(f"✅ Parallel analysis completed for: {table_name}")
                else:
                    logger.warning(f"⚠️ Plan for {table_name} returned as None.")
                    results[table_name] = {
                        "error": "AI did not return a usable plan.",
                        "audit": audit or {},
                    }
            except Exception as e:
                logger.error(f"❌ Parallelization error for {table_name}: {e}")
                results[table_name] = {"error": str(e)}

    return results