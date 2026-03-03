import pandas as pd
from sqlalchemy import create_engine, text, inspect
import os
import re
import json
class DBManager:
    """
    Handles database connections and operations for reading source data
    and writing anonymized results back to PostgreSQL.
    """
    def __init__(self, db_url=None):
        self.db_url = db_url or os.getenv(
            "DATABASE_URL",
            "postgresql://user:password@db:5432/anonify_db"
        )
        self.engine = create_engine(self.db_url)

        # Pozivamo kreiranje metadata tabele pri svakom paljenju aplikacije
        self._init_metadata_table()

    def get_all_schemas(self):
        inspector = inspect(self.engine)
        return inspector.get_schema_names()

    def get_tables_in_schema(self, schema='public'):
        inspector = inspect(self.engine)
        return inspector.get_table_names(schema=schema)

    def read_table(self, table_name, schema='public'):
        query = f"SELECT * FROM {schema}.{table_name}"
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            print(f"Error reading table {schema}.{table_name}: {e}")
            return None

    def save_anonymized_table(self, df, table_name, target_schema='anon'):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))
                conn.commit()

            df.to_sql(table_name, self.engine, schema=target_schema, if_exists='replace', index=False)
            return True
        except Exception as e:
            print(f"Error saving to {target_schema}.{table_name}: {e}")
            return False


    def mask_value(self, val):
        """Helper to partially mask values before sending to AI."""
        s = str(val)
        if len(s) <= 3:
            return "***"
        if "@" in s:
            parts = s.split("@")
            return f"{parts[0][:2]}**@{parts[1][:2]}**.com"
        return f"{s[:3]}***"

    def get_ai_ready_metadata(self, table_name, schema='public', sample_size=5):
        """Advanced Metadata Extraction with Vertical Shuffling and Masking."""
        inspector = inspect(self.engine)
        columns_info = inspector.get_columns(table_name, schema=schema)

        metadata_package = []
        for col in columns_info:
            col_name = col['name']
            query = text(f'SELECT "{col_name}" FROM {schema}.{table_name} WHERE "{col_name}" IS NOT NULL ORDER BY RANDOM() LIMIT {sample_size}')

            try:
                raw_sample = pd.read_sql(query, self.engine)[col_name].tolist()
                masked_sample = [self.mask_value(v) for v in raw_sample]
            except:
                masked_sample = []

            metadata_package.append({
                "column": col_name,
                "type": str(col['type']),
                "sample": list(set(masked_sample))
            })
        return metadata_package

    def get_shuffled_metadata(self, table_name, schema='public', sample_size=10):
        """Extracts metadata and a DE-CORRELATED sample for AI analysis."""
        inspector = inspect(self.engine)
        columns_info = inspector.get_columns(table_name, schema=schema)
        metadata_package = []

        for col in columns_info:
            col_name = col['name']
            query = text(f'SELECT "{col_name}" FROM {schema}.{table_name} WHERE "{col_name}" IS NOT NULL ORDER BY RANDOM() LIMIT {sample_size}')
            try:
                col_sample = pd.read_sql(query, self.engine)[col_name].unique().tolist()
            except:
                col_sample = []

            metadata_package.append({
                "column": col_name,
                "type": str(col['type']),
                "sample": col_sample
            })
        return metadata_package


    def apply_anonymization(self, df, plan, salt="default_secret"):
        """Applies AI plan with optional Salt for deterministic hashing."""
        import hashlib
        import numpy as np

        anonymized_df = df.copy()

        for item in plan:
            col = item['column']
            strategy = item['strategy'].lower()

            if col not in anonymized_df.columns:
                continue

            if strategy == 'hash':
                # Deterministic hashing with Salt
                anonymized_df[col] = anonymized_df[col].apply(
                    lambda x: hashlib.sha256(f"{x}{salt}".encode()).hexdigest()[:12] if pd.notnull(x) else x
                )
            elif strategy == 'mask':
                anonymized_df[col] = anonymized_df[col].apply(
                    lambda x: self.mask_value(x) if pd.notnull(x) else x
                )
            elif strategy == 'noise':
                if pd.api.types.is_numeric_dtype(anonymized_df[col]):
                    # Apply +/- 10% random noise
                    noise = np.random.uniform(0.9, 1.1, size=len(anonymized_df))
                    anonymized_df[col] = (anonymized_df[col] * noise).round(2)

        return anonymized_df

    def _init_metadata_table(self):
        """Creates the metadata schema and ai_plans table."""
        from sqlalchemy import text # Ensure 'sqlalchemy' is spelled correctly

        query = """
        CREATE SCHEMA IF NOT EXISTS metadata;
        CREATE TABLE IF NOT EXISTS metadata.ai_plans (
            schema_name TEXT,
            table_name TEXT,
            plan_json JSONB,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (schema_name, table_name)
        );
        """
        with self.engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()


    def save_ai_plan(self, schema, table, plan):
        """Saves the plan. Ensure 'plan' is converted to list of dicts before calling."""
        query = text("""
            INSERT INTO metadata.ai_plans (schema_name, table_name, plan_json, last_updated)
            VALUES (:schema, :table, :plan, CURRENT_TIMESTAMP)
            ON CONFLICT (schema_name, table_name)
            DO UPDATE SET plan_json = EXCLUDED.plan_json, last_updated = CURRENT_TIMESTAMP;
        """)
        with self.engine.connect() as conn:
            conn.execute(query, {"schema": schema, "table": table, "plan": json.dumps(plan)})
            conn.commit()

    def get_saved_plan(self, schema, table):
        """Retrieves a saved plan."""
        query = text("""
            SELECT plan_json FROM metadata.ai_plans
            WHERE schema_name = :schema AND table_name = :table;
        """)
        with self.engine.connect() as conn:
            result = conn.execute(query, {"schema": schema, "table": table}).fetchone()
            if result:
                return result[0]
            return None