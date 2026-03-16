import pandas as pd
from sqlalchemy import create_engine, text, inspect
import os
import json
import hashlib
from faker import Faker

class DBManager:
    def __init__(self, db_url=None):
        self.db_url = db_url or os.getenv(
            "DATABASE_URL",
            "postgresql://user:password@db:5432/anonify_db"
        )
        self.engine = create_engine(self.db_url)
        self.fake = Faker(['de_DE', 'en_US'])
        self._init_metadata_table()

    def get_all_schemas(self):
        inspector = inspect(self.engine)
        return inspector.get_schema_names()

    def get_tables_in_schema(self, schema='public'):
        inspector = inspect(self.engine)
        return inspector.get_table_names(schema=schema)

    def read_table(self, table_name, schema='public'):
        query = f'SELECT * FROM "{schema}"."{table_name}"'
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            print(f"Error reading table {schema}.{table_name}: {e}")
            return pd.DataFrame()

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
        s = str(val)
        if len(s) <= 3: return "***"
        if "@" in s:
            parts = s.split("@")
            return f"{parts[0][:2]}**@{parts[1][:2]}**.com"
        return f"{s[:3]}***"

    def get_ai_ready_metadata(self, table_name, schema='public', sample_size=5):
        inspector = inspect(self.engine)
        columns_info = inspector.get_columns(table_name, schema=schema)
        metadata_package = []
        for col in columns_info:
            col_name = col['name']
            query = text(f'SELECT "{col_name}" FROM "{schema}"."{table_name}" LIMIT 50')
            try:
                raw_sample = pd.read_sql(query, self.engine)[col_name].dropna().unique().tolist()[:sample_size]
                masked_sample = [self.mask_value(v) for v in raw_sample]
            except:
                masked_sample = []
            metadata_package.append({"column": col_name, "type": str(col['type']), "sample": masked_sample})
        return metadata_package

    def apply_anonymization(self, df, plan, salt="default_secret"):

        import hashlib
        import numpy as np
        from faker.providers.person.de_DE import Provider as PersonProvider
        all_names = list(PersonProvider.first_names)
        anonymized_df = df.copy()

        for item in plan:
            col = item['column']
            strategy = item.get('strategy', 'keep').lower()
            if col not in anonymized_df.columns: continue

            if strategy == 'hash':
                anonymized_df[col] = anonymized_df[col].apply(
                    lambda x: hashlib.sha256(f"{x}{salt}".encode()).hexdigest()[:12] if pd.notnull(x) else x
                )
            elif strategy == 'mask':
                anonymized_df[col] = anonymized_df[col].apply(
                    lambda x: self.mask_value(x) if pd.notnull(x) else x
                )
            elif strategy == 'synthetic':
                def get_fiksni_hans(val):
                    if pd.isnull(val): return val
                    combined = f"{val}{salt}".encode()
                    hash_idx = int(hashlib.sha256(combined).hexdigest(), 16)
                    if any(k in col.lower() for k in ['name', 'first', 'last', 'ime']):
                        return all_names[hash_idx % len(all_names)]
                    return f"anon_{hash_idx % 1000}"
                anonymized_df[col] = anonymized_df[col].apply(get_fiksni_hans)

            # --- NOVO: DETERMINISTI?KI NOISE ---
            elif strategy == 'noise':
                if pd.api.types.is_numeric_dtype(anonymized_df[col]):
                    def get_stable_noise(val):
                        if pd.isnull(val): return val
                        # Pravimo procenat šuma na osnovu hasha (izme?u -10% i +10%)
                        combined = f"{val}{salt}".encode()
                        h = int(hashlib.md5(combined).hexdigest()[:8], 16)
                        noise_percent = 0.9 + (h % 200) / 1000.0 # 0.900 do 1.100
                        return round(val * noise_percent, 2)
                    anonymized_df[col] = anonymized_df[col].apply(get_stable_noise)

            # --- NOVO: DATE SHIFTING ---
            elif strategy == 'date_shift' or (strategy == 'keep' and "date" in col.lower()):
                try:
                    anonymized_df[col] = pd.to_datetime(anonymized_df[col])
                    def shift_date(val):
                        if pd.isnull(val): return val
                        combined = f"{val}{salt}".encode()
                        h = int(hashlib.md5(combined).hexdigest()[:8], 16)
                        days_to_shift = (h % 7) - 3 # Pomera od -3 do +3 dana
                        return val + pd.Timedelta(days=days_to_shift)
                    anonymized_df[col] = anonymized_df[col].apply(shift_date)
                except:
                    pass # Ako nije pravi datum, ostavi kako jeste

        return anonymized_df




    def _init_metadata_table(self):
        query = """
        CREATE SCHEMA IF NOT EXISTS metadata;
        CREATE TABLE IF NOT EXISTS metadata.ai_plans (
            schema_name TEXT, table_name TEXT, plan_json JSONB, last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (schema_name, table_name)
        );
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
        except: pass

    def save_ai_plan(self, schema, table, plan):
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
        query = text("SELECT plan_json FROM metadata.ai_plans WHERE schema_name = :s AND table_name = :t")
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"s": schema, "t": table}).fetchone()
                return result[0] if result else None
        except: return None