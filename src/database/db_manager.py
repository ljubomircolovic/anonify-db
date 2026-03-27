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

        self.engine = create_engine(
        self.db_url,
        connect_args={'client_encoding': 'utf8'}
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

    def get_global_mapping(self, col_name, orig_val, salt):
        """Proverava da li vec imamo anonimizovanu vrednost za ovaj ID i Salt."""
        query = text("""
            SELECT anonymized_value FROM metadata.global_id_mapping
            WHERE column_name = :c AND original_value = :o AND salt_used = :s
        """)
        try:
            with self.engine.connect() as conn:
                res = conn.execute(query, {"c": col_name, "o": str(orig_val), "s": salt}).fetchone()
                return res[0] if res else None
        except:
            return None

    def save_global_mapping(self, col_name, orig_val, anon_val, salt):
        """Skladisti novu vezu u globalnu mapu."""
        query = text("""
            INSERT INTO metadata.global_id_mapping (column_name, original_value, anonymized_value, salt_used)
            VALUES (:c, :o, :a, :s)
            ON CONFLICT DO NOTHING
        """)
        try:
            with self.engine.connect() as conn:
                conn.execute(query, {"c": col_name, "o": str(orig_val), "a": str(anon_val), "s": salt})
                conn.commit()
        except:
            pass


    def apply_anonymization(self, df, plan, salt="default_secret"):
        import hashlib
        import numpy as np
        from faker.providers.person.de_DE import Provider as PersonProvider
        all_names = list(PersonProvider.first_names)
        anonymized_df = df.copy()

        notifications = [] # Kolekcija poruka za UI

        for item in plan:
            col = item['column']
            strategy = item.get('strategy', 'keep').lower()
            if col not in anonymized_df.columns: continue

            # --- LOGIKA ZA REFERENCIJALNI INTEGRITET (ID kolone) ---
            # Ako kolona u imenu ima 'id', 'pk', 'fk', tretiramo je kao kljuc
            is_id_column = any(k in col.lower() for k in ['id', 'pk', 'fk', 'key', 'sifra'])

            if strategy == 'synthetic' or (is_id_column and strategy != 'keep'):
                def get_smart_synthetic(val):
                    if pd.isnull(val): return val

                    # 1. Proveri da li vec postoji u globalnoj mapi
                    existing = self.get_global_mapping(col, val, salt)
                    if existing:
                        return existing

                    # 2. Ako ne postoji, generisi novu vrednost
                    combined = f"{val}{salt}".encode()
                    hash_obj = hashlib.sha256(combined)
                    hash_idx = int(hash_obj.hexdigest(), 16)

                    new_val = ""
                    if any(k in col.lower() for k in ['name', 'first', 'last', 'ime']):
                        new_val = all_names[hash_idx % len(all_names)]
                    else:
                        new_val = f"anon_{hash_idx % 1000000}"

                    # 3. Sacuvaj u mapu za buducu upotrebu u drugim tabelama
                    self.save_global_mapping(col, val, new_val, salt)
                    return new_val

                anonymized_df[col] = anonymized_df[col].apply(get_smart_synthetic)

                if is_id_column:
                    notifications.append(f"?? Column '{col}' handled with Referential Integrity. Values mapped across tables.")

            # --- OSTALE STRATEGIJE (Hash, Mask, Noise, Date Shift) ---
            elif strategy == 'hash':
                anonymized_df[col] = anonymized_df[col].apply(
                    lambda x: hashlib.sha256(f"{x}{salt}".encode()).hexdigest()[:12] if pd.notnull(x) else x
                )
            elif strategy == 'mask':
                anonymized_df[col] = anonymized_df[col].apply(
                    lambda x: self.mask_value(x) if pd.notnull(x) else x
                )
            elif strategy == 'noise':
                if pd.api.types.is_numeric_dtype(anonymized_df[col]):
                    def get_stable_noise(val):
                        if pd.isnull(val): return val
                        combined = f"{val}{salt}".encode()
                        h = int(hashlib.md5(combined).hexdigest()[:8], 16)
                        noise_percent = 0.9 + (h % 200) / 1000.0
                        return round(val * noise_percent, 2)
                    anonymized_df[col] = anonymized_df[col].apply(get_stable_noise)
            elif strategy == 'date_shift':
                try:
                    anonymized_df[col] = pd.to_datetime(anonymized_df[col])
                    def shift_date(val):
                        if pd.isnull(val): return val
                        combined = f"{val}{salt}".encode()
                        h = int(hashlib.md5(combined).hexdigest()[:8], 16)
                        days_to_shift = (h % 7) - 3
                        return val + pd.Timedelta(days=days_to_shift)
                    anonymized_df[col] = anonymized_df[col].apply(shift_date)
                except: pass

        return anonymized_df, notifications # Vracamo i DF i notifikacije




# ... (prethodne metode: apply_anonymization, itd.)

    def _init_metadata_table(self):
        query = """
        CREATE SCHEMA IF NOT EXISTS metadata;
        CREATE TABLE IF NOT EXISTS metadata.ai_plans (
            schema_name TEXT, table_name TEXT, plan_json JSONB, last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (schema_name, table_name)
        );
        CREATE TABLE IF NOT EXISTS metadata.global_id_mapping (
            column_name TEXT,
            original_value TEXT,
            anonymized_value TEXT,
            salt_used TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (column_name, original_value, salt_used)
        );
        CREATE TABLE IF NOT EXISTS metadata.audit_log (
            id SERIAL PRIMARY KEY,
            execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_name TEXT,
            schema_name TEXT,
            table_name TEXT,
            privacy_score INTEGER,
            salt_used TEXT,
            status TEXT
        );
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
        except Exception as e:
            print(f"Metadata init error: {e}")

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
        except:
            return None

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

    def log_action(self, user, schema, table, score, salt, status="SUCCESS"):
        """Upisuje detalje o izvrsenoj anonimizaciji u bazu."""
        query = text("""
            INSERT INTO metadata.audit_log (user_name, schema_name, table_name, privacy_score, salt_used, status)
            VALUES (:u, :s, :t, :score, :salt, :status)
        """)
        try:
            with self.engine.connect() as conn:
                conn.execute(query, {
                    "u": user, "s": schema, "t": table,
                    "score": score, "salt": salt, "status": status
                })
                conn.commit()
        except Exception as e:
            print(f"Logging error: {e}")