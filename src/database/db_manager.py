# -*- coding: utf-8 -*-
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
        self.fake = Faker(['de_DE', 'en_US'])
        self._init_metadata_table()

    def get_all_schemas(self):
        inspector = inspect(self.engine)
        return inspector.get_schema_names()

    def get_tables_in_schema(self, schema='public'):
        inspector = inspect(self.engine)
        return inspector.get_table_names(schema=schema)

    def read_table(self, table_name, schema_name='public', where_filter=None, limit=None):
        """Čita tabelu sa opcionim WHERE filterom i LIMIT-om."""
        # Koristimo navodnike za case-sensitivity u Postgresu
        query = f'SELECT * FROM "{schema_name}"."{table_name}"'

        # Dodajemo WHERE logiku
        if where_filter and where_filter.strip():
            # Čistimo filter u slučaju da je korisnik slučajno upisao "WHERE"
            clean_filter = where_filter.strip().replace("WHERE ", "").replace("where ", "")
            query += f" WHERE {clean_filter}"

        # Dodajemo LIMIT logiku
        if limit:
            query += f" LIMIT {limit}"

        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            print(f"Error reading table {schema_name}.{table_name}: {e}")
            # Vraćamo prazan DataFrame sa istim kolonama ako je moguće, ili potpuno prazan
            return pd.DataFrame()

    def save_anonymized_table(self, df, table_name, target_schema='anon'):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))
                conn.execute(text(f"SET client_encoding TO 'UTF8'"))
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

    def get_columns(self, table_name, schema_name='public'):
        """Vra?a listu naziva kolona za datu tabelu koriste?i SQLAlchemy inspect."""
        from sqlalchemy import inspect
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name, schema=schema_name)
            return [col['name'] for col in columns]
        except Exception as e:
            print(f"Error fetching columns for {schema_name}.{table_name}: {e}")
            return []


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
        
        
    def _get_mapping_values(self, category):
        """Izvlači sve lažne vrednosti za određenu kategoriju iz baze."""
        query = text("""
            SELECT fake_value 
            FROM metadata.mapping_values v
            JOIN metadata.mapping_catalog c ON v.catalog_id = c.id
            WHERE c.category_name = :cat
            ORDER BY fake_value ASC
        """)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"cat": category})
                return [row[0] for row in result]
        except Exception as e:
            print(f"Error fetching mapping for {category}: {e}")
            return []

    def _deterministic_map(self, original_val, mapping_list, salt):
        """Deterministički bira zamensku vrednost na osnovu hasha originala."""
        if not mapping_list:
            return "NO_MAPPING_DATA"
            
        import hashlib
        # Kreiramo hash od (vrednost + salt)
        combined = str(original_val) + str(salt)
        hash_obj = hashlib.sha256(combined.encode())
        # Pretvaramo u broj i koristimo modulo operaciju da dobijemo index u listi
        index = int(hash_obj.hexdigest(), 16) % len(mapping_list)
        return mapping_list[index]
        
        



    def apply_anonymization(self, df, plan, salt="default_secret"):
        import hashlib
        import numpy as np
        anonymized_df = df.copy()
        notifications = [] 
        cached_mappings = {} # Keširamo mapping liste za brzinu

        for item in plan:
            col = item['column']
            strategy = item.get('strategy', 'keep').lower()
            if col not in anonymized_df.columns: continue

            # --- 1. NOVA STRATEGIJA: MAPPING (Iz tvojih metadata tabela) ---
            if strategy == 'mapping':
                category = "first_name" if any(k in col.lower() for k in ['name', 'first', 'ime']) else "city"
                
                if category not in cached_mappings:
                    cached_mappings[category] = self._get_mapping_values(category) # Metoda koju smo ranije definisali
                
                m_list = cached_mappings[category]
                if m_list:
                    anonymized_df[col] = anonymized_df[col].apply(
                        lambda x: self._deterministic_map(x, m_list, salt) if pd.notnull(x) else x
                    )
                    notifications.append(f"✅ Column '{col}' mapped using '{category}' from DB.")
                else:
                    notifications.append(f"⚠️ No DB mapping for '{col}'. Falling back to Hash.")
                    strategy = 'hash' # Ako je tabela prazna, prebaci na hash

            # --- 2. TVOJA LOGIKA ZA HASH ---
            if strategy == 'hash':
                anonymized_df[col] = anonymized_df[col].apply(
                    lambda x: hashlib.sha256(f"{x}{salt}".encode()).hexdigest()[:12] if pd.notnull(x) else x
                )

            # --- 3. TVOJA LOGIKA ZA MASK ---
            elif strategy == 'mask':
                anonymized_df[col] = anonymized_df[col].apply(
                    lambda x: self.mask_value(x) if pd.notnull(x) else x
                )

            # --- 4. TVOJA LOGIKA ZA NOISE ---
            elif strategy == 'noise':
                if pd.api.types.is_numeric_dtype(anonymized_df[col]):
                    def get_stable_noise(val):
                        if pd.isnull(val): return val
                        combined = f"{val}{salt}".encode()
                        h = int(hashlib.md5(combined).hexdigest()[:8], 16)
                        noise_percent = 0.9 + (h % 200) / 1000.0
                        return round(val * noise_percent, 2)
                    anonymized_df[col] = anonymized_df[col].apply(get_stable_noise)

            # --- 5. TVOJA LOGIKA ZA DATE_SHIFT ---
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

        return anonymized_df, notifications



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

    def save_ai_plan(self, schema_name, table_name, plan_data):
        """Saves the entire plan as a single JSON in the plan_json column."""
        query = text("""
            INSERT INTO metadata.ai_plans (schema_name, table_name, plan_json, last_updated)
            VALUES (:s, :t, :p, CURRENT_TIMESTAMP)
            ON CONFLICT (schema_name, table_name)
            DO UPDATE SET
                plan_json = EXCLUDED.plan_json,
                last_updated = CURRENT_TIMESTAMP
        """)
        try:
            with self.engine.connect() as conn:
                conn.execute(query, {
                    "s": schema_name,
                    "t": table_name,
                    "p": json.dumps(plan_data) # Pakujemo listu re?nika u JSON
                })
                conn.commit()
                return True
        except Exception as e:
            print(f"Error saving to plan_json: {e}")
            return False

    def get_saved_plan(self, schema_name, table_name):
        """Loads the plan from the plan_json column."""
        query = text("""
            SELECT plan_json FROM metadata.ai_plans
            WHERE schema_name = :s AND table_name = :t
        """)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"s": schema_name, "t": table_name}).fetchone()
                if result and result[0]:
                    # Ako je u bazi JSONB, SQLAlchemy ?e ga vratiti kao listu/dict
                    # Ako je obi?an TEXT, moramo uraditi json.loads
                    data = result[0]
                    plan_list = data if isinstance(data, list) else json.loads(data)
                    return {"plan": plan_list}
                return None
        except Exception as e:
            print(f"Error loading from plan_json: {e}")
            return None

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
