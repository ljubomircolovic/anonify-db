# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import os
import json
import hashlib
from faker import Faker

import os
from openai import AzureOpenAI
from dotenv import load_dotenv

# Učitava varijable iz .env fajla u sistemsko okruženje
load_dotenv()

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
        azure_key = os.getenv("AZURE_OPENAI_KEY")
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")

        if azure_key and azure_endpoint:
            self.ai_client = AzureOpenAI(
                api_key=azure_key,
                api_version=os.getenv("AZURE_OPENAI_VERSION", "2024-02-15-preview"),
                azure_endpoint=azure_endpoint
            )
        else:
            self.ai_client = None
            print("⚠️ Azure OpenAI credentials not found in environment.")


        self.fake = Faker(['de_DE', 'en_US'])
        self._init_metadata_table()

    def get_all_schemas(self):
        inspector = inspect(self.engine)
        return inspector.get_schema_names()

    def get_tables_in_schema(self, schema='public'):
        inspector = inspect(self.engine)
        return inspector.get_table_names(schema=schema)

    def read_table(self, table_name, schema_name='public', where_filter=None, limit=None, params=None):
        """Čita tabelu koristeći parametrizovan SQL radi bezbednosti."""
        from sqlalchemy import text

        # Osnovni upit sa zaštićenim imenima šeme i tabele
        query_str = f'SELECT * FROM "{schema_name}"."{table_name}"'

        if where_filter and where_filter.strip():
            # Čistimo filter od reči "WHERE" ako ju je korisnik uneo
            clean_filter = where_filter.lower().replace("where", "").strip()
            query_str += f" WHERE {clean_filter}"

        if limit:
            query_str += f" LIMIT {limit}"

        query = text(query_str)

        try:
            with self.engine.connect() as conn:
                # params bi bio npr. {"min_id": 100} ako u where_filter imaš :min_id
                result = conn.execute(query, params or {})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
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


    def get_mapping_value(self, original_value, category, locale, salt):
        import hashlib
        with self.engine.connect() as conn:
            # 1. Uzmi sve dostupne fejk vrednosti za kategoriju i jezik, SORTIRANO
            query = text("""
                SELECT v.fake_value
                FROM metadata.mapping_values v
                JOIN metadata.mapping_catalog c ON v.catalog_id = c.id
                WHERE c.category_name = :cat AND c.locale = :loc
                ORDER BY v.fake_value ASC
            """)
            res = conn.execute(query, {"cat": category, "loc": locale})
            pool = [row[0] for row in res]

        if not pool:
            return f"Fake_{category}"

        # 2. Deterministički izbor: hash(original + salt) % dužina_liste
        combined = f"{original_value}{salt}".encode('utf-8')
        hash_int = int(hashlib.sha256(combined).hexdigest(), 16)
        index = hash_int % len(pool)

        return pool[index]



    def apply_anonymization(self, df, plan, salt="default_secret", locale="de"):
        import hashlib
        import pandas as pd
        import numpy as np

        anonymized_df = df.copy()
        notifications = []
        # Keširamo mapping liste: ključ će biti 'category_locale' (npr. 'first_name_de')
        cached_mappings = {}

        for item in plan:
            col = item['column']
            strategy = item.get('strategy', 'keep').lower()
            if col not in anonymized_df.columns: continue

            # --- 1. STRATEGIJA: MAPPING (Sa Locale podrškom) ---
            if strategy == 'mapping':

                category = "first_name"
                if any(k in col.lower() for k in ['last', 'prezime']): category = "last_name"
                elif any(k in col.lower() for k in ['city', 'grad', 'mesto']): category = "city"
                elif any(k in col.lower() for k in ['company', 'firma', 'preduzece']): category = "company_name"

                cache_key = f"{category}_{locale}"

                if cache_key not in cached_mappings:
                    cached_mappings[cache_key] = self._get_mapping_values_by_locale(category, locale)

                m_list = cached_mappings[cache_key]

                if m_list:
                    # Ako imamo podatke u bazi (npr. za 'de'), radi mapping
                    anonymized_df[col] = anonymized_df[col].apply(
                        lambda x: self._deterministic_map(x, m_list, salt) if pd.notnull(x) else x
                    )
                    notifications.append(f"✅ Column '{col}' mapped using '{category}' ({locale}).")
                else:
                    # --- OVO JE TAJ FALLBACK ---
                    # Ako nema podataka (npr. za 'us'), nemoj da pukneš, nego udri HASH
                    notifications.append(f"🔄 No '{locale}' data for '{category}'. Applied Hash instead.")
                    anonymized_df[col] = anonymized_df[col].apply(
                        lambda x: hashlib.sha256(f"{x}{salt}".encode()).hexdigest()[:12] if pd.notnull(x) else x
                    )

            # --- 2. LOGIKA ZA HASH (Ostaje ista) ---
            if strategy == 'hash':
                anonymized_df[col] = anonymized_df[col].apply(
                    lambda x: hashlib.sha256(f"{x}{salt}".encode()).hexdigest()[:12] if pd.notnull(x) else x
                )

            # --- 3. LOGIKA ZA MASK (Ostaje ista) ---
            elif strategy == 'mask':
                # Pretpostavljam da imaš metodu self.mask_value, ako ne, koristi x.mask(...)
                anonymized_df[col] = anonymized_df[col].apply(
                    lambda x: f"***{str(x)[-3:]}" if pd.notnull(x) else x
                )

            # --- 4. LOGIKA ZA NOISE (Deterministički šum) ---
            elif strategy == 'noise':
                if pd.api.types.is_numeric_dtype(anonymized_df[col]):
                    def get_stable_noise(val):
                        if pd.isnull(val): return val
                        combined = f"{val}{salt}".encode()
                        h = int(hashlib.md5(combined).hexdigest()[:8], 16)
                        noise_percent = 0.95 + (h % 100) / 1000.0 # Šum +/- 5%
                        return round(val * noise_percent, 2)
                    anonymized_df[col] = anonymized_df[col].apply(get_stable_noise)

            # --- 5. LOGIKA ZA DATE_SHIFT ---
            elif strategy == 'date_shift':
                try:
                    anonymized_df[col] = pd.to_datetime(anonymized_df[col])
                    def shift_date(val):
                        if pd.isnull(val): return val
                        combined = f"{val}{salt}".encode()
                        h = int(hashlib.md5(combined).hexdigest()[:8], 16)
                        days_to_shift = (h % 10) - 5 # Pomeraj +/- 5 dana
                        return val + pd.Timedelta(days=days_to_shift)
                    anonymized_df[col] = anonymized_df[col].apply(shift_date)
                except: pass

        return anonymized_df, notifications

    # --- POMOĆNE METODE KOJE MORAŠ IMATI U DBManager KLASI ---

    def _get_mapping_values_by_locale(self, category, locale):
        from sqlalchemy import text
        query = text("""
            SELECT v.fake_value
            FROM metadata.mapping_values v
            JOIN metadata.mapping_catalog c ON v.catalog_id = c.id
            WHERE c.category_name = :cat AND c.locale = :loc
            ORDER BY v.fake_value ASC
        """)
        with self.engine.connect() as conn:
            res = conn.execute(query, {"cat": category, "loc": locale})
            return [row[0] for row in res]

    def _deterministic_map(self, original_value, mapping_list, salt):
        import hashlib
        if not mapping_list: return original_value
        combined = f"{original_value}{salt}".encode('utf-8')
        hash_int = int(hashlib.sha256(combined).hexdigest(), 16)
        index = hash_int % len(mapping_list)
        return mapping_list[index]


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

    def test_connection(self):
        """Proverava da li je baza dostupna i da li imamo osnovni pristup."""
        # VAŽNO: Uvozimo text unutar metode ako nije uvezena na vrhu fajla
        from sqlalchemy import text
        try:
            with self.engine.connect() as conn:
                # Izvršavamo prost upit da potvrdimo 'handshake'
                conn.execute(text("SELECT 1"))
                return True, "Connection successful! ✅"
        except Exception as e:
            # Vraćamo detaljnu grešku da bismo znali šta nije u redu (npr. loša lozinka)
            return False, f"Connection failed: {str(e)} ❌"

    def get_foreign_key_relations_postgres(self, schema_name='public'):
        """
        Dohvata Foreign Key relacije specifične za PostgreSQL.
        Vraća DataFrame sa: table_name, column_name, foreign_table_name, foreign_column_name.
        """
        from sqlalchemy import text

        # SQL upit optimizovan za Postgres metapodatke
        query = text("""
            SELECT
                tc.table_name AS table_name,
                kcu.column_name AS column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = :schema;
        """)

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"schema": schema_name})
                df_rel = pd.DataFrame(result.fetchall(), columns=result.keys())

                # Logujemo broj pronađenih relacija u konzolu radi lakšeg debugginga
                print(f"📊 [Dependency Engine] Found {len(df_rel)} relations in schema '{schema_name}'")
                return df_rel
        except Exception as e:
            print(f"❌ Error fetching Postgres relations: {e}")
            return pd.DataFrame()

    def get_execution_order(self, selected_tables, schema_name='public'):
        """
        Sortira izabrane tabele po hijerarhiji (PK pre FK).
        Koristi topološko sortiranje na osnovu relacija.
        """
        relations = self.get_foreign_key_relations_postgres(schema_name)

        # 1. Napravi graf zavisnosti
        # dependencies[tabela] = {skup tabela od kojih ona zavisi}
        dependencies = {table: set() for table in selected_tables}

        for _, row in relations.iterrows():
            tab = row['table_name']
            parent = row['foreign_table_name']

            # Ako su obe tabele u našem izboru, zabeleži zavisnost
            if tab in dependencies and parent in selected_tables and tab != parent:
                dependencies[tab].add(parent)

        # 2. Algoritam za sortiranje (Kahn's simplified)
        ordered_tables = []
        while dependencies:
            # Pronađi tabele koje nemaju zavisnosti (ili su im zavisnosti već rešene)
            ready_nodes = [t for t, deps in dependencies.items() if not deps]

            if not ready_nodes:
                # Ako imamo kružnu zavisnost, uzmi preostale (fallback)
                ordered_tables.extend(list(dependencies.keys()))
                break

            for node in ready_nodes:
                ordered_tables.append(node)
                del dependencies[node]
                # Ukloni ovu tabelu kao zavisnost iz ostalih preostalih tabela
                for t in dependencies:
                    dependencies[t].discard(node)

        return ordered_tables
