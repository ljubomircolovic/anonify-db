# -*- coding: utf-8 -*-
import os
import json
import hashlib
import random
import logging
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from faker import Faker
from dotenv import load_dotenv

# Konfiguracija logovanja
logger = logging.getLogger(__name__)

# Učitavanje enviroment varijabli iz .env fajla
load_dotenv()

class DBManager:
    def __init__(self, db_url=None):
        # 1. Konfiguracija baze podataka
        self.db_url = db_url or os.getenv(
            "DATABASE_URL",
            "postgresql://user:password@db:5432/anonify_db"
        )

        # Koristimo pool_size i max_overflow za stabilan PARALELIZAM
        self.engine = create_engine(
            self.db_url,
            connect_args={'client_encoding': 'utf8'},
            pool_size=10,        # Broj stalnih konekcija u pool-u
            max_overflow=20      # Koliko dodatnih konekcija dozvoljavamo pod opterećenjem
        )

        # 2. Inicijalizacija Fakera (DACH + US regioni kao što si tražio)
        self.fake = Faker(['de_DE', 'en_US'])

        # 3. Inicijalizacija meta-tabela
        self._init_metadata_table()

        logger.info("✅ DBManager uspešno inicijalizovan sa Connection Pool-om.")

    def get_all_schemas(self):
        inspector = inspect(self.engine)
        all_schemas = inspector.get_schema_names()
        # Isključujemo sistemske šeme i sve što sadrži 'anon'
        forbidden = ['information_schema', 'pg_catalog', 'metadata']
        return [s for s in all_schemas if s not in forbidden and 'anon' not in s.lower()]

    def get_tables_in_schema(self, schema='public'):
        inspector = inspect(self.engine)
        all_tables = inspector.get_table_names(schema=schema)
        # Vraćamo samo tabele koje nemaju 'anon' u nazivu
        return [t for t in all_tables if 'anon' not in t.lower()]

    def read_table(self, table_name, schema_name='public', where=None, limit=100, params=None):
        from sqlalchemy import text
        import pandas as pd

        query_str = f"SELECT * FROM {schema_name}.{table_name}"

        if where and str(where).strip():
            # 1. Čistimo samo reč "WHERE" ali BEZ .lower() nad celim stringom!
            # Koristimo regex ili case-insensitive replace samo za prvu reč
            import re
            clean_filter = re.sub(r'(?i)^where\s+', '', str(where).strip())

            query_str += f" WHERE {clean_filter}"

        if limit:
            query_str += f" LIMIT {limit}"

        query = text(query_str)

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, params or {})
                return pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
            return pd.DataFrame()

    def _ensure_target_table_mirror(self, active_conn, source_schema, target_schema, table_name):
        """Ensures target table exists and mirrors source DDL types exactly."""
        exists_sql = text("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = :target_schema
                  AND table_name = :table_name
            )
        """)
        exists = active_conn.execute(
            exists_sql,
            {"target_schema": target_schema, "table_name": table_name}
        ).scalar()

        if not exists:
            active_conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"'))
            active_conn.execute(text(f'''
                CREATE TABLE "{target_schema}"."{table_name}"
                (LIKE "{source_schema}"."{table_name}" INCLUDING ALL)
            '''))
            return

        # Existing table: enforce exact source type signatures column-by-column
        type_signature_sql = text("""
            SELECT a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS column_type
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = :schema_name
              AND c.relname = :table_name
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
        """)
        source_rows = active_conn.execute(
            type_signature_sql,
            {"schema_name": source_schema, "table_name": table_name}
        ).fetchall()
        target_rows = active_conn.execute(
            type_signature_sql,
            {"schema_name": target_schema, "table_name": table_name}
        ).fetchall()
        source_types = {row[0]: row[1] for row in source_rows}
        target_types = {row[0]: row[1] for row in target_rows}

        for column_name, source_type in source_types.items():
            if column_name in target_types and target_types[column_name] != source_type:
                active_conn.execute(text(f'''
                    ALTER TABLE "{target_schema}"."{table_name}"
                    ALTER COLUMN "{column_name}" TYPE {source_type}
                    USING "{column_name}"::{source_type}
                '''))

    def _cast_dataframe_to_table_types(self, df, active_conn, schema_name, table_name):
        """
        Casts DataFrame values to match DB column types before insertion.
        This prevents invalid input syntax errors for NUMERIC/BIGINT columns.
        """
        if df.empty:
            return df

        type_sql = text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = :schema_name
              AND table_name = :table_name
            ORDER BY ordinal_position
        """)
        rows = active_conn.execute(
            type_sql,
            {"schema_name": schema_name, "table_name": table_name}
        ).fetchall()
        col_types = {row[0]: str(row[1]).lower() for row in rows}

        cast_df = df.copy()
        for col_name, col_type in col_types.items():
            if col_name not in cast_df.columns:
                continue

            if any(token in col_type for token in ["bigint", "integer", "smallint", "int"]):
                numeric_series = pd.to_numeric(cast_df[col_name], errors='coerce')
                cast_df[col_name] = numeric_series.apply(
                    lambda v: int(v) if pd.notnull(v) and float(v).is_integer() else (None if pd.notnull(v) else None)
                )
            elif any(token in col_type for token in ["numeric", "decimal", "double", "real"]):
                coerced = pd.to_numeric(cast_df[col_name], errors='coerce')
                invalid_mask = coerced.isna() & cast_df[col_name].notna()
                if invalid_mask.any():
                    invalid_idx = invalid_mask[invalid_mask].index
                    fallback_values = pd.to_numeric(df.loc[invalid_idx, col_name], errors='coerce')
                    fallback_ok = fallback_values.notna()
                    if fallback_ok.any():
                        restore_idx = fallback_values[fallback_ok].index
                        logger.warning(
                            f"⚠️ Numeric cast fallback on '{col_name}': reverting {int(fallback_ok.sum())} values to original."
                        )
                        coerced.loc[restore_idx] = fallback_values[fallback_ok]
                    remaining_invalid = coerced.isna() & cast_df[col_name].notna()
                    if remaining_invalid.any():
                        logger.warning(
                            f"⚠️ Numeric cast unresolved on '{col_name}': {int(remaining_invalid.sum())} values set to NULL."
                        )
                cast_df[col_name] = coerced
            elif "boolean" in col_type:
                bool_map = {
                    "true": True, "false": False,
                    "t": True, "f": False,
                    "1": True, "0": False,
                    "yes": True, "no": False
                }
                cast_df[col_name] = cast_df[col_name].apply(
                    lambda v: bool_map.get(str(v).strip().lower(), v) if pd.notnull(v) else v
                )
            elif any(token in col_type for token in ["date", "timestamp", "time"]):
                cast_df[col_name] = pd.to_datetime(cast_df[col_name], errors='coerce')

        return cast_df

    def save_anonymized_table(self, df, table_name, target_schema='anon', conn=None, source_schema='public'):
        """
        Snima DataFrame u ciljnu tabelu. Ako je 'conn' prisutan, ostaje u istoj sesiji.
        """
        from sqlalchemy import text

        def _run_save(active_conn):
            self._ensure_target_table_mirror(active_conn, source_schema, target_schema, table_name)
            safe_df = self._cast_dataframe_to_table_types(df, active_conn, target_schema, table_name)

            # Osiguravamo šemu ako ne postoji
            active_conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))

            # Upisujemo podatke koristeći 'append' (TRUNCATE je već odrađen na nivou Batch-a)
            # KLJUČNO: Ovde koristimo active_conn umesto self.engine
            safe_df.to_sql(
                table_name,
                active_conn,
                schema=target_schema,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            return True

        try:
            if conn:
                # Deo Batch transakcije
                return _run_save(conn)
            else:
                # Samostalni upis
                with self.engine.connect() as standalone_conn:
                    with standalone_conn.begin():
                        return _run_save(standalone_conn)
        except Exception as e:
            print(f"❌ Error saving to {target_schema}.{table_name}: {e}")
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

    def apply_anonymization_rules(self, df, table_plan, salt="secret_123"):
        """
        Transformacija podataka prema Type-Safe pravilima.
        SADA SA ZAŠTITOM OD 'TypeError' I PROVEROM TIPOVA.
        """
        import hashlib
        import pandas as pd
        import random
        from datetime import timedelta
        import json

        if df.empty:
            return df

        df_anon = df.copy()

        # 1. OSIGURAČ: Ako je table_plan stigao kao string, pretvori ga u listu
        if isinstance(table_plan, str):
            try:
                table_plan = json.loads(table_plan)
            except:
                print("❌ Error: Failed to parse plan in apply_anonymization_rules")
                return df_anon

        for item in table_plan:
            # --- 2. KLJUČNA ISPRAVKA ZA TVOJ BUG ---
            # Preskačemo ako item nije rečnik (npr. ako je zalutao string 'plan' ili 'where')
            if not isinstance(item, dict):
                continue

            col = item.get('column')
            strategy = str(item.get('strategy', 'keep')).lower()
            original_series = df_anon[col].copy() if col in df_anon.columns else None

            if not col or col not in df_anon.columns or strategy == 'keep':
                continue

            # --- 3. LOGIKA PO STRATEGIJAMA ---

            # NULL
            if strategy == 'null':
                df_anon[col] = None

            # FAKER (Dodajemo try-except za svaki red radi sigurnosti)
            elif strategy in ['faker_name', 'faker_email', 'faker_phone']:
                def get_faker(strat):
                    try:
                        if strat == 'faker_name': return self.fake.name()
                        if strat == 'faker_email': return self.fake.email()
                        if strat == 'faker_phone': return self.fake.phone_number()
                    except: return "Redacted"
                df_anon[col] = [get_faker(strategy) for _ in range(len(df_anon))]

            # DATE SHIFT (Sada proverava da li je kolona zaista datum)
            elif strategy == 'date_shift':
                if pd.api.types.is_datetime64_any_dtype(df_anon[col]):
                    def shift_date(val):
                        if pd.isnull(val): return val
                        days_to_shift = random.randint(-30, 30)
                        try:
                            return val + timedelta(days=days_to_shift)
                        except: return val
                    df_anon[col] = df_anon[col].apply(shift_date)

            # NOISE (Samo za numeričke tipove)
            elif strategy == 'noise':
                if pd.api.types.is_numeric_dtype(df_anon[col]):
                    def add_noise(val):
                        if pd.isnull(val): return val
                        variation = float(val) * random.uniform(-0.1, 0.1)
                        # Čuvamo originalni tip (int ostaje int, float ostaje float)
                        return type(val)(val + variation)
                    df_anon[col] = df_anon[col].apply(add_noise)

            # DETERMINISTIČKI MAPPING (Koristi tvoj salt)
            elif strategy == 'mapping':
                category = "first_name"
                if "last" in col.lower(): category = "last_name"
                elif "city" in col.lower(): category = "city"

                m_list = self._get_mapping_values_by_locale(category, 'de')
                if m_list:
                    df_anon[col] = df_anon[col].apply(
                        lambda x: self._deterministic_map(x, m_list, salt) if pd.notnull(x) else x
                    )

            # HASH (SHA-256 sa solju)
            elif strategy == 'hash':
                def secure_hash(val):
                    if pd.isnull(val): return val
                    hash_obj = hashlib.sha256(f"{val}{salt}".encode())
                    return hash_obj.hexdigest()[:12]
                df_anon[col] = df_anon[col].apply(secure_hash)

            # MASK
            elif strategy == 'mask':
                is_numeric_like_column = pd.api.types.is_numeric_dtype(original_series)
                if is_numeric_like_column:
                    def sanitize_numeric_mask(masked_val, fallback_val):
                        import re
                        if pd.isnull(masked_val):
                            return fallback_val
                        cleaned = re.sub(r'[^0-9.]', '', str(masked_val))
                        if cleaned.count('.') > 1:
                            first_dot = cleaned.find('.')
                            cleaned = cleaned[:first_dot + 1] + cleaned[first_dot + 1:].replace('.', '')
                        if cleaned in ("", "."):
                            logger.warning(
                                f"⚠️ Numeric sanitization fallback on column '{col}' for value '{masked_val}'. Keeping original value."
                            )
                            return fallback_val
                        try:
                            return float(cleaned)
                        except Exception:
                            logger.warning(
                                f"⚠️ Numeric cast fallback on column '{col}' for value '{masked_val}'. Keeping original value."
                            )
                            return fallback_val

                    masked_series = df_anon[col].apply(
                        lambda x: self.mask_value(x) if pd.notnull(x) else x
                    )
                    df_anon[col] = [
                        sanitize_numeric_mask(masked_val, fallback_val)
                        for masked_val, fallback_val in zip(masked_series, original_series)
                    ]
                else:
                    df_anon[col] = df_anon[col].apply(
                        lambda x: self.mask_value(x) if pd.notnull(x) else x
                    )

            # Final numeric guard: if transformed numeric-like column is invalid, keep original values.
            if pd.api.types.is_numeric_dtype(original_series):
                coerced = pd.to_numeric(df_anon[col], errors='coerce')
                invalid_mask = coerced.isna() & original_series.notna()
                if invalid_mask.any():
                    logger.warning(
                        f"⚠️ Numeric integrity fallback on '{col}': {int(invalid_mask.sum())} invalid values reverted to original."
                    )
                    df_anon.loc[invalid_mask, col] = original_series.loc[invalid_mask]

        return df_anon

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

    def save_ai_plan(self, schema_name, table_name, plan_data, where_condition=""):
        """
        Saves the entire plan as a single JSON and the WHERE filter
        into the metadata.ai_plans table.
        """
        import json
        from sqlalchemy import text

        query = text("""
            INSERT INTO metadata.ai_plans (
                schema_name,
                table_name,
                plan_json,
                where_condition,
                last_updated
            )
            VALUES (:s, :t, :p, :w, CURRENT_TIMESTAMP)
            ON CONFLICT (schema_name, table_name)
            DO UPDATE SET
                plan_json = EXCLUDED.plan_json,
                where_condition = EXCLUDED.where_condition,
                last_updated = CURRENT_TIMESTAMP
        """)

        try:
            with self.engine.connect() as conn:
                conn.execute(query, {
                    "s": schema_name,
                    "t": table_name,
                    "p": json.dumps(plan_data),
                    "w": where_condition
                })
                conn.commit()
                logger.info(f"✅ Plan & Filter successfully saved for {schema_name}.{table_name}")
                return True
        except Exception as e:
            logger.error(f"❌ Error saving to metadata.ai_plans for {table_name}: {e}")
            return False

    def get_saved_plan(self, schema_name, table_name):
        """
        Dohvata plan i WHERE uslov.
        Garantuje da je 'plan' Python LISTA, a ne string.
        """
        import json
        from sqlalchemy import text

        query = text("""
            SELECT plan_json, where_condition
            FROM metadata.ai_plans
            WHERE schema_name = :s AND table_name = :t
            LIMIT 1
        """)

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"s": schema_name, "t": table_name}).fetchone()

                if result:
                    raw_plan = result[0]
                    where_cond = result[1] or ""

                    # --- KRITIČNA ZONA: DESERIJALIZACIJA ---
                    # 1. Ako je raw_plan string (JSON u bazi), pretvori ga u Python objekat
                    if isinstance(raw_plan, str):
                        try:
                            plan_data = json.loads(raw_plan)
                        except json.JSONDecodeError:
                            print(f"❌ JSON greška za {table_name}: Neispravan format u bazi.")
                            plan_data = []
                    else:
                        plan_data = raw_plan

                    # 2. DODATNA PROVERA: Dupla serijalizacija (česta pojava)
                    # Ako je plan_data i dalje string nakon prvog json.loads, uradi još jednom
                    if isinstance(plan_data, str):
                        plan_data = json.loads(plan_data)

                    # 3. NORMALIZACIJA: Osiguravamo da je final_plan LISTA
                    if isinstance(plan_data, dict) and "plan" in plan_data:
                        final_plan = plan_data["plan"]
                    elif isinstance(plan_data, list):
                        final_plan = plan_data
                    else:
                        final_plan = []

                    # 4. FINALNA PROVERA TIPA (Batch procesor zaštita)
                    # Ako final_plan nije lista, pretvaramo ga u praznu listu da izbegnemo pad
                    if not isinstance(final_plan, list):
                        print(f"⚠️ Warning: Plan za {table_name} nije lista nego {type(final_plan)}")
                        final_plan = []

                    return {
                        "plan": final_plan,
                        "where": where_cond
                    }

                return None
        except Exception as e:
            print(f"❌ Error loading from metadata.ai_plans: {e}")
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

    def load_forced_mappings_from_db(self, schema_name='ecommerce'):
        from sqlalchemy import text
        # Koristimo duple navodnike za svaki deo naziva da izbegnemo probleme sa Case-Sensitivity
        query = text(f'SELECT column_name, is_pii, strategy, reason FROM "{schema_name}"."anon_forced_mappings"')

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query)
                # Vraćamo rečnik sa malim slovima radi lakšeg poređenja
                return {row.column_name.lower(): {
                    "is_pii": row.is_pii,
                    "strategy": row.strategy,
                    "reason": row.reason
                } for row in result}
        except Exception as e:
            # Ovde ćemo ispisati tačnu grešku u konzolu da je vidimo u Docker logovima
            print(f"❌ DATABASE ERROR: {str(e)}")
            return {}

    def analyze_table_structure(self, df_sample, agent, schema_name='ecommerce'):
        columns = df_sample.columns.tolist()
        db_mappings = self.load_forced_mappings_from_db(schema_name)

        to_analyze = []  # Lista za AI
        final_plan = []  # Konačan rezultat

        for col in columns:
            col_lower = col.lower()
            if col_lower in db_mappings:
                # Uzimamo iz baze (bez zvanja Azure-a)
                rule = db_mappings[col_lower].copy()
                rule["column"] = col
                final_plan.append(rule)
            else:
                # Pripremamo za grupni AI poziv
                sample_data = df_sample[col].dropna().head(3).tolist()
                to_analyze.append({
                    "column": col,
                    "sample_values": [str(v) for v in sample_data]
                })

        # AKO IMA KOLONA ZA ANALIZU, ŠALJEMO IH SVE ODJEDNOM
        if to_analyze:
            ai_response = agent.analyze_metadata(to_analyze)
            # Proveravamo da li je odgovor validan i da li ima 'plan'
            if ai_response and hasattr(ai_response, 'plan'):
                for item in ai_response.plan:
                    final_plan.append({
                        "column": item.column,
                        "is_pii": item.is_pii,
                        "strategy": item.strategy,
                        "reason": item.reason
                    })

        return {"plan": final_plan}

    def prepare_anonymization_target(self, source_schema, target_schema, ordered_tables):
        """
        Faza 1: Kreira šemu i tabele sa indeksima, ali BEZ stranih ključeva.
        """
        with self.engine.connect() as conn:
            # 1. Osiguraj da target šema postoji
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))

            for table in ordered_tables:
                print(f"🏗️  Kreiram skeleton za: {target_schema}.{table}")

                # Brišemo staru tabelu ako postoji (CASCADE čisti i stare veze)
                conn.execute(text(f"DROP TABLE IF EXISTS {target_schema}.{table} CASCADE"))

                # LIKE ... INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES
                # Namerno NE uključujemo FK u ovom koraku ako verzija Postgresa to dozvoljava,
                # ili ih čistimo odmah nakon kreiranja.
                conn.execute(text(f"""
                    CREATE TABLE {target_schema}.{table}
                    (LIKE {source_schema}.{table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)
                """))

                # Uklanjanje FK-ova koji su možda prekopirani (za svaki slučaj)
                self._drop_fks_from_table(conn, target_schema, table)

            conn.commit()

    def _drop_fks_from_table(self, conn, schema, table):
        """Pomoćna metoda za uklanjanje FK-ova pre punjenja podataka."""
        query = text(f"""
            SELECT conname
            FROM pg_constraint
            WHERE contype = 'f'
            AND conrelid = '{schema}.{table}'::regclass
        """)
        fks = conn.execute(query).fetchall()
        for fk in fks:
            conn.execute(text(f"ALTER TABLE {schema}.{table} DROP CONSTRAINT {fk[0]}"))

    def restore_foreign_keys(self, source_schema, target_schema, tables):
        """Prebacuje FK constraints sa izvora na target."""
        query = text("""
            SELECT
                conname,
                pg_get_constraintdef(oid) as def
            FROM pg_constraint
            WHERE contype = 'f'
            AND conrelid::regclass::text LIKE :schema_prefix
        """)

        with self.engine.connect() as conn:
            # Tražimo sve FK-ove u izvornoj šemi
            res = conn.execute(query, {"schema_prefix": f"{source_schema}.%"})
            for row in res:
                con_name = row[0]
                con_def = row[1]
                # Modifikujemo definiciju da pokazuje na target šemu
                # Primer: REFERENCES ecommerce.customers(id) -> REFERENCES ecommerce_anon.customers(id)
                new_def = con_def.replace(f"{source_schema}.", f"{target_schema}.")

                # Nađi na kojoj je tabeli taj constraint
                table_query = text(f"SELECT relname FROM pg_class c JOIN pg_constraint con ON con.conrelid = c.oid WHERE con.conname = '{con_name}'")
                tab_name = conn.execute(table_query).fetchone()[0]

                if tab_name in tables:
                    try:
                        conn.execute(text(f'ALTER TABLE "{target_schema}"."{tab_name}" ADD CONSTRAINT "{con_name}" {new_def}'))
                    except Exception as e:
                        print(f"⚠️ Mismatch on FK {con_name}: {e}")
            conn.commit()

    def drop_all_fks_for_table(self, conn, schema, table):
        """
        Pronalazi FK-ove, zapisuje ih u metadata.pending_fks i briše ih.
        """
        from sqlalchemy import text

        find_fks_sql = text("""
            SELECT
                conname,
                relname,
                pg_get_constraintdef(c.oid) as constraint_def
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            WHERE n.nspname = :schema
            AND (t.relname = :table OR c.confrelid = (SELECT oid FROM pg_class WHERE relname = :table AND relnamespace = n.oid))
            AND c.contype = 'f';
        """)

        results = conn.execute(find_fks_sql, {"schema": schema, "table": table}).fetchall()
        rehook_commands = []

        for conname, relname, condef in results:
            rehook_sql = f'ALTER TABLE "{schema}"."{relname}" ADD CONSTRAINT "{conname}" {condef}'
            rehook_commands.append(rehook_sql)

            # --- ZAPIS U METADATA TABELU ---
            conn.execute(text("""
                INSERT INTO metadata.pending_fks (target_schema, table_name, constraint_name, rehook_sql)
                VALUES (:s, :t, :c, :sql)
            """), {"s": schema, "t": relname, "c": conname, "sql": rehook_sql})

            print(f"💾 [META] Zapisan FK '{conname}' u pending_fks")
            conn.execute(text(f'ALTER TABLE "{schema}"."{relname}" DROP CONSTRAINT IF EXISTS "{conname}"'))

        return rehook_commands

    def drop_target_schema(self, target_schema):
        from sqlalchemy import text
        with self.engine.connect() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE'))
            conn.commit()

    def get_primary_keys(self, schema, table):
        """Vraća listu kolona koje su Primary Key za datu tabelu koristeći DDL meta-podatke."""
        query = text("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = :schema
            AND tc.table_name = :table;
        """)
        try:
            df = pd.read_sql(query, self.engine, params={"schema": schema, "table": table})
            print(f"🚀 [BATCH] {table}: Found {len(df)} PK columns.")
            return df['column_name'].tolist()
        except Exception as e:
            print(f"Error fetching PKs for {table}: {e}")
            return []

    def table_exists(self, table_name, schema_name):
        """Proverava da li tabela postoji u specifičnoj šemi."""
        query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = :s
                AND table_name = :t
            )
        """)
        with self.engine.connect() as conn:
            return conn.execute(query, {"s": schema_name.lower(), "t": table_name.lower()}).scalar()

    def get_row_count(self, table_name, schema_name):
        """Vraća broj redova u tabeli."""
        # Čista SQL sintaksa bez nepotrebnih navodnika
        query = text(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
        with self.engine.connect() as conn:
            return conn.execute(query).scalar()

    def get_column_details(self, table_name, schema_name):
        """
        Vraća proširene meta-podatke o kolonama (tip i nullability) za strogu validaciju.
        """
        query = text("""
            SELECT
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = :s AND table_name = :t
            ORDER BY ordinal_position
        """)
        with self.engine.connect() as conn:
            result = conn.execute(query, {"s": schema_name, "t": table_name})

            # Pakujemo u rečnik gde je ključ ime kolone,
            # a vrednost je novi rečnik sa detaljima
            return {
                row[0]: {
                    "type": row[1],
                    "nullable": row[2]  # Vraća 'YES' ili 'NO'
                }
                for row in result
            }

    def get_all_foreign_keys(self, schema_name):
        """
        Izvlači sve Foreign Key relacije u šemi kako bi se osigurao referencijalni integritet.
        Vraća listu torki: (source_table, source_column, target_table, target_column)
        """
        from sqlalchemy import text

        query = text("""
            SELECT
                kcu.table_name as source_table,
                kcu.column_name as source_column,
                rel_kcu.table_name as target_table,
                rel_kcu.column_name as target_column
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu
              ON tco.constraint_name = kcu.constraint_name
            JOIN information_schema.referential_constraints rco
              ON tco.constraint_name = rco.constraint_name
            JOIN information_schema.key_column_usage rel_kcu
              ON rco.unique_constraint_name = rel_kcu.constraint_name
            WHERE tco.constraint_type = 'FOREIGN KEY'
              AND tco.table_schema = :s
        """)

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"s": schema_name})
                # Vraćamo listu torki za lakšu iteraciju u validaciji
                return [(row[0], row[1], row[2], row[3]) for row in result]
        except Exception as e:
            print(f"Error fetching foreign keys: {e}")
            return []

    def get_all_saved_plans(self, schema_name):
        """
        Dohvata sve sačuvane planove koristeći ispravno ime kolone 'plan_json'.
        """
        import json
        from sqlalchemy import text

        # Popravljeno ime kolone: plan -> plan_json
        query = text("""
            SELECT table_name, plan_json
            FROM metadata.ai_plans
            WHERE schema_name = :s
        """)

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"s": schema_name})
                plans = {}
                for row in result:
                    # row[0] je table_name, row[1] je plan_json
                    table_name_db = row[0]
                    raw_plan = row[1]

                    if raw_plan is None:
                        plans[table_name_db] = []
                        continue

                    # Provera da li je JSON već dikt (ako koristiš JSONB) ili string
                    if isinstance(raw_plan, str):
                        plans[table_name_db] = json.loads(raw_plan)
                    else:
                        plans[table_name_db] = raw_plan

                return plans
        except Exception as e:
            # Ako Postgres baci error, transakcija mora da se "ohladi"
            print(f"❌ Error fetching all saved plans: {e}")
            return {}

    def align_db_types(self, target_schema, table_name, plan, conn=None):
        """
        Legacy hook retained for batch compatibility.
        Type mirroring is now enforced from source DDL and should not be altered.
        """
        return []

    def drop_all_fks_for_table(self, conn, schema, table):
        """
        Pronalazi FK-ove, zapisuje ih u metadata.pending_fks i briše ih.
        Pazi: 'table' je argument funkcije koji prosleđujemo SQL-u.
        """
        from sqlalchemy import text

        find_fks_sql = text("""
            SELECT
                conname,
                relname,
                pg_get_constraintdef(c.oid) as constraint_def
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            WHERE n.nspname = :schema_name
            AND (
                t.relname = :table_name
                OR c.confrelid = (
                    SELECT oid FROM pg_class
                    WHERE relname = :table_name
                    AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = :schema_name)
                )
            )
            AND c.contype = 'f';
        """)

        # OVO JE KLJUČNO: Mapiranje argumenata funkcije na SQL parametre
        results = conn.execute(find_fks_sql, {
            "schema_name": schema,
            "table_name": table
        }).fetchall()

        rehook_commands = []

        for conname, relname, condef in results:
            rehook_sql = f"ALTER TABLE {schema}.{relname} ADD CONSTRAINT {conname} {condef}"
            rehook_commands.append(rehook_sql)

            # Zapis u bazu da imamo trag ako nešto pukne
            conn.execute(text("""
                INSERT INTO metadata.pending_fks (target_schema, table_name, constraint_name, rehook_sql)
                VALUES (:s, :t, :c, :sql)
            """), {
                "s": schema,
                "t": relname,
                "c": conname,
                "sql": rehook_sql
            })

            print(f"💾 [META] Zapisan FK '{conname}' za tabelu '{relname}'")
            conn.execute(text(f'ALTER TABLE "{schema}"."{relname}" DROP CONSTRAINT IF EXISTS "{conname}"'))

        return rehook_commands

    def rehook_foreign_keys(self, conn, commands):
        """
        Vraća FK integritet i čisti metadata.pending_fks.
        """
        from sqlalchemy import text
        print(f"\n⚓ [RE-HOOK] Vraćam {len(commands)} Foreign Key-eva...")

        for cmd in commands:
            try:
                conn.execute(text(cmd))
                # --- ČIŠĆENJE IZ METADATA ---
                # Izvlačimo ime constrainta iz komande (grubo, ali radi za logiku čišćenja)
                con_name = cmd.split('CONSTRAINT "')[1].split('"')[0]
                conn.execute(text("DELETE FROM metadata.pending_fks WHERE constraint_name = :c"), {"c": con_name})

                print(f"✅ Successfully restored: {con_name}")
            except Exception as e:
                print(f"⚠️ [RE-HOOK WARNING] Neuspelo vraćanje: {e}")

    def truncate_anon_tables(self, target_schema, ordered_tables):
        from sqlalchemy import text
        if not ordered_tables: return

        # Spajamo tabele: "anon"."customers", "anon"."orders"
        tables_to_clear = ", ".join([f'"{target_schema}"."{t}"' for t in ordered_tables])

        # Dodajemo RESTART IDENTITY da ID-evi krenu od 1
        sql = text(f"TRUNCATE TABLE {tables_to_clear} RESTART IDENTITY CASCADE;")

        with self.engine.connect() as conn:
            # 1. Isključujemo trigere i FK provere za ovu sesiju
            conn.execute(text("SET session_replication_role = 'replica';"))

            print(f"DEBUG: Pokrećem TRUNCATE nad {tables_to_clear}")
            conn.execute(sql)

            # 2. Vraćamo u normalu
            conn.execute(text("SET session_replication_role = 'origin';"))

            # 3. FORCE COMMIT
            conn.commit()
            print("DEBUG: TRUNCATE executed and committed successfully.")

    def prepare_subset_metadata(self, conn):
        """
        Pravi privremenu tabelu i OSIGURAVA da je vidljiva u istoj sesiji.
        """
        from sqlalchemy import text

        # 1. Kreiramo tabelu
        # Koristimo ON COMMIT PRESERVE ROWS da tabela ne nestane pri svakom malom commitu
        conn.execute(text("""
            CREATE TEMP TABLE IF NOT EXISTS subset_tracking (
                column_name VARCHAR(255),
                key_value VARCHAR(255),
                PRIMARY KEY (column_name, key_value)
            ) ON COMMIT PRESERVE ROWS;
        """))

        # 2. KLJUČNO: Moramo potvrditi kreiranje pre nego što bilo šta ubacimo
        # U SQLAlchemy 2.0 na Connection objektu koristimo commit()
        # (ako tvoj setup podržava manualne transakcije unutar bloka)
        # ili jednostavno idemo odmah na INSERT.

        print("🛠️ Temp tabela 'subset_tracking' kreirana.")

        # Izmeni onaj debug INSERT da bude bezbedniji
        try:
            conn.execute(text("INSERT INTO subset_tracking (column_name, key_value) VALUES ('test', '1') ON CONFLICT DO NOTHING"))
            print("✅ Debug INSERT uspešan.")
        except Exception as e:
            print(f"❌ Error during debug INSERT: {e}")

    def register_keys(self, conn, column_name, values):
        """Ubacuje ključeve u temp tabelu za kasniji JOIN."""
        if not values: return

        # Batch insert ključeva
        data = [{"c": column_name, "v": str(v)} for v in values]
        conn.execute(text("""
            INSERT INTO subset_tracking (column_name, key_value)
            VALUES (:c, :v)
            ON CONFLICT DO NOTHING
        """), data)

    def execute_anonymization_batch(self, selected_schema, target_schema, full_plan, ordered_tables):
        """
        Enterprise-grade batch: Subsetting + RI Propagacija + FK Re-Hook.
        Sve unutar jedne 'begin()' transakcije da TEMP TABLE ne bi nestala.
        """
        from sqlalchemy import text
        import pandas as pd

        all_rehook_commands = []

        # 1. Otvaramo konekciju
        with self.engine.connect() as conn:
            # 2. POKREĆEMO TRANSAKCIJU (Ovo drži sesiju i TEMP tabelu živom!)
            with conn.begin():
                # Priprema RI Subset mehanizma (sada unutar transakcije)
                self.prepare_subset_metadata(conn)

                all_relations = self.get_all_foreign_keys(selected_schema)

                for table_name in ordered_tables:
                    if table_name not in full_plan:
                        continue

                    data = full_plan[table_name]
                    plan = data.get('plan', [])
                    base_where = data.get('where', "").strip()

                    # --- SUBSET JOIN LOGIKA ---
                    parent_filters = []
                    for rel in all_relations:
                        child_table, child_col, parent_table, parent_col = rel[0], rel[1], rel[2], rel[3]
                        if child_table == table_name:
                            parent_filters.append((child_col, parent_col))

                    query = f"SELECT t.* FROM {selected_schema}.{table_name} t"

                    if parent_filters:
                        for i, (c_col, p_col) in enumerate(parent_filters):
                            alias = f"s{i}"
                            # Kastujemo u VARCHAR jer temp tabela čuva sve kao stringove
                            query += f" JOIN subset_tracking {alias} ON t.{c_col}::VARCHAR = {alias}.key_value"
                            query += f" AND {alias}.column_name = '{p_col}'"

                    if base_where:
                        # Dodajemo filter, pazeći na to da li već imamo JOIN/WHERE
                        query += f" WHERE ({base_where})"

                    print(f"🚀 [BATCH] Procesuiram: {table_name}")

                    # Čitanje mora ići preko iste 'conn'
                    df = pd.read_sql(text(query), conn)

                    if df.empty:
                        print(f"⚠️ Tabela {table_name} je prazna nakon subsettinga. Preskačem.")
                        continue

                    # --- REGISTRACIJA KLJUČEVA ---
                    for rel in all_relations:
                        if rel[2] == table_name:
                            p_col = rel[3]
                            if p_col in df.columns:
                                unique_keys = df[p_col].unique().tolist()
                                self.register_keys(conn, p_col, unique_keys)

                    # --- DDL SYNC & SAVE ---
                    # Prosleđujemo 'conn' da bi align_db_types mogao da piše u pending_fks
                    table_fks = self.align_db_types(target_schema, table_name, plan, conn=conn)
                    if table_fks:
                        all_rehook_commands.extend(table_fks)

                    # Anonimizacija i snimanje (prosleđujemo conn!)
                    df_anon = self.apply_anonymization_rules(df, plan)
                    self.save_anonymized_table(
                        df_anon,
                        table_name,
                        target_schema,
                        conn=conn,
                        source_schema=selected_schema
                    )

                # --- FINALNI KORAK: RE-HOOK ---
                if all_rehook_commands:
                    unique_fks = list(set(all_rehook_commands))
                    self.rehook_foreign_keys(conn, unique_fks)

                # Na kraju 'with conn.begin():' bloka, SQLAlchemy AUTOMATSKI radi COMMIT.
                # Ako se desi greška bilo gde iznad, AUTOMATSKI radi ROLLBACK.

            print("🎯 [COMPLETED] Batch proces uspešno završen.")

    def get_table_sample(self, schema, table, limit=5):
        """
        Dohvata uzorak podataka za AI analizu.
        Sve pretvara u string da bi izbegli probleme sa specifičnim tipovima u JSON-u.
        """
        from sqlalchemy import text
        import pandas as pd

        query = f"SELECT * FROM {schema}.{table} LIMIT {limit}"
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
                # Bitno: pretvaramo sve u stringove pre slanja AI-ju
                return df.astype(str).to_dict(orient='records')
        except Exception as e:
            logger.error(f"⚠️ Error fetching sample for {table}: {e}")
            return []

    def get_tables(self, schema_name='public'):
        """
        Vraća listu svih tabela u zadatoj šemi, isključujući interne AnonifyDB tabele.
        """
        # Lista tabela koje sistem koristi interno i ne treba da budu ponuđene korisniku
        excluded_tables = ("anon_forced_mappings", "anonymization_logs", "audit_trail")

        query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
            AND table_type = 'BASE TABLE'
            AND table_name NOT IN {excluded_tables}
            ORDER BY table_name;
        """
        try:
            df = pd.read_sql(query, self.engine)
            return df['table_name'].tolist()
        except Exception as e:
            logger.error(f"❌ Error listing tables: {e}")
            return []