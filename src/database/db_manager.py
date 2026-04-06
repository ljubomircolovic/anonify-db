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
from sqlalchemy import text

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
        all_schemas = inspector.get_schema_names()
        # Isključujemo sistemske šeme i sve što sadrži 'anon'
        forbidden = ['information_schema', 'pg_catalog', 'metadata']
        return [s for s in all_schemas if s not in forbidden and 'anon' not in s.lower()]

    def get_tables_in_schema(self, schema='public'):
        inspector = inspect(self.engine)
        all_tables = inspector.get_table_names(schema=schema)
        # Vraćamo samo tabele koje nemaju 'anon' u nazivu
        return [t for t in all_tables if 'anon' not in t.lower()]

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


    def apply_anonymization_rules(self, df, table_plan, salt="secret_123"):
        """
        Transformacija podataka na nivou tabele.
        """
        import hashlib
        df_anon = df.copy()

        # Ovde je bila greška: koristimo 'table_plan' umesto 'plan'
        for item in table_plan:
            col = item['column']
            strategy = item.get('strategy', 'keep').lower()

            if col not in df_anon.columns or strategy == 'keep':
                continue

            # --- 1. FAKER STRATEGIJE ---
            if 'faker_first_name' in strategy:
                df_anon[col] = [self.fake.first_name() for _ in range(len(df_anon))]
            elif 'faker_last_name' in strategy:
                df_anon[col] = [self.fake.last_name() for _ in range(len(df_anon))]
            elif 'faker_email' in strategy:
                df_anon[col] = [self.fake.email() for _ in range(len(df_anon))]

            # --- 2. DETERMINISTIČKI MAPPING (Tvoja custom logika iz baze) ---
            elif strategy == 'mapping':
                # Mapiramo na osnovu imena kolone (fallback na first_name ako ne prepoznamo)
                category = "first_name"
                if "last" in col.lower(): category = "last_name"
                elif "city" in col.lower(): category = "city"

                m_list = self._get_mapping_values_by_locale(category, 'de')
                if m_list:
                    df_anon[col] = df_anon[col].apply(lambda x: self._deterministic_map(x, m_list, salt) if pd.notnull(x) else x)

            # --- 3. HASH / MASK / NOISE ---
            elif strategy == 'hash':
                df_anon[col] = df_anon[col].apply(
                    lambda x: hashlib.sha256(f"{x}{salt}".encode()).hexdigest()[:12] if pd.notnull(x) else x
                )

            elif strategy == 'mask':
                df_anon[col] = df_anon[col].apply(
                    lambda x: self.mask_value(x) if pd.notnull(x) else x
                )

        return df_anon


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
        """Loads the plan from the plan_json column and ensures it is a list."""
        import json
        from sqlalchemy import text

        # IZBACILI SMO 'ORDER BY created_at' jer kolona ne postoji u bazi
        query = text("""
            SELECT plan_json FROM metadata.ai_plans
            WHERE schema_name = :s AND table_name = :t
            LIMIT 1
        """)

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"s": schema_name, "t": table_name}).fetchone()

                if result and result[0]:
                    data = result[0]

                    # 1. Parsiranje: TEXT -> JSON (list/dict), JSONB -> already Python object
                    if isinstance(data, str):
                        try:
                            plan_list = json.loads(data)
                        except json.JSONDecodeError:
                            print(f"Error decoding JSON string for {table_name}")
                            return None
                    else:
                        plan_list = data

                    # 2. Raspakivanje ako je unutar {"plan": [...]}
                    if isinstance(plan_list, dict) and "plan" in plan_list:
                        return plan_list["plan"]

                    # 3. Osiguravamo da vraćamo listu (bitno za st.data_editor)
                    return plan_list if isinstance(plan_list, list) else None

                return None
        except Exception as e:
            # Ovo će ti sada ispisati u logu ako postoji bilo koji drugi problem
            print(f"Error loading from plan_json for {table_name}: {e}")
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

    # U src/database/db_manager.py

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

    # U src/database/db_manager.py

# U src/database/db_manager.py

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

    def execute_anonymization_batch(self, source_schema, target_schema, execution_plan):
        # Osiguravamo da tabele idu po redosledu zavisnosti
        ordered_tables = list(execution_plan.keys())

        # Faza 1: DDL Skeleton (Bez FK)
        self.prepare_anonymization_target(source_schema, target_schema, ordered_tables)

        for table in ordered_tables:
            print(f"⚡ Processing: {table}")

            # Koristimo text() za sigurnost
            query = text(f'SELECT * FROM "{source_schema}"."{table}"')
            df = pd.read_sql(query, self.engine)

            if df.empty:
                print(f"⚠️ Table {table} is empty, skipping...")
                continue

            # Faza 2: Transform
            df_anon = self.apply_anonymization_rules(df, execution_plan[table])

            # Load (Koristimo 'append' jer je prepare_anonymization_target već napravio tabelu)
            df_anon.to_sql(table, self.engine, schema=target_schema, if_exists='append', index=False)

        # Faza 3: Rekonstrukcija veza
        print("🔗 Rekonstrukcija Foreign Key relacija...")
        self.restore_foreign_keys(source_schema, target_schema, ordered_tables)
        print("✅ Batch proces uspešno završen!")


    def drop_target_schema(self, target_schema):
        from sqlalchemy import text
        with self.engine.connect() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE'))
            conn.commit()

    def get_primary_keys(self, schema, table):
        """Vraća listu kolona koje su Primary Key za datu tabelu koristeći DDL meta-podatke."""
        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = %s
            AND tc.table_name = %s;
        """
        try:
            df = pd.read_sql(query, self.engine, params=(schema, table))
            return df['column_name'].tolist()
        except Exception as e:
            print(f"Error fetching PKs: {e}")
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
        # Koristimo f-string za ime tabele jer SQLAlchemy ne dozvoljava parametrizaciju imena objekata
        query = text(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
        with self.engine.connect() as conn:
            return conn.execute(query).scalar()