# -*- coding: utf-8 -*-
import pandas as pd
import logging
import os
from sqlalchemy import text
from src.db import DBManager
import hashlib


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DB_Init")


create_tables_sql = """
    CREATE TABLE IF NOT EXISTS metadata.mapping_catalog (
        id SERIAL PRIMARY KEY,
        category_name VARCHAR(50) UNIQUE NOT NULL,
        description TEXT,
        file_hash VARCHAR(64) -- DODATO: Za čuvanje SHA-256 otiska fajla
    );
    ...
    """

def get_file_hash(filepath):
    """Generiše SHA-256 hash sadržaja fajla."""
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def initialize_metadata():
    import os
    import pandas as pd
    import hashlib
    from sqlalchemy import text

    db = DBManager()
    seed_path = "data/seed/"
    init_logs = []

    def get_file_hash(filepath):
        hasher = hashlib.sha256()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    create_schema_sql = "CREATE SCHEMA IF NOT EXISTS metadata;"

    # NOVA STRUKTURA: Dodat locale i UNIQUE na (category, locale)
    create_tables_sql = """
    CREATE TABLE IF NOT EXISTS metadata.mapping_catalog (
        id SERIAL PRIMARY KEY,
        category_name VARCHAR(50) NOT NULL,
        locale VARCHAR(10) NOT NULL,
        description TEXT,
        file_hash VARCHAR(64),
        UNIQUE(category_name, locale)
    );
    CREATE TABLE IF NOT EXISTS metadata.mapping_values (
        id SERIAL PRIMARY KEY,
        catalog_id INTEGER REFERENCES metadata.mapping_catalog(id) ON DELETE CASCADE,
        fake_value VARCHAR(255) NOT NULL
    );
    CREATE TABLE IF NOT EXISTS metadata.sql_audit_logs (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        username VARCHAR(255) NOT NULL DEFAULT 'anonymous_user',
        session_id VARCHAR(255) NOT NULL,
        query_type VARCHAR(32) NOT NULL,
        target_database VARCHAR(255) NOT NULL,
        sql_text TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_sql_audit_logs_session_ts
        ON metadata.sql_audit_logs (session_id, timestamp DESC);
    """

    category_map = {
        'first_names': 'first_name',
        'last_names': 'last_name',
        'cities': 'city',
        'companies': 'company_name'
    }

    try:
        with db.engine.connect() as conn:
            conn.execute(text(create_schema_sql))
            conn.execute(text(create_tables_sql))
            conn.commit()
            init_logs.append("🔍 Database structure (v2 - Locale Support) verified.")

            if not os.path.exists(seed_path):
                os.makedirs(seed_path)
                return ["📁 Seed path created. Please add CSVs."]

            # PETLJA KROZ CSV FAJLOVE
            for filename in os.listdir(seed_path):
                if filename.endswith(".csv"):
                    filepath = os.path.join(seed_path, filename)
                    current_hash = get_file_hash(filepath)

                    # 1. Detekcija locale-a iz naziva fajla (npr. cities_de.csv -> de)
                    locale = 'de' # default
                    if '_us' in filename: locale = 'us'
                    elif '_de' in filename: locale = 'de'

                    # 2. Mapiranje kategorije
                    file_prefix = filename.split('_de')[0].split('_us')[0].replace('.csv', '')
                    category = category_map.get(file_prefix)

                    if category:
                        # 3. Osiguraj da kategorija+locale postoji u katalogu
                        conn.execute(
                            text("""
                                INSERT INTO metadata.mapping_catalog (category_name, locale)
                                VALUES (:n, :l) ON CONFLICT DO NOTHING
                            """),
                            {"n": category, "l": locale}
                        )
                        conn.commit()

                        # 4. Uzmi hash i ID iz baze
                        res = conn.execute(
                            text("SELECT file_hash, id FROM metadata.mapping_catalog WHERE category_name = :n AND locale = :l"),
                            {"n": category, "l": locale}
                        )
                        row = res.fetchone()
                        db_hash, cat_id = row[0], row[1]

                        if db_hash == current_hash:
                            init_logs.append(f"⏩ Skipping {filename} ({locale}) - No changes.")
                            continue

                        init_logs.append(f"🔄 Re-seeding {filename} [Locale: {locale}]...")

                        # 5. Refresh vrednosti
                        conn.execute(text("DELETE FROM metadata.mapping_values WHERE catalog_id = :cid"), {"cid": cat_id})

                        df_seed = pd.read_csv(filepath)
                        values = df_seed.iloc[:, 0].dropna().unique().tolist()

                        for val in values:
                            conn.execute(
                                text("INSERT INTO metadata.mapping_values (catalog_id, fake_value) VALUES (:cid, :val)"),
                                {"cid": cat_id, "val": val}
                            )

                        conn.execute(
                            text("UPDATE metadata.mapping_catalog SET file_hash = :h WHERE id = :cid"),
                            {"h": current_hash, "cid": cat_id}
                        )
                        conn.commit()

            init_logs.append("✅ System initialization complete!")
            return init_logs

    except Exception as e:
        return [f"❌ Init Error: {e}"]
if __name__ == "__main__":
    initialize_metadata()