# -*- coding: utf-8 -*-
import pandas as pd
import logging
import os
from sqlalchemy import text
from src.database.db_manager import DBManager
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
    from sqlalchemy import text # Osiguraj da je text importovan

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
    create_tables_sql = """
    CREATE TABLE IF NOT EXISTS metadata.mapping_catalog (
        id SERIAL PRIMARY KEY,
        category_name VARCHAR(50) UNIQUE NOT NULL,
        description TEXT,
        file_hash VARCHAR(64)
    );
    CREATE TABLE IF NOT EXISTS metadata.mapping_values (
        id SERIAL PRIMARY KEY,
        catalog_id INTEGER REFERENCES metadata.mapping_catalog(id) ON DELETE CASCADE,
        fake_value VARCHAR(255) NOT NULL
    );
    """

    category_map = {
        'first_names': 'first_name',
        'last_names': 'last_name',
        'cities': 'city',
        'companies': 'company_name'
    }

    try:
        with db.engine.connect() as conn:
            # 1. Provera strukture
            logger.info("Checking database structure...")
            conn.execute(text(create_schema_sql))
            conn.execute(text(create_tables_sql))
            conn.commit()
            init_logs.append("🔍 Database structure verified.")

            # 2. Migracija kolone (za svaki slučaj)
            try:
                conn.execute(text("ALTER TABLE metadata.mapping_catalog ADD COLUMN IF NOT EXISTS file_hash VARCHAR(64);"))
                conn.commit()
            except Exception as e:
                logger.warning(f"Note: Column migration check: {e}")

            # 3. Inicijalizacija kategorija
            for cat_name in category_map.values():
                conn.execute(
                    text("INSERT INTO metadata.mapping_catalog (category_name) VALUES (:n) ON CONFLICT DO NOTHING"),
                    {"n": cat_name}
                )
            conn.commit()

            if not os.path.exists(seed_path):
                os.makedirs(seed_path)
                init_logs.append("📁 Seed folder created. Please add CSV files.")
                return init_logs

            # 4. Petlja kroz fajlove (SADA ISPRAVNA)
            for filename in os.listdir(seed_path):
                if filename.endswith(".csv"):
                    filepath = os.path.join(seed_path, filename)
                    current_hash = get_file_hash(filepath)

                    # Čišćenje naziva fajla za mapiranje (npr. first_names_de.csv -> first_names)
                    file_prefix = filename.split('_de')[0].split('_us')[0].replace('.csv', '')
                    category = category_map.get(file_prefix)

                    if category:
                        res = conn.execute(
                            text("SELECT file_hash, id FROM metadata.mapping_catalog WHERE category_name = :n"),
                            {"n": category}
                        )
                        row = res.fetchone()
                        db_hash, cat_id = row[0], row[1]

                        if db_hash == current_hash:
                            msg = f"⏩ Skipping {filename} - No changes."
                            logger.info(msg)
                            init_logs.append(msg)
                            continue # IDE NA SLEDEĆI FAJL

                        # Ako je hash drugačiji, radi re-seed
                        msg = f"🔄 Change detected in {filename}. Re-seeding '{category}'..."
                        logger.info(msg)
                        init_logs.append(msg)

                        # Brisanje i ponovni unos
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

            # 5. KRAJ (Van petlje!)
            init_logs.append("✅ System initialization complete!")
            return init_logs

    except Exception as e:
        error_msg = f"❌ Initialization failed: {e}"
        logger.error(error_msg)
        return [error_msg]

if __name__ == "__main__":
    initialize_metadata()