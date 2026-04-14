# -*- coding: utf-8 -*-
import pandas as pd
import logging
from src.agents.privacy_agent import PrivacyAgent
import concurrent.futures
import time

# Inicijalizacija loggera
logger = logging.getLogger(__name__)

class AnonymizationPlanner:
    def __init__(self, db_manager):
        self.db = db_manager
        self.agent = PrivacyAgent()

    def generate_suggestion_plan(self, schema, table_name, allow_sampling=True, sample_limit=5):
        """
        Glavna metoda: Uzima uzorak iz baze i traži preporuku od Azure AI.
        Ova metoda je sada THREAD-SAFE (uklonjene st.* komande).
        """
        try:
            logger.info(f"🔍 Analiziram tabelu: {schema}.{table_name}...")

            metadata_package = []

            # 1. Uslovno dohvatanje uzorka (Sampling)
            if allow_sampling:
                # Dohvatamo podatke direktno preko db_managera
                sample_data = self.db.get_table_sample(schema, table_name, limit=sample_limit)

                if sample_data:
                    # Pakovanje metapodataka sa stvarnim uzorcima vrednosti
                    all_columns = list(sample_data[0].keys())
                    for col in all_columns:
                        col_samples = [row.get(col) for row in sample_data]
                        metadata_package.append({
                            "column": col,
                            "sample": col_samples
                        })
                else:
                    logger.warning(f"Tabela {table_name} je prazna. AI koristi samo metapodatke.")
                    allow_sampling = False

            # 2. Fallback: Ako sampling nije dozvoljen ili je tabela prazna
            if not allow_sampling:
                all_columns = self.db.get_columns(schema, table_name)
                metadata_package = [{"column": col, "sample": []} for col in all_columns]

            # 3. Priprema Audit informacija
            audit_info = {
                "target_table": f"{schema}.{table_name}",
                "sampling_enabled": allow_sampling,
                "rows_sent": sample_limit if allow_sampling else 0,
                "payload": metadata_package,
                "policy": "Azure OpenAI Enterprise (No Training)"
            }

            # 4. Poziv Azure AI Agenta preko LangChain-a
            # Spinner se sada kontroliše iz UI fajla (tabs_content.py)
            analysis = self.agent.analyze_metadata(metadata_package)

            if analysis and analysis.plan:
                # Pretvaramo Pydantic objekte u listu rečnika za stabilan prenos između threadova
                final_plan = []
                for p in analysis.plan:
                    final_plan.append({
                        "column": p.column,
                        "is_pii": p.is_pii,
                        "strategy": p.strategy,
                        "reason": p.reason
                    })

                logger.info(f"✅ Uspešno generisan plan za {table_name}")
                return final_plan, audit_info

            return None, None

        except Exception as e:
            logger.error(f"❌ Greška u Planner-u za {table_name}: {e}")
            return None, None

def analyze_tables_parallel(db_manager, tables, schema="public", allow_sampling=True, sample_limit=5):
    """
    Pokreće AI analizu za više tabela istovremeno uz throttling.
    Sada potpuno bezbedno za korišćenje u Streamlit aplikaciji.
    """
    planner = AnonymizationPlanner(db_manager)
    results = {}
    future_to_table = {}

    # Koristimo ThreadPoolExecutor za I/O bound zadatke (API pozivi)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:

        for table in tables:
            # Šaljemo zadatak u thread pool
            future = executor.submit(
                planner.generate_suggestion_plan,
                schema, table, allow_sampling, sample_limit
            )
            future_to_table[future] = table

            # Throttling: Sprečavamo 429 grešku na Azure-u
            time.sleep(0.5)
            logger.info(f"📡 Zadatak poslat za tabelu: {table}, čekam 0.5s pre sledećeg...")

        # Prikupljanje rezultata
        for future in concurrent.futures.as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                plan, audit = future.result()
                if plan:
                    results[table_name] = {"plan": plan, "audit": audit}
                    logger.info(f"✅ Paralelna analiza završena za: {table_name}")
                else:
                    logger.warning(f"⚠️ Plan za {table_name} je vraćen kao None.")
            except Exception as e:
                logger.error(f"❌ Greška u paralelizaciji za {table_name}: {e}")
                results[table_name] = None

    return results