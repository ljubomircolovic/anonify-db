# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import logging
from src.agents.privacy_agent import PrivacyAgent

logger = logging.getLogger(__name__)

class AnonymizationPlanner:
    def __init__(self, db_manager):
        self.db = db_manager
        self.agent = PrivacyAgent()

    def generate_suggestion_plan(self, schema, table_name, allow_sampling=True, sample_limit=5):
        """
        Glavna metoda: Uzima uzorak iz baze, pakuje podatke i traži preporuku od Azure AI servisa.
        Vraća tuple: (final_plan, audit_info)
        """
        try:
            st.info(f"🔍 Analiziram tabelu: **{schema}.{table_name}**...")

            metadata_package = []

            # 1. Uslovno dohvatanje uzorka (Sampling)
            if allow_sampling:
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
                    st.warning(f"Tabela {table_name} je prazna. AI koristi samo metapodatke (imena kolona).")
                    allow_sampling = False

            # 2. Fallback: Ako sampling nije dozvoljen ili je tabela prazna, šaljemo samo listu kolona
            if not allow_sampling:
                all_columns = self.db.get_columns(schema, table_name)
                metadata_package = [{"column": col, "sample": []} for col in all_columns]

            # 3. Priprema Audit informacija (Ovo ćemo čuvati u session_state)
            audit_info = {
                "target_table": f"{schema}.{table_name}",
                "sampling_enabled": allow_sampling,
                "rows_sent": sample_limit if allow_sampling else 0,
                "payload": metadata_package,
                "policy": "Azure OpenAI Enterprise (No Training)"
            }

            # --- PRIVREMENI PRIKAZ (opciono, možeš ostaviti radi potvrde pre poziva) ---
            with st.expander("📡 Trenutni paket podataka za Azure AI"):
                st.json(audit_info)

            # 4. Poziv Azure AI Agenta preko LangChain-a
            with st.spinner("Azure AI generiše strategiju anonimizacije..."):
                analysis = self.agent.analyze_metadata(metadata_package)

            if analysis and analysis.plan:
                # Pretvaramo Pydantic objekte u listu rečnika za lakši rad u UI
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
            logger.error(f"❌ Greška u Planner-u: {e}")
            st.error(f"Došlo je do greške prilikom analize: {e}")
            return None, None