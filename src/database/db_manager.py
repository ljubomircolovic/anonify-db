# -*- coding: utf-8 -*-
import os
import json
import hashlib
import random
import logging
import re
import secrets
from decimal import Decimal, InvalidOperation
from datetime import timedelta
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import make_url
from faker import Faker
from dotenv import load_dotenv

from src.db.queries import data_discovery as dd_queries
from src.db.queries import metadata_reads
from src.db.queries import schema_queries
from src.db.services.anonymization_engine import AnonymizationEngine
from src.db.services.batch_executor import BatchExecutor
from src.db.services.connection_factory import ConnectionFactory, create_pooled_engine, slugify_name
from src.db.services.ddl_manager import DdlManager
from src.db.services.plan_persistence import PlanPersistence

# Konfiguracija logovanja
logger = logging.getLogger(__name__)

# Učitavanje enviroment varijabli iz .env fajla
load_dotenv()

class DBManager:
    def __init__(self, db_url=None):
        # 1. Konfiguracija baze podataka
        self.source_db_url = db_url or os.getenv(
            "DATABASE_URL",
            "postgresql://user:password@db:5432/anonify_db"
        )
        self.target_db_url = self.source_db_url
        self.db_url = self.target_db_url
        self.metadata_schema = "_anon_metadata"
        self.runtime_salt = os.getenv("ANONIFY_SALT", "default_plan_salt")
        self._structural_sync_counters = {"indexes_recreated": 0, "fks_recreated": 0}

        # Source engine: reads schema/tables from the original database
        self.source_engine = create_pooled_engine(self.source_db_url)
        # Target engine: metadata + anonymized writes
        self.target_engine = create_pooled_engine(self.target_db_url)
        # Backward compatibility for modules still using db.engine directly
        self.engine = self.target_engine

        # 2. Inicijalizacija Fakera (strict global locale for deterministic shadow parity)
        self.fake = Faker("en_US")
        self._apply_runtime_seed()

        # 3. Inicijalizacija meta-tabela + servisi
        self._plans = PlanPersistence(self)
        self._anonymization = AnonymizationEngine(self)
        self._batch = BatchExecutor(self)
        self._conn = ConnectionFactory(self)
        self._ddl = DdlManager(self)
        self._plans.init_metadata_tables()

        logger.info("✅ [DB_MANAGER] DBManager successfully initialized with Connection Pool.")

    def _runtime_seed_int(self):
        seed_hex = hashlib.sha256(str(self.runtime_salt).encode("utf-8")).hexdigest()[:16]
        return int(seed_hex, 16)

    def _apply_runtime_seed(self):
        """
        Aligns random/Faker streams with ANONIFY_SALT so legacy and shadow paths
        produce deterministic equivalent outputs.
        """
        seed_value = self._runtime_seed_int()
        random.seed(seed_value)
        try:
            self.fake.seed_instance(seed_value)
        except Exception:
            pass

    def _compute_source_list_hash(self):
        """Compatibility wrapper; delegates to :class:`PlanPersistence`."""
        return self._plans.compute_source_list_hash()

    def ensure_plan_security_metadata(self, schema_name, table_name):
        """Compatibility wrapper; delegates to :class:`PlanPersistence`."""
        return self._plans.ensure_plan_security_metadata(schema_name, table_name)

    @staticmethod
    def _quote_ident(identifier):
        return schema_queries.quote_sql_identifier(identifier)

    def quote_identifier(self, identifier):
        """Public identifier quoting helper for SQL names."""
        return self._quote_ident(identifier)

    def _get_source_type_signatures(self, source_schema, table_name):
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
        with self.source_engine.connect() as source_conn:
            source_conn.execute(text(f"SET search_path TO {self.quote_identifier(source_schema)}, public;"))
            return source_conn.execute(
                type_signature_sql,
                {"schema_name": source_schema, "table_name": table_name}
            ).fetchall()

    def _build_target_db_name(self, plan_name):
        return self._conn.build_target_db_name(plan_name)

    @staticmethod
    def _normalize_target_db_name(name_value):
        return ConnectionFactory.normalize_target_db_name(name_value)

    def _build_database_url(self, database_name):
        return self._conn.build_database_url(database_name)

    def _build_postgres_admin_url(self):
        return self._conn.build_postgres_admin_url()

    def _database_exists(self, database_name):
        return self._conn.database_exists(database_name)

    def plan_exists(self, plan_name, custom_db_name=None, allow_non_anon_prefix=False):
        return self._conn.plan_exists(plan_name, custom_db_name, allow_non_anon_prefix)

    def _set_target_engine(self, db_url):
        self._conn.set_target_engine(db_url)

    def bootstrap_plan_database(self, plan_name, custom_db_name=None, allow_non_anon_prefix=False):
        return self._conn.bootstrap_plan_database(plan_name, custom_db_name, allow_non_anon_prefix)

    def connect_to_existing_plan_database(self, plan_db_name):
        return self._conn.connect_to_existing_plan_database(plan_db_name)

    def list_existing_plan_databases(self):
        return self._conn.list_existing_plan_databases()

    def get_all_schemas(self):
        return schema_queries.fetch_all_schemas(self.source_engine)

    def get_tables_in_schema(self, schema='public'):
        return schema_queries.fetch_tables_in_schema(self.source_engine, schema)

    def read_table(self, table_name, schema_name='public', where=None, limit=100, params=None):
        return schema_queries.read_table_to_dataframe(
            self.source_engine,
            table_name=table_name,
            schema_name=schema_name,
            where=where,
            limit=limit,
            params=params,
        )

    def _ensure_target_table_mirror(self, active_conn, source_schema, target_schema, table_name):
        """Compatibility wrapper for :class:`DdlManager`."""
        return self._ddl.ensure_target_table_mirror(active_conn, source_schema, target_schema, table_name)

    @staticmethod
    def _rewrite_schema_references(sql_def, source_schema, target_schema):
        return DdlManager.rewrite_schema_references(sql_def, source_schema, target_schema)

    def _constraint_exists(self, active_conn, schema_name, table_name, constraint_name):
        return self._ddl.constraint_exists(active_conn, schema_name, table_name, constraint_name)

    def _index_exists(self, active_conn, schema_name, index_name):
        return self._ddl.index_exists(active_conn, schema_name, index_name)

    def get_indexed_columns(self, schema_name, table_name):
        return self._ddl.get_indexed_columns(schema_name, table_name)

    def log_index_distribution_preflight(self, source_schema, ordered_tables):
        return self._ddl.log_index_distribution_preflight(source_schema, ordered_tables)

    def reset_structural_sync_counters(self):
        return self._ddl.reset_structural_sync_counters()

    def _sync_pk_unique_constraints(self, active_conn, source_schema, target_schema, table_name):
        return self._ddl.sync_pk_unique_constraints(active_conn, source_schema, target_schema, table_name)

    def _sync_non_constraint_indexes(self, active_conn, source_schema, target_schema, table_name):
        return self._ddl.sync_non_constraint_indexes(active_conn, source_schema, target_schema, table_name)

    def sync_foreign_keys_for_tables(self, source_schema, target_schema, ordered_tables):
        return self._ddl.sync_foreign_keys_for_tables(source_schema, target_schema, ordered_tables)

    def check_fk_integrity(self, source_schema, target_schema, ordered_tables):
        return self._ddl.check_fk_integrity(source_schema, target_schema, ordered_tables)

    def create_anonymized_table(self, source_schema, table_name, target_db, target_schema="anon"):
        return self._ddl.create_anonymized_table(source_schema, table_name, target_db, target_schema)

    @staticmethod
    def _coerce_decimal_for_sql(value):
        """Ensures NUMERIC/DECIMAL-compatible python values before INSERT."""
        return AnonymizationEngine.coerce_decimal_for_sql(value)

    def _cast_dataframe_to_table_types(self, df, active_conn, schema_name, table_name, preserve_native_columns=None):
        return self._anonymization.cast_dataframe_to_table_types(
            df, active_conn, schema_name, table_name, preserve_native_columns
        )

    def _infer_sql_type_from_series(self, series):
        return self._anonymization.infer_sql_type_from_series(series)

    def _ensure_target_table_from_dataframe(self, active_conn, target_schema, table_name, df):
        return self._anonymization.ensure_target_table_from_dataframe(
            active_conn, target_schema, table_name, df
        )

    def save_anonymized_table(
        self,
        df,
        table_name,
        target_schema='anon',
        conn=None,
        source_schema='public',
        preserve_native_columns=None,
        disable_constraints_mode=None,
    ):
        """
        Snima DataFrame u ciljnu tabelu. Ako je 'conn' prisutan, ostaje u istoj sesiji.
        """
        from sqlalchemy import text

        def _run_save(active_conn):
            use_session_replica = str(disable_constraints_mode or "").lower() == "session_replica"
            try:
                if use_session_replica:
                    active_conn.execute(text("SET session_replication_role = 'replica';"))
                self._ensure_target_table_mirror(active_conn, source_schema, target_schema, table_name)
            except Exception as mirror_err:
                logger.warning(
                    f"⚠️ [DB_MANAGER] Mirror DDL failed for {target_schema}.{table_name}: {mirror_err}. "
                    "Falling back to DataFrame-based auto-DDL."
                )
                self._ensure_target_table_from_dataframe(active_conn, target_schema, table_name, df)
            try:
                safe_df = self._cast_dataframe_to_table_types(
                    df, active_conn, target_schema, table_name, preserve_native_columns=preserve_native_columns
                )

                # Osiguravamo šemu ako ne postoji
                active_conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.quote_identifier(target_schema)}"))

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
                target_db_name = make_url(self.target_db_url).database or "target_db"
                logger.info(
                    f"✅ [DB_MANAGER] Inserted anonymized rows into {target_db_name}.{target_schema}.{table_name} "
                    f"from source schema {source_schema}"
                )
                return True
            finally:
                if use_session_replica:
                    active_conn.execute(text("SET session_replication_role = 'origin';"))

        try:
            if conn:
                # Deo Batch transakcije
                return _run_save(conn)
            else:
                # Samostalni upis
                with self.target_engine.connect() as standalone_conn:
                    with standalone_conn.begin():
                        return _run_save(standalone_conn)
        except Exception as e:
            logger.error(f"❌ [DB_MANAGER] Error saving to {target_schema}.{table_name}: {e}")
            return False

    def mask_value(self, val):
        return self._anonymization.mask_value(val)

    def get_global_mapping(self, col_name, orig_val, salt):
        return self._anonymization.get_global_mapping(col_name, orig_val, salt)

    def save_global_mapping(self, col_name, orig_val, anon_val, salt):
        return self._anonymization.save_global_mapping(col_name, orig_val, anon_val, salt)

    def get_mapping_value(self, original_value, category, locale, salt):
        return self._anonymization.get_mapping_value(original_value, category, locale, salt)

    def apply_anonymization_rules(self, df, table_plan, salt=None, consistency_seed_map=None):
        return self._anonymization.apply_anonymization_rules(df, table_plan, salt, consistency_seed_map)

    def _get_mapping_values_by_locale(self, category, locale):
        return self._anonymization.get_mapping_values_by_locale(category, locale)

    def _deterministic_map(self, original_value, mapping_list, salt):
        return self._anonymization.deterministic_map(original_value, mapping_list, salt)

    def _init_metadata_table(self):
        """Legacy hook; metadata DDL is owned by :class:`PlanPersistence`."""
        self._plans.init_metadata_tables()

    def get_columns(self, table_name, schema_name='public'):
        """Vra?a listu naziva kolona za datu tabelu koriste?i SQLAlchemy inspect."""
        return schema_queries.fetch_column_names(self.source_engine, table_name, schema_name)

    def get_ai_ready_metadata(self, table_name, schema='public', sample_size=5):
        inspector = inspect(self.source_engine)
        columns_info = inspector.get_columns(table_name, schema=schema)
        metadata_package = []
        for col in columns_info:
            col_name = col['name']
            query = text(f'SELECT "{col_name}" FROM "{schema}"."{table_name}" LIMIT 50')
            try:
                raw_sample = pd.read_sql(query, self.source_engine)[col_name].dropna().unique().tolist()[:sample_size]
                masked_sample = [self.mask_value(v) for v in raw_sample]
            except:
                masked_sample = []
            metadata_package.append({"column": col_name, "type": str(col['type']), "sample": masked_sample})
        return metadata_package

    def save_ai_plan(self, schema_name, table_name, plan_data, where_condition=""):
        return self._plans.save_ai_plan(schema_name, table_name, plan_data, where_condition)

    def get_saved_plan(self, schema_name, table_name):
        return self._plans.get_saved_plan(schema_name, table_name)

    def log_action(self, user, schema, table, score, salt, status="SUCCESS"):
        return self._plans.log_action(user, schema, table, score, salt, status)

    def log_unified_ai_scan(self, user, schema, tables, status="UNIFIED_AI_SCAN", score=0, salt="unified_batch", estimated_tokens=0):
        return self._plans.log_unified_ai_scan(user, schema, tables, status, score, salt, estimated_tokens)

    def get_audit_logs(self, limit=50):
        return self._plans.get_audit_logs(limit)

    def test_connection(self):
        """Proverava da li je baza dostupna i da li imamo osnovni pristup."""
        return metadata_reads.verify_source_connection(self.source_engine)

    def get_foreign_key_relations_postgres(self, schema_name='public'):
        """
        Dohvata Foreign Key relacije specifične za PostgreSQL.
        Vraća DataFrame sa: table_name, column_name, foreign_table_name, foreign_column_name.
        """
        return dd_queries.fetch_foreign_key_relations_postgres(self.source_engine, schema_name)

    def get_execution_order(self, selected_tables, schema_name='public'):
        """
        Sortira izabrane tabele po hijerarhiji (PK pre FK).
        Koristi topološko sortiranje na osnovu relacija.
        """
        relations = self.get_foreign_key_relations_postgres(schema_name)
        return dd_queries.compute_execution_order(selected_tables, relations)

    def load_forced_mappings_from_db(self, schema_name='ecommerce'):
        return self._anonymization.load_forced_mappings_from_db(schema_name)

    def analyze_table_structure(self, df_sample, agent, schema_name='ecommerce'):
        return self._anonymization.analyze_table_structure(df_sample, agent, schema_name)

    def prepare_anonymization_target(self, source_schema, target_schema, ordered_tables):
        return self._ddl.prepare_anonymization_target(source_schema, target_schema, ordered_tables)

    def restore_foreign_keys(self, source_schema, target_schema, tables):
        return self._ddl.restore_foreign_keys(source_schema, target_schema, tables)

    def drop_target_schema(self, target_schema):
        return self._ddl.drop_target_schema(target_schema)

    def get_primary_keys(self, schema, table):
        """Vraća listu kolona koje su Primary Key za datu tabelu koristeći DDL meta-podatke."""
        return dd_queries.fetch_primary_key_column_names(self.source_engine, schema, table)

    def table_exists(self, table_name, schema_name):
        """Proverava da li tabela postoji u specifičnoj šemi."""
        return dd_queries.table_exists(self.source_engine, table_name, schema_name)

    def get_row_count(self, table_name, schema_name):
        """Vraća broj redova u tabeli."""
        return dd_queries.fetch_row_count(self.source_engine, table_name, schema_name)

    def get_column_details(self, table_name, schema_name):
        """
        Vraća proširene meta-podatke o kolonama (tip i nullability) za strogu validaciju.
        """
        return dd_queries.fetch_column_details(self.source_engine, table_name, schema_name)

    def get_all_foreign_keys(self, schema_name):
        """
        Izvlači sve Foreign Key relacije u šemi kako bi se osigurao referencijalni integritet.
        Vraća listu torki: (source_table, source_column, target_table, target_column)
        """
        return dd_queries.fetch_all_foreign_keys_tuples(self.source_engine, schema_name)

    def get_all_saved_plans(self, schema_name):
        return self._plans.get_all_saved_plans(schema_name)

    def align_db_types(self, target_schema, table_name, plan, conn=None):
        return self._ddl.align_db_types(target_schema, table_name, plan, conn)

    def drop_all_fks_for_table(self, conn, schema, table):
        return self._ddl.drop_all_fks_for_table(conn, schema, table)

    def rehook_foreign_keys(self, conn, commands):
        return self._ddl.rehook_foreign_keys(conn, commands)

    def truncate_anon_tables(self, target_schema, ordered_tables, clear_mode="truncate_cascade"):
        return self._ddl.truncate_anon_tables(target_schema, ordered_tables, clear_mode)

    def set_fk_constraints_temporarily_disabled(self, target_schema, ordered_tables, disabled=True):
        return self._ddl.set_fk_constraints_temporarily_disabled(target_schema, ordered_tables, disabled)

    def prepare_subset_metadata(self, conn):
        return self._batch.prepare_subset_metadata(conn)

    def register_keys(self, conn, column_name, values):
        return self._batch.register_keys(conn, column_name, values)

    def execute_anonymization_batch(self, selected_schema, target_schema, full_plan, ordered_tables):
        return self._batch.execute_anonymization_batch(selected_schema, target_schema, full_plan, ordered_tables)

    def get_table_sample(self, schema, table, limit=5):
        """
        Dohvata uzorak podataka za AI analizu.
        Sve pretvara u string da bi izbegli probleme sa specifičnim tipovima u JSON-u.
        """
        return dd_queries.fetch_table_sample_as_str_records(
            self.source_engine, schema, table, limit=int(limit)
        )

    def get_unified_ai_scan_payload(self, schema, tables, sample_limit=5):
        """
        Builds table-scoped metadata + representative row samples for a unified AI request.
        Always caps row sampling to 5 rows per table for scalability.
        """
        payload = []
        strict_limit = max(0, min(int(sample_limit or 0), 5))

        for table_name in tables or []:
            column_details = self.get_column_details(table_name, schema) or {}
            sample_rows = self.get_table_sample(schema, table_name, limit=strict_limit) if strict_limit > 0 else []
            payload.append({
                "schema": schema,
                "table": table_name,
                "columns": [
                    {"column": col_name, "type": details.get("type", "")}
                    for col_name, details in column_details.items()
                ],
                "sample_rows": sample_rows[:5],
            })

        return payload

    def get_tables(self, schema_name='public'):
        """
        Vraća listu svih tabela u zadatoj šemi, isključujući interne AnonifyDB tabele.
        """
        return schema_queries.fetch_tables_excluding_internal(self.source_engine, schema_name)

    def get_source_schema_catalog(self, schema_name='public'):
        """
        Returns source schema tables with PostgreSQL table comments.
        """
        return schema_queries.fetch_source_schema_catalog(self.source_engine, schema_name)