# AnonifyDB — Architecture Map

This document describes the modular layout of the application after the refactor: where code lives, what each major area is responsible for, and how data flows from the UI to PostgreSQL.

---

## Dependency flow (textual)

```
Streamlit UI (src/ui/**)
    │
    ├─► AppState & workflow (src/logic/**) — session façade, URL/sync helpers, plan↔source binding
    │
    ├─► DBManager (src/database/db_manager.py) — orchestrates engines, delegates to services
    │
    ├─► Services (src/db/services/**) — writes, DDL, anonymization, batches, connection bootstrap
    │
    ├─► Queries (src/db/queries/**) — read-only SQL helpers (schemas, FKs, samples, audit reads)
    │
    └─► PostgreSQL (source + target engines)
```

**Rule of thumb:** UI talks to `DBManager` and `AppState`; `DBManager` composes **services** for mutations and **queries** for reads; services may call queries or engines directly where appropriate.

---

## Repository tree (high level)

```
anonify-db/
├── app_ui.py                 # Streamlit entry: layout, session init, tab routing
├── init_db.py                # Metadata / bootstrap helpers for local setup
├── ARCHITECTURE_MAP.md       # This file
├── config/                   # YAML / static configuration
├── src/
│   ├── main.py               # Alternate or CLI-style entry (if used)
│   ├── database/             # DBManager façade + legacy DB utilities
│   ├── db/                   # Modular DB layer (queries + services)
│   ├── logic/                # AppState, workflow, source URL logic, naming
│   ├── ui/                   # Streamlit tabs, sidebar, planner, source package
│   ├── engine/               # Anonymization / file engines
│   ├── agents/               # Azure / privacy agent integration
│   ├── adapters/legacy/      # Adapter wrapping DBManager for ports
│   └── core/                 # Domain entities, strategies, ports, guards
└── tests/                    # Smoke / integration tests
```

---

## `src/database/` — Legacy façade & DB utilities

| Module | Responsibility | Key functions / types |
|--------|----------------|----------------------|
| `db_manager.py` | Central façade: engines, session salt, delegates to `src.db.services` and queries. | `DBManager`, `save_anonymized_table`, connection/plan bootstrap wrappers |
| `db_utils.py` | Shared DB helper routines used by database package. | Utility functions for DB I/O |
| `exporter.py` | Export flows from the database layer. | Export helpers |
| `generator.py` | Synthetic or seed data generation tied to DB workflows. | Generator helpers |

---

## `src/db/queries/` — Read-only SQL

| Module | Responsibility | Key functions |
|--------|----------------|---------------|
| `schema_queries.py` | Schemas, table lists, quoted identifiers, read-only table access. | `fetch_tables_in_schema`, `quote_sql_identifier`, read helpers |
| `data_discovery.py` | FK graphs, PK lists, execution order, row samples. | `fetch_all_foreign_keys_tuples`, `compute_execution_order`, sample helpers |
| `metadata_reads.py` | Connectivity checks and audit log reads. | `verify_source_connection`, audit fetch helpers |

---

## `src/db/services/` — Write / transform / DDL

| Module | Responsibility | Key functions / types |
|--------|----------------|----------------------|
| `anonymization_engine.py` | Column masking, casts, deterministic maps for preview and batch. | `apply_anonymization_rules`, `cast_dataframe_to_table_types` |
| `plan_persistence.py` | `_anon_metadata` plans, audit rows, security metadata. | `save_ai_plan`, `get_saved_plan`, `init_metadata_tables` |
| `batch_executor.py` | Ordered batch anonymization run with FK/truncate coordination. | `execute_anonymization_batch` |
| `connection_factory.py` | Plan DB naming, URLs, engine swap, list/bootstrap plan databases. | `ConnectionFactory`, `create_pooled_engine`, `slugify_name` |
| `ddl_manager.py` | Structural mirror: types, PK/UQ, indexes, FK sync entrypoints. | `DdlManager.ensure_target_table_mirror`, `sync_foreign_keys_for_tables` |
| `ddl_extensions.py` | CTAS/mirror create, skeleton prep, FK drop/rehook, truncate, triggers. | `DdlExtensions` (composed by `DdlManager`) |

---

## `src/logic/` — Application logic (no widgets)

| Module | Responsibility | Key functions / types |
|--------|----------------|----------------------|
| `app_state.py` | Typed access to `session_state` for cross-tab keys. | `AppState`, `get_session_store`, schema/source getters |
| `workflow.py` | Plan ↔ source binding and readiness messaging. | `bind_plan_metadata_to_source`, `maybe_auto_bind_plan_to_source` |
| `source_connection.py` | Compose/sync DB URLs from session and `.env`. | `compose_postgresql_source_url`, `resolve_postgresql_source_url`, `sync_db_config_from_session` |
| `source_constants.py` | Domain and source-type enumerations. | `DOMAIN_OPTIONS`, `SOURCE_TYPES` |
| `naming.py` | Safe name normalization for plans/databases. | `normalize_name_fragment` |

---

## `src/ui/source/` — Source tab (modular)

| Module | Responsibility | Key functions |
|--------|----------------|---------------|
| `source_tab.py` | Orchestrates control bar + active panel + log. | `render_source_tab` |
| `source_utils.py` | Constants, event log, `.env` persistence, session seeding, CSS inject. | `init_source_state`, `log_source_event`, `persist_source_confirmation_to_env` |
| `source_control_bar.py` | Status pill, DB action row (Test / Initialize / Change / Confirm), domain. | `render_source_control_bar_and_domain`, `render_master_source_selector` |
| `source_database_panel.py` | Connection form, schema/tables, technical metadata, preview. | `render_db_source_section`, `render_db_engine_subselector` |
| `source_file_panel.py` | File parse, column map, filtered preview. | `render_file_source_section`, `load_file_dataframe` |
| `source_api_log_panel.py` | HTTP API probe + response monitor + source event table. | `render_api_source_section`, `render_source_log_section` |

**Import path:** use `from src.ui.source.source_tab import render_source_tab` (the old `src.ui.source_tab` shim was removed).

---

## `src/ui/tabs/planner/` — Planner tab package

| Module | Responsibility | Key functions |
|--------|----------------|---------------|
| `planner_table_config.py` | Main Plan tab: scan triggers, roadmap, expanders, delegates actions/editor. | `render_planner_tab` |
| `planner_actions.py` | Global actions, review, execute/finalize block. | `render_planner_actions_block` |
| `planner_validation.py` | Session plan helpers, unsaved detection, FK violation collection. | `_get_persisted_plan_for_table`, `_collect_sensitive_keep_violations` |
| `planner_save_pipeline.py` | Strict validation + save + navigation after save. | `save_and_move_to_next` |
| `table_render_utils.py` | Per-table editor and filter/preview expander. | `render_planner_table_plan_editor`, `render_planner_filter_preview_expander` |
| `planner_header.py` | Target/plan context banners. | `render_target_context_banner` |
| `planner_components.py` | Small planner widgets (status chain, audit, action stack). | `render_planner_action_buttons`, `render_ai_audit_log` |
| `planner_logic.py` | Plan row normalization / cleaning for persistence. | `get_clean_plan` |
| `planner_navigation.py` | Table order helpers for planner navigation. | `get_next_table_in_chain`, `handle_navigation_history` |
| `planner_secrets.py` | Plan salt / consistency seed resolution for execution. | `resolve_plan_salt`, `build_consistency_seed_maps`, `resolve_active_plan_seed` |

---

## `src/ui/` — Other UI

| Module | Responsibility | Key functions |
|--------|----------------|---------------|
| `sidebar.py` | Navigation, connection dashboard, session initialize triggers. | `render_sidebar`, … |
| `tabs_content.py` | Tab strip helpers / comparison routing. | `render_comparison_tab` (and related) |
| `tabs/execute_tab.py` | Execute confirmation dialogs and pipeline run. | `render_execute_confirmation_dialog`, `run_all_anonymization` |
| `tabs/comparison_tab.py` | Source vs anon comparison UI. | `render_comparison_tab` |
| `planner.py` | Parallel AI scan + `AnonymizationPlanner` (non-widget core for scans). | `analyze_tables_parallel`, `AnonymizationPlanner` |
| `main_menu.py`, `selection_tab.py`, `auth.py`, … | Shell, selection, login. | Various `render_*` |

---

## `src/core/` & `src/adapters/` — Hex-style domain (incremental)

| Area | Responsibility |
|------|------------------|
| `core/domain/entities` | Plan and table value objects. |
| `core/domain/strategies` | Mask/hash/faker strategy implementations. |
| `core/domain/services` | Plan validation, unsaved guard, strategy-driven anonymization service. |
| `core/ports` | Interfaces for DB/plan repositories and engines. |
| `adapters/legacy/db_manager_adapter.py` | Bridges `DBManager` to port interfaces. |

---

## `src/engine/` & `src/agents/`

| Module | Responsibility |
|--------|------------------|
| `engine/anonify_engine.py` | Higher-level anonymization orchestration. |
| `engine/file_engine.py` | File-backed processing path. |
| `agents/privacy_agent.py` | LLM-backed metadata / plan suggestions. |

---

## Tests & tooling

| Path | Responsibility |
|------|----------------|
| `tests/anonify_test.py` | Automated checks against the stack. |
| `setup_test_db.py`, `seed_mappings.py` | Local DB prep / seed scripts. |

---

## Cleanup notes (this pass)

- **Removed** `src/ui/source_tab.py` shim; **`app_ui.py`** imports `render_source_tab` from `src.ui.source.source_tab`.
- **Moved** into `src/ui/tabs/planner/`: `planner_components.py`, `planner_logic.py`, `planner_navigation.py`, `planner_secrets.py` (from `src/ui/` or `src/ui/tabs/`), with imports updated across `src/` and `app_ui.py`.
- **`src/database/db_manager.py`** remains the active `DBManager` implementation (not a backup); do not delete.
- Package `__init__.py` files under `src/db`, `src/logic`, `src/ui/source`, and `src/ui/tabs/planner` retain intentional re-exports for stable public import paths.

---

*Generated to reflect the modular layout. Update this file when you add new top-level packages or move boundaries between UI, logic, and data layers.*
