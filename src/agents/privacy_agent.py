# -*- coding: utf-8 -*-
import os
import json
import re
from typing import List, Dict, Any
from pydantic import BaseModel, Field
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.messages import SystemMessage, HumanMessage

# 1. Osnovna struktura za jednu kolonu
class AnonymizationPlan(BaseModel):
    column: str = Field(description="Name of the database column")
    is_pii: bool = Field(default=False, description="True if sensitive")
    strategy: str = Field(description="Strategy: 'hash', 'mask', 'faker_name', 'faker_email', 'faker_phone', or 'keep'")
    reason: str = Field(description="Brief explanation in English")

# 2. Omotač koji sadrži listu planova (Ovo je nedostajalo!)
class PrivacyAnalysis(BaseModel):
    plan: List[AnonymizationPlan]

# 3. Glavna klasa Agent
class PrivacyAgent:
    def __init__(self):
        # Inicijalizacija Azure klijenta
        self.llm = AzureChatOpenAI(
            azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
            api_key=os.getenv("AZURE_OPENAI_KEY"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_version=os.getenv("AZURE_OPENAI_VERSION"),
            temperature=0
        )
        self.parser = PydanticOutputParser(pydantic_object=PrivacyAnalysis)
        self.default_system_policy = (
            "You are a Senior Data Privacy Expert (GDPR & HIPAA Compliance). "
            "The sample data provided is for contextual analysis only. "
            "It MUST NOT be used for model training or stored beyond this session. "
            "Return ONLY a raw JSON object. Do not include markdown code blocks, preamble, "
            "or any text outside the JSON structure."
        )

    def analyze_metadata(self, metadata_package: List[Dict]) -> PrivacyAnalysis:
        """
        Prima listu rečnika: [{'column': 'email', 'sample': ['a@b.com', ...]}, ...]
        Šalje uzorak podataka Azure AI-ju za preciznu analizu.
        """
        template = """
        You are a Senior Data Privacy Expert (GDPR & HIPAA Compliance).

        ENTERPRISE PRIVACY POLICY: 
        The sample data provided is for contextual analysis only. 
        It MUST NOT be used for model training or stored beyond this session.

        TASK:
        Analyze the database columns and their actual sample values to identify PII (Personally Identifiable Information).
        Use the provided sample values to confirm the context, as column names can be misleading.

        COLUMNS AND SAMPLES:
        {metadata}

        {format_instructions}

        STRATEGY GUIDE:
        - 'faker_name': Use for full names, first names, or last names.
        - 'faker_email': Use for any email addresses.
        - 'faker_phone': Use for phone numbers (recognize formats from samples).
        - 'hash': Use for unique IDs (UUIDs, UserIDs) that must remain consistent.
        - 'mask': Use for sensitive strings like addresses or descriptions.
        - 'keep': Use for safe, non-identifiable data (counts, timestamps, amounts, statuses).

        CONSISTENCY MAPPING (MANDATORY):
        - If a column is a Foreign Key, it must use the same anonymization function/seed logic as the corresponding Primary Key.
        - Example: if customers.id is transformed to X-123, then orders.customer_id referencing that key must transform to X-123 as well.
        - Never propose FK strategies that would break join compatibility with referenced PK values.
        - Keep anonymized outputs type-compatible with indexed columns.

        IMPORTANT: Return ONLY valid JSON.
        """

        prompt = ChatPromptTemplate.from_template(template)
        format_instructions = self.parser.get_format_instructions()
        
        # LangChain Chain
        chain = prompt | self.llm | self.parser
        
        try:
            # Slanje paketa sa uzorcima podataka ka Azure AI
            return chain.invoke({
                "metadata": metadata_package,
                "format_instructions": format_instructions
            })
        except Exception as e:
            print(f"❌ Azure Analysis Error: {e}")
            return None

    @staticmethod
    def _safe_json_dump(payload: Any) -> str:
        try:
            return json.dumps(payload, ensure_ascii=False, default=str)
        except Exception:
            return str(payload)

    @staticmethod
    def extract_json(text: str) -> str:
        """
        Extracts JSON object content from noisy model output by taking
        substring between first '{' and last '}'.
        """
        if not text:
            return ""
        # Remove common markdown fences first.
        cleaned = re.sub(r"```(?:json)?", "", str(text), flags=re.IGNORECASE).strip()
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1 or end < start:
            return ""
        return cleaned[start:end + 1]

    def build_unified_payload(self, schema: str, table_packages: List[Dict[str, Any]]) -> str:
        """
        Builds XML-tagged payload with clear table boundaries.
        Each table package is expected to have:
        {
            "table_name": str,
            "columns": [{"name": str, "type": str}, ...],
            "sample_rows": [dict, ...]   # max 5 rows
        }
        """
        parts = [f'<dataset schema="{schema}">']
        for table_pkg in table_packages:
            table_name = str(table_pkg.get("table_name", "unknown"))
            cols = table_pkg.get("columns", [])
            rows = table_pkg.get("sample_rows", [])
            parts.append(f'  <table name="{table_name}">')
            parts.append("    <columns>")
            for col in cols:
                col_name = str(col.get("name", ""))
                col_type = str(col.get("type", ""))
                parts.append(f'      <column name="{col_name}" type="{col_type}" />')
            parts.append("    </columns>")
            parts.append("    <sample_rows_json>")
            parts.append(self._safe_json_dump(rows))
            parts.append("    </sample_rows_json>")
            parts.append("  </table>")
        parts.append("</dataset>")
        return "\n".join(parts)

    def estimate_token_count(self, payload_text: str) -> int:
        # Rough estimate ~4 chars/token.
        return max(1, len(payload_text) // 4)

    def build_unified_prompt(self, schema: str, table_packages: List[Dict[str, Any]]) -> str:
        unified_payload = self.build_unified_payload(schema, table_packages)
        prompt = (
            "Analyze all provided database tables and identify PII per column.\n"
            "Preserve relational boundaries and infer potential FK links from names/samples.\n\n"
            "CONSISTENCY MAPPING (MANDATORY):\n"
            "- Foreign Keys must use the same anonymization function/seed behavior as their referenced Primary Keys.\n"
            "- If customers.id maps to X-123, orders.customer_id must map to X-123 for matching source values.\n"
            "- Ensure anonymized values remain type-compatible with likely indexed columns.\n\n"
            "Return ONLY a raw JSON object. Do not include markdown, prose, or any wrapper text.\n"
            "Use exactly this JSON schema:\n"
            "{\n"
            '  "tables": {\n'
            '    "users": {\n'
            '      "columns": {\n'
            '        "email": {"is_pii": true, "function": "faker_email", "reason": "..."},\n'
            '        "id": {"is_pii": false, "function": "keep", "reason": "..."}\n'
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n"
        )
        return f"{prompt}\nUNIFIED_TABLE_PAYLOAD:\n{unified_payload}"

    def analyze_unified_tables(self, schema: str, table_packages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Sends all selected tables in a single request and returns:
        {
            "table_results": { "table_name": [plan_item_dict, ...], ... },
            "warnings": ["...", ...],
            "raw": "<raw model output>"
        }
        """
        full_prompt = self.build_unified_prompt(schema, table_packages)

        response = self.llm.invoke([
            SystemMessage(content=self.default_system_policy),
            HumanMessage(content=full_prompt)
        ])
        raw_text = getattr(response, "content", "") or ""

        parsed: Dict[str, Any] = {"tables": {}}
        warnings: List[str] = []
        json_text = self.extract_json(raw_text)
        try:
            parsed = json.loads(json_text or "")
        except Exception:
            warnings.append("Unified AI response is not valid JSON.")
            return {"table_results": {}, "warnings": warnings, "raw": raw_text}

        table_results: Dict[str, List[Dict[str, Any]]] = {}
        tables_section = parsed.get("tables", {})

        # New strict schema: {"tables": {"table_name": {"columns": {...}}}}
        if isinstance(tables_section, dict):
            for t_name, t_block in tables_section.items():
                try:
                    if not isinstance(t_block, dict):
                        warnings.append(f"Malformed table block skipped: {self._safe_json_dump(t_block)}")
                        continue
                    columns_block = t_block.get("columns", {})
                    if not isinstance(columns_block, dict):
                        warnings.append(f"Missing/invalid 'columns' for table: {t_name}")
                        continue
                    normalized_plan = []
                    for col_name, col_meta in columns_block.items():
                        if not isinstance(col_meta, dict):
                            continue
                        normalized_plan.append({
                            "column": str(col_name),
                            "is_pii": bool(col_meta.get("is_pii", False)),
                            "strategy": str(col_meta.get("function", "keep")).lower().strip(),
                            "reason": str(col_meta.get("reason", "")),
                        })
                    table_results[str(t_name)] = normalized_plan
                except Exception:
                    warnings.append(f"Malformed table entry could not be parsed: {self._safe_json_dump(t_block)}")

        # Backward compatibility fallback: list-style format.
        elif isinstance(tables_section, list):
            for table_item in tables_section:
                try:
                    t_name = str(table_item.get("table_name", "")).strip()
                    t_plan = table_item.get("plan", [])
                    if not t_name or not isinstance(t_plan, list):
                        warnings.append(f"Malformed table block skipped: {self._safe_json_dump(table_item)}")
                        continue
                    normalized_plan = []
                    for p in t_plan:
                        if not isinstance(p, dict):
                            continue
                        normalized_plan.append({
                            "column": str(p.get("column", "")),
                            "is_pii": bool(p.get("is_pii", False)),
                            "strategy": str(p.get("strategy", "keep")).lower().strip(),
                            "reason": str(p.get("reason", "")),
                        })
                    table_results[t_name] = normalized_plan
                except Exception:
                    warnings.append(f"Malformed table entry could not be parsed: {self._safe_json_dump(table_item)}")
        return {"table_results": table_results, "warnings": warnings, "raw": raw_text}