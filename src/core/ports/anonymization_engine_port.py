from abc import ABC, abstractmethod
from typing import Any


class AnonymizationEnginePort(ABC):
    @abstractmethod
    def apply_anonymization_rules(self, df: Any, table_plan: list[dict], salt: str | None = None) -> Any:
        raise NotImplementedError

    @abstractmethod
    def ensure_plan_security_metadata(self, schema_name: str, table_name: str) -> tuple[str, str, bool]:
        raise NotImplementedError

    @abstractmethod
    def get_table_sample(self, schema: str, table: str, limit: int = 5) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def get_unified_ai_scan_payload(self, schema: str, tables: list[str], sample_limit: int = 5) -> list[dict]:
        raise NotImplementedError

