from abc import ABC, abstractmethod
from typing import Any


class MetadataRepositoryPort(ABC):
    @abstractmethod
    def log_action(
        self,
        user: str,
        schema: str,
        table: str,
        score: int,
        salt: str,
        status: str = "SUCCESS",
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def log_unified_ai_scan(
        self,
        user: str,
        schema: str,
        tables: list[str],
        status: str = "UNIFIED_AI_SCAN",
        score: int = 0,
        salt: str = "unified_batch",
        estimated_tokens: int = 0,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_audit_logs(self, limit: int = 50) -> Any:
        raise NotImplementedError

