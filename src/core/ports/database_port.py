from abc import ABC, abstractmethod
from typing import Any


class DatabasePort(ABC):
    @abstractmethod
    def get_all_schemas(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_tables(self, schema_name: str = "public") -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_tables_in_schema(self, schema: str = "public") -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def read_table(
        self,
        table_name: str,
        schema_name: str = "public",
        where: str | None = None,
        limit: int = 100,
        params: dict | None = None,
    ) -> Any:
        raise NotImplementedError

    @abstractmethod
    def get_columns(self, table_name: str, schema_name: str = "public") -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_column_details(self, table_name: str, schema_name: str) -> dict:
        raise NotImplementedError

    @abstractmethod
    def get_primary_keys(self, schema: str, table: str) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_all_foreign_keys(self, schema_name: str) -> list[tuple]:
        raise NotImplementedError

    @abstractmethod
    def get_execution_order(self, selected_tables: list[str], schema_name: str = "public") -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def create_anonymized_table(
        self,
        source_schema: str,
        table_name: str,
        target_db: str,
        target_schema: str = "anon",
    ) -> tuple[bool, str]:
        raise NotImplementedError

    @abstractmethod
    def save_anonymized_table(
        self,
        df: Any,
        table_name: str,
        target_schema: str = "anon",
        conn: Any = None,
        source_schema: str = "public",
        preserve_native_columns: list[str] | None = None,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def truncate_anon_tables(self, target_schema: str, ordered_tables: list[str]) -> None:
        raise NotImplementedError

