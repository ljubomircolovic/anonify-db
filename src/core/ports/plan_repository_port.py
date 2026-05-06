from abc import ABC, abstractmethod


class PlanRepositoryPort(ABC):
    @abstractmethod
    def save_ai_plan(
        self,
        schema_name: str,
        table_name: str,
        plan_data: list[dict],
        where_condition: str = "",
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_saved_plan(self, schema_name: str, table_name: str) -> dict | None:
        raise NotImplementedError

    @abstractmethod
    def get_all_saved_plans(self, schema_name: str) -> dict:
        raise NotImplementedError

