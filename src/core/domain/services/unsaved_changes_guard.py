from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass(frozen=True)
class UnsavedChangesCheckResult:
    has_unsaved_changes: bool
    unsaved_table_keys: List[str] = field(default_factory=list)


class UnsavedChangesGuard:
    """
    Preserves existing unsaved-plan safety logic from the UI layer and exposes
    it as a domain service so the same guard can be reused by use cases.
    """

    @staticmethod
    def get_unsaved_tables(current_plan_data: Dict[str, Any] | None) -> List[str]:
        current_data = current_plan_data or {}
        return [
            key
            for key, value in current_data.items()
            if isinstance(value, dict) and bool(value.get("dirty"))
        ]

    def check(self, current_plan_data: Dict[str, Any] | None) -> UnsavedChangesCheckResult:
        unsaved = self.get_unsaved_tables(current_plan_data)
        return UnsavedChangesCheckResult(
            has_unsaved_changes=bool(unsaved),
            unsaved_table_keys=unsaved,
        )

