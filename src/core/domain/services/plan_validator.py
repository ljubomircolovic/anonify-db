from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    errors: List[str] = field(default_factory=list)


class PlanValidator:
    """
    Phase 1 extractor placeholder.
    Concrete validation logic remains in existing runtime paths and will be
    migrated incrementally in later phases with no logic deletion.
    """

    def validate(self, _plan_rows: list[dict]) -> ValidationResult:
        return ValidationResult(valid=True, errors=[])

