from dataclasses import dataclass


@dataclass(frozen=True)
class PlanRule:
    column: str
    is_pii: bool
    strategy: str
    reason: str = ""

