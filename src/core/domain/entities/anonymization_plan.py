from dataclasses import dataclass, field
from typing import List

from .plan_rule import PlanRule
from .table_ref import TableRef


@dataclass(frozen=True)
class AnonymizationPlan:
    table: TableRef
    rules: List[PlanRule] = field(default_factory=list)
    where_condition: str = ""
    salt: str | None = None

