from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionResult:
    success: bool
    message: str = ""
    failed_table: str | None = None

