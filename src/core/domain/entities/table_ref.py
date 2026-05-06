from dataclasses import dataclass


@dataclass(frozen=True)
class TableRef:
    schema_name: str
    table_name: str

    @property
    def fq_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}"

