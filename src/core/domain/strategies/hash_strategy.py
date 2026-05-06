import hashlib

import pandas as pd

from .base_strategy import AnonymizationStrategy


class HashStrategy(AnonymizationStrategy):
    """
    Mirrors DBManager HASH behavior:
    - null values remain unchanged
    - uses SHA-256 over f"{value}{salt}"
    - output truncated to first 12 hex chars
    """

    @staticmethod
    def secure_hash(value, salt: str) -> str:
        hash_obj = hashlib.sha256(f"{value}{salt}".encode())
        return hash_obj.hexdigest()[:12]

    def apply(self, series: pd.Series, **kwargs) -> pd.Series:
        effective_salt = kwargs.get("salt") or "default_plan_salt"
        return series.apply(
            lambda value: value if pd.isnull(value) else self.secure_hash(value, effective_salt)
        )

