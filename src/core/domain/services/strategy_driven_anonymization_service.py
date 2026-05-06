import logging
import hashlib
import os
from typing import Any, Callable

import pandas as pd
from faker import Faker

from src.core.domain.strategies import StrategyRegistry

logger = logging.getLogger(__name__)


class StrategyDrivenAnonymizationService:
    """
    Applies anonymization plans using extracted Strategy classes.
    For strategies not extracted yet, it can delegate per-column execution to
    the legacy transformer to preserve behavior during migration.
    """

    def __init__(self, registry: StrategyRegistry | None = None):
        self._registry = registry or StrategyRegistry()

    @staticmethod
    def _seed_value_from_env() -> int:
        seed_salt = os.getenv("ANONIFY_SALT", "default_plan_salt")
        return int(hashlib.sha256(str(seed_salt).encode("utf-8")).hexdigest()[:16], 16)

    def _build_seeded_faker(self, faker_instance: Any = None):
        seed_value = self._seed_value_from_env()
        fake = faker_instance if faker_instance is not None else Faker("en_US")
        try:
            fake.seed_instance(seed_value)
        except Exception:
            pass
        return fake

    def apply_anonymization_rules(
        self,
        df: pd.DataFrame,
        table_plan: list[dict],
        salt: str | None = None,
        faker_instance: Any = None,
        fallback_legacy_transform: Callable[[pd.DataFrame, list[dict], str | None], pd.DataFrame] | None = None,
    ) -> pd.DataFrame:
        if df.empty:
            return df

        df_anon = df.copy()
        effective_salt = salt or "default_plan_salt"
        seeded_faker = self._build_seeded_faker(faker_instance)

        for item in table_plan or []:
            if not isinstance(item, dict):
                continue

            col = item.get("column")
            strategy = str(item.get("strategy", "keep")).lower()
            if not col or col not in df_anon.columns:
                continue

            original_series = df_anon[col].copy()

            if strategy == "keep":
                df_anon[col] = original_series
                continue

            if strategy == "null":
                df_anon[col] = None
                continue

            if strategy in {"mask", "hash", "faker_name", "faker_email", "faker_phone"}:
                df_anon[col] = self._registry.apply(
                    strategy,
                    df_anon[col],
                    salt=effective_salt,
                    original_series=original_series,
                    column_name=col,
                    faker_instance=seeded_faker,
                    strategy_name=strategy,
                )
            elif fallback_legacy_transform is not None:
                # Preserve exact behavior for not-yet-extracted strategies.
                temp_df = fallback_legacy_transform(df_anon[[col]].copy(), [item], effective_salt)
                if col in temp_df.columns:
                    df_anon[col] = temp_df[col]
            else:
                logger.warning(
                    "No extracted strategy and no fallback available for strategy '%s' on column '%s'.",
                    strategy,
                    col,
                )

            # Mirror DBManager final numeric guard to keep behavior aligned.
            if pd.api.types.is_numeric_dtype(original_series):
                coerced = pd.to_numeric(df_anon[col], errors="coerce")
                invalid_mask = coerced.isna() & original_series.notna()
                if invalid_mask.any():
                    df_anon.loc[invalid_mask, col] = original_series.loc[invalid_mask]

        return df_anon

