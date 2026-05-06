import logging
import re

import pandas as pd

from .base_strategy import AnonymizationStrategy

logger = logging.getLogger(__name__)


class MaskingStrategy(AnonymizationStrategy):
    """
    Mirrors DBManager MASK behavior:
    - uses string masking for non-numeric-like columns
    - for numeric-like columns, masks then sanitizes to numeric
    - if sanitization fails, falls back to original value
    """

    @staticmethod
    def mask_value(value):
        value_str = str(value)
        if len(value_str) <= 3:
            return "***"
        if "@" in value_str:
            parts = value_str.split("@")
            return f"{parts[0][:2]}**@{parts[1][:2]}**.com"
        return f"{value_str[:3]}***"

    @staticmethod
    def _sanitize_numeric_mask(masked_val, fallback_val, column_name: str):
        if pd.isnull(masked_val):
            return fallback_val
        cleaned = re.sub(r"[^0-9.]", "", str(masked_val))
        if cleaned.count(".") > 1:
            first_dot = cleaned.find(".")
            cleaned = cleaned[: first_dot + 1] + cleaned[first_dot + 1 :].replace(".", "")
        if cleaned in ("", "."):
            logger.warning(
                f"⚠️ Numeric sanitization fallback on column '{column_name}' for value '{masked_val}'. Keeping original value."
            )
            return fallback_val
        try:
            return float(cleaned)
        except Exception:
            logger.warning(
                f"⚠️ Numeric cast fallback on column '{column_name}' for value '{masked_val}'. Keeping original value."
            )
            return fallback_val

    def apply(self, series: pd.Series, **kwargs) -> pd.Series:
        original_series = kwargs.get("original_series", series)
        column_name = str(kwargs.get("column_name", ""))
        is_numeric_like_column = pd.api.types.is_numeric_dtype(original_series)

        if is_numeric_like_column:
            masked_series = series.apply(
                lambda value: self.mask_value(value) if pd.notnull(value) else value
            )
            return pd.Series(
                [
                    self._sanitize_numeric_mask(masked_val, fallback_val, column_name)
                    for masked_val, fallback_val in zip(masked_series, original_series)
                ],
                index=series.index,
            )

        return series.apply(
            lambda value: self.mask_value(value) if pd.notnull(value) else value
        )

