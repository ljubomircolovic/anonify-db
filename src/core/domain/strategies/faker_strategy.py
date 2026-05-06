from faker import Faker
import pandas as pd

from .base_strategy import AnonymizationStrategy


class FakerStrategy(AnonymizationStrategy):
    """
    Mirrors DBManager FAKER behavior:
    - strategy_name one of faker_name/faker_email/faker_phone
    - generates a replacement for each row (including null rows)
    - on provider exception returns "Redacted"
    """

    @staticmethod
    def _resolve_faker_method(fake: Faker, strategy_name: str):
        if strategy_name == "faker_name":
            return fake.name
        if strategy_name == "faker_email":
            return fake.email
        if strategy_name == "faker_phone":
            return fake.phone_number
        return None

    def apply(self, series: pd.Series, **kwargs) -> pd.Series:
        strategy_name = str(kwargs.get("strategy_name", "faker_name")).lower()
        fake = kwargs.get("faker_instance") or Faker(["de_DE", "en_US"])
        provider = self._resolve_faker_method(fake, strategy_name)

        def _safe_generate():
            try:
                if provider is None:
                    return "Redacted"
                return provider()
            except Exception:
                return "Redacted"

        return pd.Series([_safe_generate() for _ in range(len(series))], index=series.index)

