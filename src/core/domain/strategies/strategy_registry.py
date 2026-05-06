import pandas as pd

from .base_strategy import AnonymizationStrategy
from .faker_strategy import FakerStrategy
from .hash_strategy import HashStrategy
from .masking_strategy import MaskingStrategy


class StrategyRegistry:
    def __init__(self):
        self._strategies: dict[str, AnonymizationStrategy] = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        self.register("mask", MaskingStrategy())
        self.register("hash", HashStrategy())
        self.register("faker_name", FakerStrategy())
        self.register("faker_email", FakerStrategy())
        self.register("faker_phone", FakerStrategy())

    def register(self, name: str, strategy: AnonymizationStrategy) -> None:
        self._strategies[str(name).lower()] = strategy

    def get(self, name: str) -> AnonymizationStrategy:
        key = str(name).lower()
        if key not in self._strategies:
            raise KeyError(f"Strategy '{name}' is not registered")
        return self._strategies[key]

    def apply(self, name: str, series: pd.Series, **kwargs) -> pd.Series:
        strategy = self.get(name)
        if str(name).lower().startswith("faker_") and "strategy_name" not in kwargs:
            kwargs["strategy_name"] = str(name).lower()
        return strategy.apply(series, **kwargs)

