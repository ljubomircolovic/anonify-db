from abc import ABC, abstractmethod

import pandas as pd


class AnonymizationStrategy(ABC):
    @abstractmethod
    def apply(self, series: pd.Series, **kwargs) -> pd.Series:
        raise NotImplementedError

