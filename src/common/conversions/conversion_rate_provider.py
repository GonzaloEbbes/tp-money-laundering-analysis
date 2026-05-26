from abc import ABC, abstractmethod


class ConversionRateProviderError(RuntimeError):
    pass


class ConversionRateProvider(ABC):
    @abstractmethod
    def get_rate_to_usd(self, currency, date):
        """Return the rate needed to convert one unit of currency to USD."""
        raise NotImplementedError
