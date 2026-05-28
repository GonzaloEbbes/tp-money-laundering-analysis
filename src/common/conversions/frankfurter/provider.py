from ..conversion_rate_provider import ConversionRateProvider
from .api import FrankfurterClient


class FrankfurterConversionRateProvider(ConversionRateProvider):
    def __init__(self, client=None):
        self.client = client if client else FrankfurterClient()

    def get_rate_to_usd(self, currency, date):
        return self.client.get_rate(currency, "USD", date)
