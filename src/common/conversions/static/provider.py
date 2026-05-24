from decimal import Decimal

from ..conversion_rate_provider import ConversionRateProvider, ConversionRateProviderError


class StaticConversionRateProvider(ConversionRateProvider):
    def __init__(self, rates=None):
        self.rates = {}
        for key, value in (rates or {}).items():
            self.rates[str(key)] = Decimal(str(value))

    def get_rate_to_usd(self, currency, date):
        if not currency:
            raise ConversionRateProviderError("Currency is required")
        if not date:
            raise ConversionRateProviderError("Date is required")

        currency_key = str(currency).strip()
        date_key = str(date)[:10]
        dated_key = f"{currency_key}|{date_key}"

        if dated_key in self.rates:
            return self.rates[dated_key]
        if currency_key in self.rates:
            return self.rates[currency_key]

        raise ConversionRateProviderError(
            f"Missing static conversion rate for currency={currency_key} date={date_key}"
        )
