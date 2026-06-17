from .conversion_rate_provider import ConversionRateProviderError


DATASET_TO_FRANKFURTER_CODE = {
    "Australian Dollar": "AUD",
    "Brazil Real": "BRL",
    "Canadian Dollar": "CAD",
    "Euro": "EUR",
    "Mexican Peso": "MXN",
    "Rupee": "INR",
    "Shekel": "ILS",
    "Swiss Franc": "CHF",
    "UK Pound": "GBP",
    "US Dollar": "USD",
    "Yen": "JPY",
    "Yuan": "CNY",
}

UNSUPPORTED_FRANKFURTER_CURRENCIES = {
    "Bitcoin",
    "Ruble",
    "Saudi Riyal",
}


def to_frankfurter_currency(currency):
    if not currency:
        raise ConversionRateProviderError("Currency is required")

    value = str(currency).strip()
    if value in DATASET_TO_FRANKFURTER_CODE:
        return DATASET_TO_FRANKFURTER_CODE[value]
    if value in UNSUPPORTED_FRANKFURTER_CURRENCIES:
        return value
    return value


def is_unsupported_by_frankfurter(currency):
    return str(currency).strip() in UNSUPPORTED_FRANKFURTER_CURRENCIES
