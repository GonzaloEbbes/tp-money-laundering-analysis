import hashlib

from .conversion_rate_provider import ConversionRateProviderError


def conversion_key(currency, date, target_currency="USD"):
    if not currency:
        raise ConversionRateProviderError("Currency is required")
    if not date:
        raise ConversionRateProviderError("Date is required")
    if not target_currency:
        raise ConversionRateProviderError("Target currency is required")

    source = str(currency).strip()
    target = str(target_currency).strip()
    day = str(date)[:10].replace("/", "-")
    return f"{source}|{target}|{day}"


def stable_hash(value):
    digest = hashlib.sha256(str(value).encode("utf-8")).hexdigest()
    return int(digest, 16)


def conversion_shard(key, total_workers):
    workers = int(total_workers)
    if workers <= 0:
        raise ConversionRateProviderError("total_workers must be greater than 0")
    return stable_hash(key) % workers
