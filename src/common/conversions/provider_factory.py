import json
import os

from .conversion_rate_provider import ConversionRateProviderError
from .frankfurter import (
    DEFAULT_BASE_URL,
    DEFAULT_USER_AGENT,
    FrankfurterConversionRateProvider,
    FrankfurterClient,
)
from .static import StaticConversionRateProvider


def build_conversion_rate_provider(name=None, environ=None, static_rates=None):
    env = environ if environ is not None else os.environ
    provider_name = (name or env.get("CONVERSION_PROVIDER", "frankfurter")).strip()

    if provider_name == "frankfurter":
        return _build_frankfurter_provider(env)
    if provider_name == "static":
        return StaticConversionRateProvider(
            rates=static_rates if static_rates is not None else _static_rates_from_config(env, required=True)
        )

    raise ConversionRateProviderError(f"Unknown conversion provider: {provider_name}")


def _build_frankfurter_provider(env):
    client = FrankfurterClient(
        base_url=env.get("FRANKFURTER_API_URL", DEFAULT_BASE_URL),
        timeout_seconds=float(env.get("FRANKFURTER_TIMEOUT_SECONDS", "5")),
        user_agent=env.get("FRANKFURTER_USER_AGENT", DEFAULT_USER_AGENT),
        max_retries=int(env.get("FRANKFURTER_MAX_RETRIES", "2")),
        retry_delay_seconds=float(env.get("FRANKFURTER_RETRY_DELAY_SECONDS", "1")),
        max_retry_delay_seconds=float(env.get("FRANKFURTER_MAX_RETRY_DELAY_SECONDS", "60")),
    )
    return FrankfurterConversionRateProvider(client=client)


def _static_rates_from_config(env, required):
    rates_path = env.get("STATIC_CONVERSION_RATES_PATH")
    if not rates_path:
        if not required:
            return {}
        raise ConversionRateProviderError(
            "STATIC_CONVERSION_RATES_PATH is required when CONVERSION_PROVIDER=static"
        )

    try:
        with open(rates_path, "r", encoding="utf-8") as rates_file:
            rates = json.load(rates_file)
    except OSError as error:
        raise ConversionRateProviderError(
            f"Could not read STATIC_CONVERSION_RATES_PATH={rates_path}"
        ) from error
    except json.JSONDecodeError as error:
        raise ConversionRateProviderError(
            f"Invalid JSON in STATIC_CONVERSION_RATES_PATH={rates_path}"
        ) from error

    return _validate_static_rates(rates, source=rates_path)


def _validate_static_rates(rates, source):
    if not isinstance(rates, dict):
        raise ConversionRateProviderError(f"{source} must be a JSON object")
    return rates
