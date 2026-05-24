import json
import logging
import time
from decimal import Decimal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ..conversion_rate_provider import ConversionRateProviderError


DEFAULT_BASE_URL = "https://api.frankfurter.app"
DEFAULT_USER_AGENT = "tp-money-laundering-analysis/1.0"
LOGGER = logging.getLogger(__name__)


class FrankfurterApiError(ConversionRateProviderError):
    pass


class FrankfurterClient:
    def __init__(
        self,
        base_url=DEFAULT_BASE_URL,
        timeout_seconds=5,
        user_agent=DEFAULT_USER_AGENT,
        max_retries=2,
        retry_delay_seconds=1,
        max_retry_delay_seconds=60,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent
        self.max_retries = int(max_retries)
        self.retry_delay_seconds = float(retry_delay_seconds)
        self.max_retry_delay_seconds = float(max_retry_delay_seconds)

    def get_rate(self, base_currency, quote_currency, date):
        base = _validated_currency(base_currency)
        quote = _validated_currency(quote_currency)

        if base == quote:
            return Decimal("1")

        data = self._get_json(self._rate_url(base, quote, date))
        try:
            return Decimal(str(data["rates"][quote]))
        except KeyError as error:
            LOGGER.exception(
                "Frankfurter response did not include requested rate. base=%s quote=%s date=%s response=%s",
                base,
                quote,
                date,
                data,
            )
            raise FrankfurterApiError(f"Frankfurter response missing rate: {data}") from error

    def _rate_url(self, base, quote, date):
        if not date:
            raise FrankfurterApiError("Date is required")

        params = {"base": base, "symbols": quote}
        return f"{self.base_url}/{str(date)[:10]}?{urlencode(params)}"

    def _get_json(self, url):
        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            },
        )

        body = self._get_body_with_retries(request, url)

        try:
            return json.loads(body)
        except json.JSONDecodeError as error:
            LOGGER.exception("Frankfurter returned invalid JSON. url=%s body=%s", url, body)
            raise FrankfurterApiError(f"Invalid JSON from Frankfurter: {body}") from error

    def _get_body_with_retries(self, request, url):
        attempts = self.max_retries + 1
        last_error = None

        for attempt in range(1, attempts + 1):
            try:
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    return response.read().decode("utf-8")
            except HTTPError as error:
                detail = error.read().decode("utf-8", errors="replace")
                last_error = FrankfurterApiError(
                    f"Frankfurter HTTP {error.code} for {url}: {detail}"
                )
                if not _should_retry_http(error.code) or attempt == attempts:
                    LOGGER.exception(
                        "Frankfurter HTTP error. attempt=%s max_attempts=%s status=%s url=%s detail=%s",
                        attempt,
                        attempts,
                        error.code,
                        url,
                        detail,
                    )
                    raise last_error from error

                delay = self._retry_delay(detail)
                LOGGER.warning(
                    "Retrying Frankfurter HTTP error. attempt=%s max_attempts=%s status=%s delay_seconds=%s url=%s detail=%s",
                    attempt,
                    attempts,
                    error.code,
                    delay,
                    url,
                    detail,
                )
                time.sleep(delay)
            except URLError as error:
                last_error = FrankfurterApiError(
                    f"Frankfurter request failed for {url}: {error}"
                )
                if attempt == attempts:
                    LOGGER.exception(
                        "Frankfurter request failed. attempt=%s max_attempts=%s url=%s",
                        attempt,
                        attempts,
                        url,
                    )
                    raise last_error from error

                delay = min(self.retry_delay_seconds, self.max_retry_delay_seconds)
                LOGGER.warning(
                    "Retrying Frankfurter request failure. attempt=%s max_attempts=%s delay_seconds=%s url=%s error=%s",
                    attempt,
                    attempts,
                    delay,
                    url,
                    error,
                )
                time.sleep(delay)

        raise last_error

    def _retry_delay(self, response_body):
        retry_after = _retry_after_from_body(response_body)
        if retry_after is not None:
            return min(retry_after, self.max_retry_delay_seconds)
        return min(self.retry_delay_seconds, self.max_retry_delay_seconds)


def _validated_currency(currency):
    if not currency:
        raise FrankfurterApiError("Currency is required")
    return str(currency).strip()


def _should_retry_http(status_code):
    return 500 <= int(status_code) <= 599


def _retry_after_from_body(response_body):
    try:
        data = json.loads(response_body)
    except json.JSONDecodeError:
        return None

    retry_after = data.get("retry_after")
    if retry_after is None:
        return None

    try:
        return float(retry_after)
    except (TypeError, ValueError):
        return None
