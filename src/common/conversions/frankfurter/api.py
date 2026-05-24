import json
import logging
from decimal import Decimal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = "https://api.frankfurter.app"
DEFAULT_USER_AGENT = "tp-money-laundering-analysis/1.0"
LOGGER = logging.getLogger(__name__)


class FrankfurterApiError(RuntimeError):
    pass


class FrankfurterClient:
    def __init__(
        self,
        base_url=DEFAULT_BASE_URL,
        timeout_seconds=5,
        user_agent=DEFAULT_USER_AGENT,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent

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

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read().decode("utf-8")
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            LOGGER.exception(
                "Frankfurter HTTP error. status=%s url=%s detail=%s",
                error.code,
                url,
                detail,
            )
            raise FrankfurterApiError(
                f"Frankfurter HTTP {error.code} for {url}: {detail}"
            ) from error
        except URLError as error:
            LOGGER.exception("Frankfurter request failed. url=%s", url)
            raise FrankfurterApiError(f"Frankfurter request failed for {url}: {error}") from error

        try:
            return json.loads(body)
        except json.JSONDecodeError as error:
            LOGGER.exception("Frankfurter returned invalid JSON. url=%s body=%s", url, body)
            raise FrankfurterApiError(f"Invalid JSON from Frankfurter: {body}") from error


def _validated_currency(currency):
    if not currency:
        raise FrankfurterApiError("Currency is required")
    return str(currency).strip()
