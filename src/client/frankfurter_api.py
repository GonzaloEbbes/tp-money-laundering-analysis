import json
import logging
from decimal import Decimal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = "https://api.frankfurter.app"
LOGGER = logging.getLogger(__name__)


class FrankfurterApiError(RuntimeError):
    pass


DEFAULT_USER_AGENT = "tp-money-laundering-analysis/1.0"


class FrankfurterClient:
    def __init__(
        self,
        base_url=DEFAULT_BASE_URL,
        timeout_seconds=5,
        api_version="v1",
        user_agent=DEFAULT_USER_AGENT,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.api_version = api_version
        self.user_agent = user_agent

    def get_rate(self, base_currency, quote_currency, date=None):
        base = _validated_currency(base_currency)
        quote = _validated_currency(quote_currency)

        if base == quote:
            return Decimal("1")

        url = self._rate_url(base, quote, date)
        data = self._get_json(url)

        if "rate" in data:
            return Decimal(str(data["rate"]))

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

    def convert(self, amount, base_currency, quote_currency="USD", date=None):
        rate = self.get_rate(base_currency, quote_currency, date)
        return Decimal(str(amount)) * rate

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

    def _rate_url(self, base, quote, date):
        if self.api_version == "v2":
            path = f"/v2/rate/{base}/{quote}"
            params = {}
            if date:
                params["date"] = str(date)[:10]
            url = f"{self.base_url}{path}"
        else:
            path = f"/{str(date)[:10]}" if date else "/latest"
            params = {"base": base, "symbols": quote}
            url = f"{self.base_url}{path}"

        if params:
            return f"{url}?{urlencode(params)}"
        return url


def _validated_currency(currency):
    if not currency:
        raise FrankfurterApiError("Currency is required")
    return str(currency).strip()
