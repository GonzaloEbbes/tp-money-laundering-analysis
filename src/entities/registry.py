from joiners import JoinAverage
from mappers import MapAverage
from workers import (
    CurrencyConverter,
)

ENTITY_CLASSES = {
    "MapAverage": MapAverage,
    "JoinAverage": JoinAverage,
    "CurrencyConverter": CurrencyConverter,
}
