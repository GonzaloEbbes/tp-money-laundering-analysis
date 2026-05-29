from joiners import JoinAverage
from mappers import MapAverage
from workers import (
    CurrencyConverter,
    DynamicAmountFilter,
)

ENTITY_CLASSES = {
    "DynamicAmountFilter": DynamicAmountFilter,
    "MapAverage": MapAverage,
    "JoinAverage": JoinAverage,
    "CurrencyConverter": CurrencyConverter,
}
