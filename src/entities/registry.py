from general import DataPerBankRedirector, TransferDataController
from joiners import JoinAverage, JoinMaxAmountPerBank, JoinScatterGather
from mappers import MapAverage, MapMaxAmountPerBank, MapScatterGather
from workers import (
    AggregationScatterGather,
    AmountFilter,
    CurrencyConverter,
    CurrencyFilter,
    DynamicAmountFilter,
    FilterDateWindow,
    PayFormatFilter,
    TransferCounter,
)


ENTITY_CLASSES = {
    "TransferDataController": TransferDataController,
    "CurrencyFilter": CurrencyFilter,
    "AmountFilter": AmountFilter,
    "DataPerBankRedirector": DataPerBankRedirector,
    "MapMaxAmountPerBank": MapMaxAmountPerBank,
    "JoinMaxAmountPerBank": JoinMaxAmountPerBank,
    "FilterDateWindow": FilterDateWindow,
    "DynamicAmountFilter": DynamicAmountFilter,
    "MapAverage": MapAverage,
    "JoinAverage": JoinAverage,
    "MapScatterGather": MapScatterGather,
    "AggregationScatterGather": AggregationScatterGather,
    "JoinScatterGather": JoinScatterGather,
    "PayFormatFilter": PayFormatFilter,
    "CurrencyConverter": CurrencyConverter,
    "TransferCounter": TransferCounter,
}
