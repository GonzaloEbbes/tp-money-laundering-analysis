from general import DataPerBankRedirector, TransferDataController
from joiners import JoinAverage, JoinMaxAmountPerBank, JoinScatterGather
from mappers import MapAverage, MapMaxAmountPerBank, MapScatterGather
from workers import (
    AggregationScatterGather,
    CurrencyConverter,
    DynamicAmountFilter,
    BankDeduplicator,
)

ENTITY_CLASSES = {
    "TransferDataController": TransferDataController,
    "DataPerBankRedirector": DataPerBankRedirector,
    "MapMaxAmountPerBank": MapMaxAmountPerBank,
    "JoinMaxAmountPerBank": JoinMaxAmountPerBank,
    "DynamicAmountFilter": DynamicAmountFilter,
    "MapAverage": MapAverage,
    "JoinAverage": JoinAverage,
    "MapScatterGather": MapScatterGather,
    "AggregationScatterGather": AggregationScatterGather,
    "JoinScatterGather": JoinScatterGather,
    "CurrencyConverter": CurrencyConverter,
    "BankDeduplicator": BankDeduplicator,
}
