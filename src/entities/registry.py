from general import DataPerBankRedirector, TransferDataController
from joiners import JoinAverage, JoinMaxAmountPerBank
from mappers import MapAverage, MapMaxAmountPerBank
from workers import (
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
    "CurrencyConverter": CurrencyConverter,
    "BankDeduplicator": BankDeduplicator,
}
