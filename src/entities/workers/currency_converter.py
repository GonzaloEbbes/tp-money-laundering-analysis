from common.entity import PipelineEntity


class CurrencyConverter(PipelineEntity):
    def entity_type(self):
        return "currency_converter"
