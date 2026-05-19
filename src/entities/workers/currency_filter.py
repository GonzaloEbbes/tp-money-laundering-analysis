from common.entity import PipelineEntity


class CurrencyFilter(PipelineEntity):
    def entity_type(self):
        return "currency_filter"
