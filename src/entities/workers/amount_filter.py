from common.entity import PipelineEntity


class AmountFilter(PipelineEntity):
    def entity_type(self):
        return "amount_filter"
