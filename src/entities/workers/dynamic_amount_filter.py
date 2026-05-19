from common.entity import PipelineEntity


class DynamicAmountFilter(PipelineEntity):
    def entity_type(self):
        return "dynamic_amount_filter"
