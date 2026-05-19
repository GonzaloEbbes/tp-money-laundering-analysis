from common.entity import PipelineEntity


class AggregationScatterGather(PipelineEntity):
    def entity_type(self):
        return "aggregation_scatter_gather"
