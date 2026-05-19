from common.entity import PipelineEntity


class JoinScatterGather(PipelineEntity):
    def entity_type(self):
        return "join_scatter_gather"
