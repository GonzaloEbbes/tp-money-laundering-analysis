from common.entity import PipelineEntity


class JoinAverage(PipelineEntity):
    def entity_type(self):
        return "join_average"
