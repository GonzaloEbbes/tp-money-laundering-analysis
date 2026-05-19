from common.entity import PipelineEntity


class FilterDateWindow(PipelineEntity):
    def entity_type(self):
        return "filter_date_window"
