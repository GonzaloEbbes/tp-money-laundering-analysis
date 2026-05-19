from common.entity import PipelineEntity


class PayFormatFilter(PipelineEntity):
    def entity_type(self):
        return "pay_format_filter"
