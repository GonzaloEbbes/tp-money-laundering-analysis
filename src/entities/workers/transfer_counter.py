from common.entity import PipelineEntity


class TransferCounter(PipelineEntity):
    def entity_type(self):
        return "transfer_counter"
