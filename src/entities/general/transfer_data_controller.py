from common.entity import PipelineEntity


class TransferDataController(PipelineEntity):
    def entity_type(self):
        return "transfer_data_controller"
