from common.entity import PipelineEntity


class DataPerBankRedirector(PipelineEntity):
    def entity_type(self):
        return "data_per_bank_redirector"
