from common.entity import PipelineEntity


class JoinMaxAmountPerBank(PipelineEntity):
    def entity_type(self):
        return "join_max_amount_per_bank"
