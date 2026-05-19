import logging
import os

from registry import ENTITY_CLASSES


def main():
    logging.basicConfig(level=logging.INFO)
    entity_class_name = os.environ["ENTITY_CLASS"]
    entity_class = ENTITY_CLASSES[entity_class_name]
    entity = entity_class(
        mom_host=os.environ["MOM_HOST"],
        input_queue=os.environ["INPUT_QUEUE"],
        output_queue=os.environ["OUTPUT_QUEUE"],
    )
    entity.start()


if __name__ == "__main__":
    main()
