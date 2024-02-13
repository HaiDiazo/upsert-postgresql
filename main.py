import traceback
from loguru import logger
from config import kafka_config
from service.pg_service import PgService
from config.base import settings
import time

TOPIC = settings.MP_KAFKA_TOPIC
GROUP_ID = settings.MP_KAFKA_GROUP_ID


def fetch(message_lists: list):
    for message in message_lists:
        source = message.value
        # topic = message.topic

        # print(source)
        service = PgService(data=source)
        service.upsert()


if __name__ == "__main__":
    consumer = kafka_config.consumer_config(topic=TOPIC)
    logger.info("Start Consuming...[{}]", TOPIC)

    try:
        try:
            while True:
                messages = consumer.poll(1.0)

                for _, message_list in messages.items():
                    start_time = time.time()
                    fetch(message_list)
                    logger.info("=======================================")
                    logger.info(f"consumer {TOPIC} {GROUP_ID} {len(message_list)}")
                    logger.info(f"Time Exec: {time.time() - start_time}")
                    logger.info("=======================================")
        except Exception as err:
            traceback.print_exc()
            raise err
    except KeyboardInterrupt:
        print("Interrupt Keyboard")
