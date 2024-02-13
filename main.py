from loguru import logger
from config import kafka_config
from service.pg_service import PgService
from config.base import settings

TOPIC = settings.MP_KAFKA_TOPIC


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
                    fetch(message_list)

                    logger.info("=======================================")
                    logger.info(f"consumer {TOPIC} {len(message_list)}")
                    logger.info("=======================================")
        except Exception as err:
            raise err
    except KeyboardInterrupt:
        print("Interrupt Keyboard")
