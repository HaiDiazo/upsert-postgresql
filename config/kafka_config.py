import json

from kafka import KafkaConsumer
from config.base import settings


def consumer_config(topic: str):
    consumer = KafkaConsumer(
        bootstrap_servers=settings.MP_KAFKA_AI.split(','),
        group_id=settings.MP_KAFKA_GROUP_ID,
        max_poll_records=settings.MP_KAFKA_MAX_POLL_RECORDS,
        auto_offset_reset=settings.MP_KAFKA_AUTO_OFFSET_RESET,
        value_deserializer=lambda message: json.loads(message.decode('utf-8'))
    )
    consumer.subscribe(topics=topic.split(','))
    return consumer
