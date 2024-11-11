from .utils import Message, MacRange
from .log import logger

from typing import List, Tuple
from kafka.structs import OffsetAndMetadata
import kafka
import time
import uuid
import sys
import re
import json


class Consumer:
    def __init__(self, db: str, topic: str, consumer_timeout: int) -> None:
        self.db = db
        self.conn = None
        self.topic = topic
        self.consumer_timeout = consumer_timeout
        self.message = Message()

    def __enter__(self) -> kafka.KafkaConsumer:
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


    def connect(self) -> kafka.KafkaConsumer:
        if self.is_connected() is False:
            self.conn = kafka.KafkaConsumer(self.topic,
                                            bootstrap_servers=self.db,
                                            client_id="consumer_1",
                                            group_id="cgw_tests_consumer",
                                            auto_offset_reset='latest',
                                            enable_auto_commit=True,
                                            consumer_timeout_ms=self.consumer_timeout,
                                            value_deserializer=lambda m: json.loads(m.decode('utf-8')))
            logger.info("consumer: connected to kafka")
        else:
            logger.info("consumer: already connected to kafka")
        return self.conn

    def disconnect(self) -> None:
        if self.is_connected() is False:
            return
        self.conn.close()
        logger.info("consumer: disconnected from kafka")
        self.conn = None

    def is_connected(self) -> bool:
        return self.conn is not None

    def flush(self):
        assert self.is_connected(), \
                f"consumer: Cannot flush kafka topic while not connected!"

        for message in self.conn:
            logger.debug("consumer: Flushed kafka msg: %s key=%s value=%s ts=%s" %
                    (message.topic,message.key, message.value, message.timestamp))

    def get_msgs(self):
        res_list = []

        assert self.is_connected(),\
                f"consumer: Cannot get Kafka result msg, Not connected!"

        for message in self.conn:
            res_list.append(message)
            logger.debug("consumer: Recv kafka msg: %s key=%s value=%s ts=%s" %
                    (message.topic, message.key, message.value, message.timestamp))

        return res_list

    def get_result_msg(self, uuid_val: int):
        res_uuid = str(uuid.UUID(int=uuid_val))

        assert self.is_connected(),\
                f"consumer: Cannot get Kafka result msg, Not connected!"

        for message in self.conn:
            logger.debug("Flushed kafka msg: %s key=%s value=%s ts=%s" %
                    (message.topic, message.key, message.value, message.timestamp))
            if res_uuid == message.value['uuid']:
                return message

    def get_msg_by_substring(self, substring: int):
        res_uuid = uuid.UUID(int=uuid_val)

        assert self.is_connected(),\
                f"Cannot get Kafka result msg, Not connected!"

        for message in self.conn:
            res_list.append(message)
            if re.search(substring, message.value):
                logger.debug("Found '%s' in kafka msg: %s key=%s value=%s ts=%s" %
                        (substring, message.topic, message.key, message.value, message.timestamp))
                return message
