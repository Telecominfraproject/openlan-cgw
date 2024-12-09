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

    def flush(self, timeout_ms: int = 1000):
        assert self.is_connected(), \
                f"consumer: Cannot flush kafka topic while not connected!"

        while True:
            # We explicitly use get_single_msg instead of <get_msgs>
            # to make sure we return as soon as we find result,
            # without waiting for potential T/O
            message = self.get_single_msg(timeout_ms=timeout_ms)
            if message is None:
                break

            logger.debug("Flushed kafka msg: %s key=%s value=%s ts=%s" %
                    (message.topic, message.key, message.value, message.timestamp))

    def get_msgs(self, timeout_ms: int = 12000):
        res_list = []

        assert self.is_connected(),\
                f"consumer: Cannot get Kafka result msg, Not connected!"

        while True:
            # We explicitly use get_single_msg instead of <get_msgs>
            # to make sure we return as soon as we find result,
            # without waiting for potential T/O
            message = self.get_single_msg(timeout_ms=timeout_ms)
            if message is None:
                break

            res_list.append(message)
            logger.debug("consumer: Recv kafka msg: %s key=%s value=%s ts=%s" %
                    (message.topic, message.key, message.value, message.timestamp))

        return res_list

    def get_result_msg(self, uuid_val: int, timeout_ms: int = 12000):
        res_uuid = str(uuid.UUID(int=uuid_val))

        assert self.is_connected(),\
                f"consumer: Cannot get Kafka result msg, Not connected!"

        while True:
            # We explicitly use get_single_msg instead of <get_msgs>
            # to make sure we return as soon as we find result,
            # without waiting for potential T/O
            message = self.get_single_msg(timeout_ms=timeout_ms)
            if message is None:
                break

            logger.debug("Flushed kafka msg: %s key=%s value=%s ts=%s" %
                    (message.topic, message.key, message.value, message.timestamp))
            if res_uuid == message.value['uuid']:
                return message
        return None

    def get_single_msg(self, timeout_ms: int = 12000):
        assert self.is_connected(),\
                f"consumer: Cannot get Kafka result msg, Not connected!"

        msg = self.conn.poll(timeout_ms=timeout_ms, max_records=1)
        for partition, msgs in msg.items():
            for m in msgs:
                return m

        return None

    def get_msg_by_substring(self, substring: int, uuid_val: int, timeout_ms: int = 12000):
        res_uuid = uuid.UUID(int=uuid_val)

        assert self.is_connected(),\
                f"Cannot get Kafka result msg, Not connected!"

        while True:
            # We explicitly use get_single_msg instead of <get_msgs>
            # to make sure we return as soon as we find result,
            # without waiting for potential T/O
            message = self.get_single_msg(timeout_ms=timeout_ms)
            if message is None:
                break

            if re.search(substring, message.value):
                logger.debug("Found '%s' in kafka msg: %s key=%s value=%s ts=%s" %
                        (substring, message.topic, message.key, message.value, message.timestamp))
                return message

        return None
