from .utils import Message, MacRange
from .log import logger

from typing import List, Tuple
import kafka
import time
import uuid
import sys


class Producer:
    def __init__(self, db: str, topic: str) -> None:
        self.db = db
        self.conn = None
        self.topic = topic
        self.message = Message()

    def __enter__(self) -> kafka.KafkaProducer:
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self) -> kafka.KafkaProducer:
        if self.conn is None:
            self.conn = kafka.KafkaProducer(bootstrap_servers=self.db, client_id="producer")
            logger.info("connected to kafka")
        else:
            logger.info("already connected to kafka")
        return self.conn

    def disconnect(self) -> None:
        if self.conn is None:
            return
        self.conn.close()
        logger.info("disconnected from kafka")
        self.conn = None

    def handle_group_creation(self, create: List[Tuple[str, int, str]], delete: List[str]) -> None:
        with self as conn:
            for group, shard_id, name in create:
                conn.send(self.topic, self.message.group_create(group, shard_id, name),
                          bytes(group, encoding="utf-8"))
            for group in delete:
                conn.send(self.topic, self.message.group_delete(group),
                          bytes(group, encoding="utf-8"))

    def handle_device_assignment(self, add: List[Tuple[str, MacRange]], remove: List[Tuple[str, MacRange]]) -> None:
        with self as conn:
            for group, mac_range in add:
                logger.debug(f"{group = }, {mac_range = }")
                conn.send(self.topic, self.message.add_dev_to_group(group, mac_range),
                          bytes(group, encoding="utf-8"))
            for group, mac_range in remove:
                conn.send(self.topic, self.message.remove_dev_from_group(group, mac_range),
                          bytes(group, encoding="utf-8"))

    def handle_device_messages(self, message: dict, group: str, mac_range: MacRange,
                               count: int, time_s: int, interval_s: int) -> None:
        if not time_s:
            end = sys.maxsize
        else:
            end = time.time() + time_s
        if not count:
            count = sys.maxsize

        with self as conn:
            for seq in range(count):
                for mac in mac_range:
                    conn.send(self.topic, self.message.to_device(group, mac, message, seq),
                              bytes(group, encoding="utf-8"))
                #time.sleep(interval_s)
                #if time.time() > end:
                #    break
