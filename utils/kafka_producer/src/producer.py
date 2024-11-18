from .utils import Message, MacRange
from .log import logger

from typing import List, Tuple
import kafka
import time
import uuid
import sys
import random


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
        if self.is_connected() is False:
            self.conn = kafka.KafkaProducer(bootstrap_servers=self.db, client_id="producer")
            logger.info("producer: connected to kafka")
        else:
            logger.info("producer: already connected to kafka")
            raise Exception('')
        return self.conn

    def disconnect(self) -> None:
        if self.is_connected() is False:
            return
        self.conn.close()
        logger.info("producer: disconnected from kafka")
        self.conn = None

    def is_connected(self) -> bool:
        return self.conn is not None

    def handle_single_group_delete(self, group: str, uuid_val: int = None):
        if group is None:
            raise Exception('producer: Cannot destroy group without group_id specified!')

        self.conn.send(self.topic, self.message.group_delete(group, uuid_val),
            bytes(group, encoding="utf-8"))
        self.conn.flush()

    def handle_single_group_create(self, group: str, uuid_val: int = None):
        if group is None:
            raise Exception('producer: Cannot create new group without group id specified!')

        self.conn.send(self.topic, self.message.group_create(group, "cgw_default_group_name", uuid_val),
                bytes(group, encoding="utf-8"))
        self.conn.flush()

    def handle_single_group_create_to_shard(self, group: str, shard_id: int, uuid_val: int = None):
        if group is None:
            raise Exception('producer: Cannot create new group without group id specified!')

        self.conn.send(self.topic, self.message.group_create_to_shard(group, shard_id, "cgw_default_group_name", uuid_val),
                bytes(group, encoding="utf-8"))
        self.conn.flush()

    def handle_group_creation(self, create: List[Tuple[str, str]], delete: List[str]) -> None:
        with self as conn:
            for group, name in create:
                conn.send(self.topic, self.message.group_create(group, name),
                          bytes(group, encoding="utf-8"))
            for group in delete:
                conn.send(self.topic, self.message.group_delete(group),
                          bytes(group, encoding="utf-8"))
            conn.flush()

    def handle_single_device_assign(self, group: str, mac: str, uuid_val: int):
        if group is None:
            raise Exception('producer: Cannot assign infra to group without group id specified!')

        if mac is None:
            raise Exception('producer: Cannot assign infra to group without infra MAC specified!')

        mac_range = MacRange(mac)

        self.conn.send(self.topic, self.message.add_dev_to_group(group, mac_range, uuid_val),
                bytes(group, encoding="utf-8"))
        self.conn.flush()

    def handle_single_device_deassign(self, group: str, mac: str):
        if group is None:
            raise Exception('Cannot deassign infra from group without group id specified!')

        if mac is None:
            raise Exception('Cannot deassign infra from group without infra MAC specified!')

        mac_range = MacRange(mac)

        self.conn.send(self.topic, self.message.remove_dev_from_group(group, mac_range),
                bytes(group, encoding="utf-8"))
        self.conn.flush()

    def handle_device_assignment(self, add: List[Tuple[str, MacRange]], remove: List[Tuple[str, MacRange]]) -> None:
        with self as conn:
            for group, mac_range in add:
                logger.debug(f"{group = }, {mac_range = }")
                conn.send(self.topic, self.message.add_dev_to_group(group, mac_range),
                          bytes(group, encoding="utf-8"))
            for group, mac_range in remove:
                conn.send(self.topic, self.message.remove_dev_from_group(group, mac_range),
                          bytes(group, encoding="utf-8"))
            conn.flush()

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
                conn.flush()
                #time.sleep(interval_s)
                #if time.time() > end:
                #    break
