from .utils import Message, MacRange, UCentralConfigRequest
from .log import logger

from typing import List, Tuple
import kafka
import time
import uuid
import sys
import random
import json


class Producer:
    @staticmethod
    def device_message_reboot(mac: str, id: int = None):
        msg = {}
        params = {}

        if mac is None:
            raise Exception('Cannot format message without MAC specified')

        if id is None:
            id = 1

        params["serial"] = mac
        params["when"] = 0

        msg["jsonrpc"] = "2.0"
        msg["method"] = "reboot"
        msg["params"] = params
        msg["id"] = id

        return msg

    @staticmethod
    def device_message_factory(mac: str, id: int = None, keep_rediretor: bool = None):
        msg = {}
        params = {}

        if mac is None:
            raise Exception('Cannot format message without MAC specified')

        if id is None:
            id = 1

        if keep_rediretor is None:
            keep_rediretor = True

        params["serial"] = mac
        params["when"] = 0
        params["keep_rediretor"] = keep_rediretor

        msg["jsonrpc"] = "2.0"
        msg["method"] = "factory"
        msg["params"] = params
        msg["id"] = id

        return msg

    @staticmethod
    def device_message_ping(mac: str, id: int = None):
        msg = {}
        params = {}

        if mac is None:
            raise Exception('Cannot format message without MAC specified')

        if id is None:
            id = 1

        params["serial"] = mac

        msg["jsonrpc"] = "2.0"
        msg["method"] = "ping"
        msg["params"] = params
        msg["id"] = id

        return msg

    def device_message_config_ap_basic(self, mac: str, id: int = None) -> str:
        if mac is None:
            raise Exception('Cannot format message without MAC specified')

        if id is None:
            id = 1

        msg = self.ucentral_configs.get_ap_basic_cfg(mac, id)
        return json.loads(msg)

    def device_message_config_ap_basic_invalid(self, mac: str, id: int = None) -> str:
        if mac is None:
            raise Exception('Cannot format message without MAC specified')

        if id is None:
            id = 1

        msg = self.ucentral_configs.get_ap_basic_invalid_cfg(mac, id)
        return json.loads(msg)

    def device_message_config_switch_basic(self, mac: str, id: int = None) -> str:
        if mac is None:
            raise Exception('Cannot format message without MAC specified')

        if id is None:
            id = 1

        msg = self.ucentral_configs.get_switch_basic_cfg(mac, id)
        return json.loads(msg)

    def device_message_config_switch_basic_invalid(self, mac: str, id: int = None) -> str:
        if mac is None:
            raise Exception('Cannot format message without MAC specified')

        if id is None:
            id = 1

        msg = self.ucentral_configs.get_switch_basic_invalid_cfg(mac, id)
        return json.loads(msg)

    def __init__(self, db: str, topic: str) -> None:
        self.db = db
        self.conn = None
        self.topic = topic
        self.message = Message()
        self.ucentral_configs = UCentralConfigRequest()

    def __enter__(self) -> kafka.KafkaProducer:
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self) -> kafka.KafkaProducer:
        if self.is_connected() is False:
            self.conn = kafka.KafkaProducer(
                bootstrap_servers=self.db,
                client_id="producer",
                max_block_ms=12000,
                request_timeout_ms=12000)
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

    def handle_single_group_delete(self, group: int, uuid_val: int = None):
        if group is None:
            raise Exception(
                'producer: Cannot destroy group without group_id specified!')

        self.conn.send(self.topic, self.message.group_delete(group, uuid_val),
                       bytes(str(group), encoding="utf-8"))
        self.conn.flush()

    def handle_single_group_create(self, group: int, uuid_val: int = None, shard_id: int = None):
        if group is None:
            raise Exception(
                'producer: Cannot create new group without group id specified!')

        if shard_id is None:
            self.conn.send(self.topic, self.message.group_create(group, uuid_val),
                           bytes(str(group), encoding="utf-8"))
        else:
            self.conn.send(self.topic, self.message.group_create_to_shard(group, shard_id, uuid_val),
                           bytes(str(group), encoding="utf-8"))
        self.conn.flush()

    def handle_group_creation(self, create: List[int], delete: List[int]) -> None:
        with self as conn:
            for group in create:
                conn.send(self.topic, self.message.group_create(group),
                          bytes(str(group), encoding="utf-8"))
            for group in delete:
                conn.send(self.topic, self.message.group_delete(group),
                          bytes(str(group), encoding="utf-8"))
            conn.flush()

    def handle_single_device_assign(self, group: int, mac: str, uuid_val: int):
        if group is None:
            raise Exception(
                'producer: Cannot assign infra to group without group id specified!')

        if mac is None:
            raise Exception(
                'producer: Cannot assign infra to group without infra MAC specified!')

        mac_range = MacRange(mac)

        self.conn.send(self.topic, self.message.add_dev_to_group(group, mac_range, uuid_val),
                       bytes(str(group), encoding="utf-8"))
        self.conn.flush()

    def handle_single_device_deassign(self, group: int, mac: str, uuid_val: int):
        if group is None:
            raise Exception(
                'Cannot deassign infra from group without group id specified!')

        if mac is None:
            raise Exception(
                'Cannot deassign infra from group without infra MAC specified!')

        mac_range = MacRange(mac)

        self.conn.send(self.topic, self.message.remove_dev_from_group(group, mac_range, uuid_val),
                       bytes(str(group), encoding="utf-8"))
        self.conn.flush()

    def handle_multiple_devices_assign(self, group: int, mac_list: list, uuid_val: int):
        if group is None:
            raise Exception(
                'producer: Cannot assign infra to group without group id specified!')

        if mac_list is None:
            raise Exception(
                'producer: Cannot assign infra to group without infra MAC list specified!')

        self.conn.send(self.topic, self.message.add_devices_to_group(group, mac_list, uuid_val),
                       bytes(str(group), encoding="utf-8"))
        self.conn.flush()

    def handle_multiple_devices_deassign(self, group: int, mac_list: list, uuid_val: int):
        if group is None:
            raise Exception(
                'Cannot deassign infra from group without group id specified!')

        if mac_list is None:
            raise Exception(
                'Cannot deassign infra from group without infra MAC list specified!')

        self.conn.send(self.topic, self.message.remove_dev_from_group(group, mac_list, uuid_val),
                       bytes(str(group), encoding="utf-8"))
        self.conn.flush()

    def handle_device_assignment(self, add: List[Tuple[int, MacRange]], remove: List[Tuple[int, MacRange]]) -> None:
        with self as conn:
            for group, mac_range in add:
                logger.debug(f"{group = }, {mac_range = }")
                conn.send(self.topic, self.message.add_dev_to_group(group, mac_range),
                          bytes(str(group), encoding="utf-8"))
            for group, mac_range in remove:
                conn.send(self.topic, self.message.remove_dev_from_group(group, mac_range),
                          bytes(str(group), encoding="utf-8"))
            conn.flush()

    def handle_single_device_message(self, message: dict, group: int, mac: str, uuid_val: int) -> None:
        self.conn.send(self.topic, self.message.to_device(group, mac, message, 0, uuid_val),
                       bytes(str(group), encoding="utf-8"))
        self.conn.flush()

    def handle_device_messages(self, message: dict, group: int, mac_range: MacRange,
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
                              bytes(str(group), encoding="utf-8"))
                conn.flush()
                # time.sleep(interval_s)
                # if time.time() > end:
                #    break

    def handle_cloud_header(self, group_header: List[int], infras_header: List[Tuple[int, MacRange]]) -> None:
        with self as conn:
            for group in group_header:
                conn.send(self.topic, self.message.group_header(group),
                          bytes(str(group), encoding="utf-8"))
            for group, infra in infras_header:
                conn.send(self.topic, self.message.infras_header(group, infra),
                          bytes(str(group), encoding="utf-8"))
            conn.flush()
