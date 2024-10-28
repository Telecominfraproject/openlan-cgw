from dataclasses import dataclass
from typing import List, Tuple
from typing import Tuple
import copy
import json
import uuid


class MacRange:
    """
    Return an object that produces a sequence of MAC addresses from
    START (inclusive) to END (inclusive). START and END are exctracted
    from the input string if it is in the format
    "11:22:AA:BB:00:00-11:22:AA:BB:00:05" (where START=11:22:AA:BB:00:00,
    END=11:22:AA:BB:00:05, and the total amount of MACs in the range is 6).

    Examples (all of these are identical):

    00:00:00:00:XX:XX
    00:00:00:00:00:00-00:00:00:00:FF:FF
    00:00:00:00:00:00^65536

    Raises ValueError
    """
    def __init__(self, input: str = "XX:XX:XX:XX:XX:XX") -> None:
        self.__base_as_num, self.__len = self.__parse_input(input.upper())
        self.__idx = 0

    def __iter__(self):
        return self

    def __next__(self) -> str:
        if self.__idx >= len(self):
            self.__idx = 0
            raise StopIteration()
        mac = self.num2mac(self.__base_as_num + self.__idx)
        self.__idx += 1
        return mac

    def __len__(self) -> int:
        return self.__len

    def __str__(self) -> str:
        return f"MacRange[start={self.base}, " \
               f"end={self.num2mac(self.__base_as_num + len(self) - 1)}]"

    def __repr__(self) -> str:
        return f"MacRange('{self.base}^{len(self)}')"

    @property
    def base(self) -> str:
        return self.num2mac(self.__base_as_num)

    @staticmethod
    def mac2num(mac: str) -> int:
        return int(mac.replace(":", ""), base=16)

    @staticmethod
    def num2mac(mac: int) -> str:
        hex = f"{mac:012X}"
        return ":".join([a+b for a, b in zip(hex[::2], hex[1::2])])

    def __parse_input(self, input: str) -> Tuple[int, int]:
        if "X" in input:
            string = f"{input.replace('X', '0')}-{input.replace('X', 'F')}"
        else:
            string = input
        if "-" in string:
            start, end = string.split("-")
            start, end = self.mac2num(start), self.mac2num(end)
            if start > end:
                raise ValueError(f"Invalid MAC range {start}-{end}")
            return start, end - start + 1
        if "^" in string:
            base, count = string.split("^")
            return self.mac2num(base), int(count)
        return self.mac2num(input), 1


class Message:
    TEMPLATE_FILE = "./data/message_template.json"
    GROUP_ADD = "add_group"
    GROUP_DEL = "del_group"
    DEV_TO_GROUP = "add_to_group"
    DEV_FROM_GROUP = "del_from_group"
    TO_DEVICE = "message_infra"
    GROUP_ID = "infra_group_id"
    GROUP_NAME = "infra_name"
    SHARD_ID = "infra_shard_id"
    DEV_LIST = "infra_group_infras"
    MAC = "mac"
    DATA = "msg"
    MSG_UUID = "uuid"

    def __init__(self) -> None:
        with open(self.TEMPLATE_FILE) as f:
            self.templates = json.loads(f.read())

    def group_create(self, id: str, shard_id: int, name: str) -> bytes:
        msg = copy.copy(self.templates[self.GROUP_ADD])
        msg[self.GROUP_ID] = id
        msg[self.SHARD_ID] = shard_id
        msg[self.GROUP_NAME] = name
        msg[self.MSG_UUID] = str(uuid.uuid1())
        return json.dumps(msg).encode('utf-8')

    def group_delete(self, id: str) -> bytes:
        msg = copy.copy(self.templates[self.GROUP_DEL])
        msg[self.GROUP_ID] = id
        msg[self.MSG_UUID] = str(uuid.uuid1())
        return json.dumps(msg).encode('utf-8')

    def add_dev_to_group(self, id: str, mac_range: MacRange) -> bytes:
        msg = copy.copy(self.templates[self.DEV_TO_GROUP])
        msg[self.GROUP_ID] = id
        msg[self.DEV_LIST] = list(mac_range)
        msg[self.MSG_UUID] = str(uuid.uuid1())
        return json.dumps(msg).encode('utf-8')

    def remove_dev_from_group(self, id: str, mac_range: MacRange) -> bytes:
        msg = copy.copy(self.templates[self.DEV_FROM_GROUP])
        msg[self.GROUP_ID] = id
        msg[self.DEV_LIST] = list(mac_range)
        msg[self.MSG_UUID] = str(uuid.uuid1())
        return json.dumps(msg).encode('utf-8')

    def to_device(self, id: str, mac: str, data, sequence: int = 0):
        msg = copy.copy(self.templates[self.TO_DEVICE])
        msg[self.GROUP_ID] = id
        msg[self.MAC] = mac
        if type(data) is dict:
            msg[self.DATA] = data
        else:
            msg[self.DATA] = {"data": data}
        msg[self.MSG_UUID] = str(uuid.uuid1(node=MacRange.mac2num(mac), clock_seq=sequence))
        return json.dumps(msg).encode('utf-8')


@dataclass
class Args:
    add_groups: List[Tuple[str, int, str]]
    del_groups: List[str]
    assign_to_group: List[Tuple[str, MacRange]]
    remove_from_group: List[Tuple[str, MacRange]]
    topic: str
    db: str
    message: dict
    count: int
    time_to_send_s: float
    interval_s: float
    group_id: int
    send_to_macs: MacRange
