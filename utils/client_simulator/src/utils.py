from dataclasses import dataclass
from typing import List
import argparse
import random
import json
import re
import os


TEMPLATE_LOCATION = "./data/message_templates.json"


@dataclass
class Args:
    number_of_connections: int
    masks: List[str]
    ca_path: str
    cert_path: str
    msg_size: int
    msg_interval: int
    wait_for_sig: bool
    server_proto: str = "ws"
    server_address: str = "localhost"
    server_port: int = 50001

    @property
    def server(self):
        return f"{self.server_proto}://{self.server_address}:{self.server_port}"


def parse_msg_size(input: str) -> int:
    match = re.match(r"^(\d+)([kKmM]?)$", input)
    if match is None:
        raise ValueError(f"Unable to parse message size \"{input}\"")
    num, prefix = match.groups()
    num = int(num)
    if prefix and prefix in "kK":
        num *= 1000
    elif prefix and prefix in "mM":
        num *= 1000000
    return num


def parse_args():
    parser = argparse.ArgumentParser(
        description="Used to simulate multiple clients that connect to a single server.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-s", "--server", metavar="ADDRESS", required=True,
                        default="ws://localhost:50001",
                        help="server address")
    parser.add_argument("-N", "--number-of-connections", metavar="NUMBER", type=int,
                        default=1,
                        help="number of concurrent connections per thread pool")
    parser.add_argument("-M", "--mac-mask", metavar="XX:XX:XX:XX:XX:XX", action="append",
                        default=[],
                        help="the mask determines what MAC addresses will be used by clients "
                             "in the thread pool. Specifying multiple masks will increase "
                             "the number of thread pools used.")
    parser.add_argument("-a", "--ca-cert", metavar="CERT",
                        default="./certs/ca/ca.crt",
                        help="path to CA certificate")
    parser.add_argument("-c", "--client-certs-path", metavar="PATH",
                        default="./certs/client",
                        help="path to client certificates directory")
    parser.add_argument("-t", "--msg-interval", metavar="SECONDS", type=int,
                        default=10,
                        help="time between client messages to gw")
    parser.add_argument("-p", "--payload-size", metavar="SIZE", type=str,
                        default="1k",
                        help="size of each client message")
    parser.add_argument("-w", "--wait-for-signal", action="store_true",
                        help="wait for SIGUSR1 before running simulation")

    parsed_args = parser.parse_args()

    args = Args(number_of_connections=parsed_args.number_of_connections,
                masks=parsed_args.mac_mask,
                ca_path=parsed_args.ca_cert,
                cert_path=parsed_args.client_certs_path,
                msg_interval=parsed_args.msg_interval,
                msg_size=parse_msg_size(parsed_args.payload_size),
                wait_for_sig=parsed_args.wait_for_signal)

    if len(args.masks) == 0:
        args.masks.append("XX:XX:XX:XX:XX:XX")

    # PROTO :// ADDRESS : PORT
    match = re.match(r"(?:(wss?)://)?([\d\w\.]+):?(\d+)?", parsed_args.server)
    if match is None:
        raise ValueError(f"Unable to parse server address {parsed_args.server}")
    proto, addr, port = match.groups()
    if proto is not None:
        args.server_proto = proto
    if addr is not None:
        args.server_address = addr
    if port is not None:
        args.server_port = port

    return args


def rand_mac(mask="02:xx:xx:xx:xx:xx"):
    return ''.join([n.lower().replace('x', f'{random.randint(0, 15):x}') for n in mask])


def get_msg_templates():
    with open(TEMPLATE_LOCATION, "r") as templates:
        return json.loads(templates.read())


def gen_certificates(mask: str, count=int):
    cwd = os.getcwd()
    os.chdir("../cert_generator")
    try:
        rc = os.system(f"./generate_certs.sh -c {count} -m \"{mask}\" -o")
        assert rc == 0, "Generating certificates failed"
    finally:
        os.chdir(cwd)
