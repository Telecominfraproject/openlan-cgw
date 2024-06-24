#!/usr/bin/env python3
from dataclasses import dataclass
from typing import List
import argparse
import random
import json
import re
import os

from src.simulation_runner import Device


@dataclass
class Args:
    mac: str
    ca_path: str
    cert_path: str
    msg_size: int
    msg_interval: int
    server_proto: str = "ws"
    server_address: str = "localhost"
    server_port: int = 15002
    cert_check: bool = True

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
        description="Used to simulate a single client that connects to a single server.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-s", "--server", metavar="ADDRESS", required=True,
                        default="ws://localhost:15002",
                        help="server address")
    parser.add_argument("-m", "--mac", metavar="XX:XX:XX:XX:XX:XX", required=True,
                        default="",
                        help="the mask determines what MAC addresses will be used by the client.")
    parser.add_argument("-a", "--ca-cert", metavar="CERT",
                        default="./certs/ca/ca.crt",
                        help="path to CA certificate")
    parser.add_argument("-c", "--client-certs-path", metavar="PATH",
                        default="./certs/client",
                        help="path to client certificates directory")
    parser.add_argument("-C", "--no-cert-check", action='store_true',
                        default=False,
                        help="do not check certificate")
    parser.add_argument("-t", "--msg-interval", metavar="SECONDS", type=int,
                        default=10,
                        help="time between client messages to gw")
    parser.add_argument("-p", "--payload-size", metavar="SIZE", type=str,
                        default="1k",
                        help="size of each client message")

    parsed_args = parser.parse_args()

    args = Args(mac=parsed_args.mac,
                ca_path=parsed_args.ca_cert,
                cert_path=parsed_args.client_certs_path,
                msg_interval=parsed_args.msg_interval,
                msg_size=parse_msg_size(parsed_args.payload_size),
                cert_check=not parsed_args.no_cert_check
                )

    # PROTO :// ADDRESS : PORT
    # TODO: fixme the host portion can contain a lot more than just these characters!
    match = re.match(r"(?:(wss?)://)?([\d\w\.-]+):?(\d+)?", parsed_args.server)
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


def main(args):
    mac = args.mac
    device = Device(mac, args.server, args.ca_path, args.msg_interval, args.msg_size,
                      os.path.join(args.cert_path, f"{mac}.crt"),
                      os.path.join(args.cert_path, f"{mac}.key"),
                      args.cert_check,
                      None, None)
    print(f"Server: {device.server_addr}")
    print(f"MAC:    {mac}")
    device.single_run()


if __name__ == "__main__":
    args = parse_args()
    main(args)
