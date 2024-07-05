#!/usr/bin/env python3
from .utils import get_msg_templates, Args
from .log import logger
from websockets.sync import client
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError, ConnectionClosed
from typing import List
import multiprocessing
import threading
import resource
import string
import random
import signal
import copy
import time
import json
import ssl
import os
import re


class Message:
    def __init__(self, mac: str, size: int):
        self.templates = get_msg_templates()
        self.connect = json.dumps(self.templates["connect"]).replace("MAC", mac)
        self.state = json.dumps(self.templates["state"]).replace("MAC", mac)
        self.reboot_response = json.dumps(self.templates["reboot_response"]).replace("MAC", mac)
        self.log = copy.deepcopy(self.templates["log"])
        self.log["params"]["data"] = {"msg": ''.join(random.choices(string.ascii_uppercase + string.digits, k=size))}
        self.log = json.dumps(self.log).replace("MAC", mac)

    @staticmethod
    def to_json(msg) -> str:
        return json.dumps(msg)

    @staticmethod
    def from_json(msg) -> dict:
        return json.loads(msg)


class Device:
    def __init__(self, mac: str, server: str, ca_cert: str,
                 msg_interval: int, msg_size: int,
                 client_cert: str, client_key: str, check_cert: bool,
                 start_event: multiprocessing.Event,
                 stop_event: multiprocessing.Event):
        self.mac = mac
        self.interval = msg_interval
        self.messages = Message(self.mac, msg_size)
        self.server_addr = server
        self.start_event = start_event
        self.stop_event = stop_event
        self.reboot_time_s = 10
        self._socket = None

        self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.ssl_context.load_cert_chain(client_cert, client_key, "")
        self.ssl_context.load_verify_locations(ca_cert)
        if check_cert:
            self.ssl_context.verify_mode = ssl.CERT_REQUIRED
        else:
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = ssl.CERT_NONE

    def send_hello(self, socket: client.ClientConnection):
        logger.debug(self.messages.connect)
        socket.send(self.messages.connect)

    def send_log(self, socket: client.ClientConnection):
        socket.send(self.messages.log)

    def handle_messages(self, socket: client.ClientConnection):
        try:
            msg = socket.recv(self.interval)
            msg = self.messages.from_json(msg)
            logger.info(msg)
            if msg["method"] == "reboot":
                self.handle_reboot(socket, msg)
            else:
                logger.error(f"Unknown method {msg['method']}")
        except TimeoutError:  # no messages
            pass
        except (ConnectionClosedOK, ConnectionClosedError, ConnectionClosed):
            logger.critical("Did not expect socket to be closed")
            raise

    def handle_reboot(self, socket: client.ClientConnection, msg: dict):
        resp = self.messages.from_json(self.messages.reboot_response)
        if "id" in msg:
            resp["result"]["id"] = msg["id"]
        else:
            del resp["result"]["id"]
            logger.warn("Reboot request is missing 'id' field")
        socket.send(self.messages.to_json(resp))
        self.disconnect()
        time.sleep(self.reboot_time_s)
        self.connect()
        self.send_hello(self._socket)

    def connect(self):
        if self._socket is None:
            self._socket = client.connect(self.server_addr, ssl_context=self.ssl_context, open_timeout=7200)
        return self._socket

    def disconnect(self):
        if self._socket is not None:
            self._socket.close()
            self._socket = None

    def single_run(self):
        logger.debug("starting simulation")
        self.connect()
        start = time.time()
        try:
            self.send_hello(self._socket)
            while True:
                if self._socket is None:
                    logger.error("Connection to GW is lost. Trying to reconnect...")
                    self.connect()
                if time.time() - start > self.interval:
                    logger.info(f"Sent log")
                    self.send_log(self._socket)
                    start = time.time()
                self.handle_messages(self._socket)
        finally:
            self.disconnect()
        logger.debug("simulation done")

    def job(self):
        logger.debug("waiting for start trigger")
        self.start_event.wait()
        if self.stop_event.is_set():
            return
        logger.debug("starting simulation")
        self.connect()
        start = time.time()
        try:
            self.send_hello(self._socket)
            while not self.stop_event.is_set():
                if self._socket is None:
                    logger.error("Connection to GW is lost. Trying to reconnect...")
                    self.connect()
                if time.time() - start > self.interval:
                    logger.info(f"Sent log")
                    self.send_log(self._socket)
                    start = time.time()
                self.handle_messages(self._socket)
        finally:
            self.disconnect()
        logger.debug("simulation done")


def get_avail_mac_addrs(path, mask="XX:XX:XX:XX:XX:XX"):
    _mask = "".join(("[0-9a-fA-F]" if c == "X" else c) for c in mask.upper())
    certs = sorted(os.listdir(path))
    macs = set(cert.split(".")[0] for cert in certs if "crt" in cert and re.match(_mask, cert))
    return list(macs)


def update_fd_limit():
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except ValueError:
        logger.critical("Failed to update fd limit")
        raise
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    logger.warning(f"changed fd limit {soft, hard}")


def process(args: Args, mask: str, start_event: multiprocessing.Event, stop_event: multiprocessing.Event):
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # ignore Ctrl+C in child processes
    threading.current_thread().name = mask
    logger.info(f"process started")
    macs = get_avail_mac_addrs(args.cert_path, mask)
    if len(macs) < args.number_of_connections:
        logger.warn(f"expected {args.number_of_connections} certificates, but only found {len(macs)} "
                    f"({mask = })")
    update_fd_limit()

    devices = [Device(mac, args.server, args.ca_path, args.msg_interval, args.msg_size,
                      os.path.join(args.cert_path, f"{mac}.crt"),
                      os.path.join(args.cert_path, f"{mac}.key"),
                      args.check_cert,
                      start_event, stop_event)
               for mac, _ in zip(macs, range(args.number_of_connections))]
    threads = [threading.Thread(target=d.job, name=d.mac) for d in devices]
    [t.start() for t in threads]
    [t.join() for t in threads]


def verify_cert_availability(cert_path: str, masks: List[str], count: int):
    for mask in masks:
        macs = get_avail_mac_addrs(cert_path, mask)
        assert len(macs) >= count, \
            f"Simulation requires {count} certificates, but only found {len(macs)}"


def trigger_start(evt):
    def fn(signum, frame):
        logger.info("Signal received, starting simulation...")
        evt.set()
    return fn


def main(args: Args):
    verify_cert_availability(args.cert_path, args.masks, args.number_of_connections)
    stop_event = multiprocessing.Event()
    start_event = multiprocessing.Event()
    if not args.wait_for_sig:
        start_event.set()
    signal.signal(signal.SIGUSR1, trigger_start(start_event))
    processes = [multiprocessing.Process(target=process, args=(args, mask, start_event, stop_event))
                 for mask in args.masks]
    try:
        for p in processes:
            p.start()
        time.sleep(1)
        logger.info(f"Started {len(processes)} processes")
        if args.wait_for_sig:
            logger.info("Waiting for SIGUSR1...")
        while True:
            time.sleep(100)
    except KeyboardInterrupt:
        logger.warn("Stopping all processes...")
        stop_event.set()
        start_event.set()
        [p.join() for p in processes]
