from .utils import MacRange, Args
import argparse
import json


def time(input: str) -> float:
    """
    Expects a string in the format:
    *number*
    *number*s
    *number*m
    *number*h

    Returns the number of seconds as an integer. Example:
    100 -> 100
    100s -> 100
    5m -> 300
    2h -> 7200
    """
    if all(char not in input for char in "smh"):
        return float(input)
    n, t = float(input[:-1]), input[-1:]
    if t == "h":
        return n * 60 * 60
    if t == "m":
        return n * 60
    return n


def parse_args():
    parser = argparse.ArgumentParser(description="Creates entries in kafka.")

    parser.add_argument("-g", "--new-group", metavar=("GROUP-ID"),
                        nargs=1, action="append",
                        help="create a new group")
    parser.add_argument("-G", "--rm-group", metavar=("GROUP-ID"),
                        nargs=1, action="append",
                        help="delete an existing group")
    parser.add_argument("-d", "--assign-to-group", metavar=("GROUP-ID", "MAC-RANGE"),
                        nargs=2, action="append",
                        help="add a range of mac address to a group")
    parser.add_argument("-D", "--remove-from-group", metavar=("GROUP-ID", "MAC-RANGE"),
                        nargs=2, action="append",
                        help="remove mac address from a group")
    parser.add_argument("-T", "--topic", default="cnc",
                        help="kafka topic (default: \"cnc\")")
    parser.add_argument("-s", "--bootstrap-server", metavar="ADDRESS", default="127.0.0.1:9092",
                        help="kafka address (default: \"127.0.0.1:9092\")")
    parser.add_argument("-m", "--send-message", metavar="JSON", type=str,
                        help="this message will be sent down from the GW to all devices "
                             "specified in the --send-to-mac")
    parser.add_argument("-c", "--send-count", metavar="COUNT", type=int,
                        help="how many messages will be sent (per mac address, default: 1)")
    parser.add_argument("-t", "--send-for", metavar="TIME", type=time,
                        help="how long to send the messages for")
    parser.add_argument("-i", "--send-interval", metavar="INTERVAL", type=time, default="1",
                        help="time between messages (default: \"1.0s\")")
    parser.add_argument("-p", "--send-to-group", metavar="GROUP-ID", type=str)
    parser.add_argument("-r", "--send-to-mac", metavar="MAC-RANGE", type=MacRange,
                        help="range of mac address that will be receiving the messages")
    parser.add_argument("-gh", "--group-header", metavar=("GROUP-ID"),
                        nargs=1, action="append",
                        help="set group header")
    parser.add_argument("-ih", "--infras-header", metavar=("GROUP-ID", "MAC-RANGE"),
                        nargs=2, action="append",
                        help="set group infras header")
    parser.add_argument("-tg", "--topomap-generate-timeout", metavar=("GROUP-ID, TIMEOUT"),
                        nargs=2, action="append",
                        help="set topomap generate timeout")

    parsed_args = parser.parse_args()

    if parsed_args.send_message is not None and (
        parsed_args.send_to_group is None or
        parsed_args.send_to_mac is None
    ):
        parser.error(
            "--send-message requires --send-to-group and --send-to-mac")

    message = None
    if parsed_args.send_message is not None:
        try:
            message = json.loads(parsed_args.send_message)
        except json.JSONDecodeError:
            parser.error("--send-message must be in JSON format")

    count = 1
    if parsed_args.send_count is not None:
        count = parsed_args.send_count
    elif parsed_args.send_for is not None:
        count = 0

    args = Args(
        [], [], [], [],
        topic=parsed_args.topic,
        db=parsed_args.bootstrap_server,
        message=message,
        count=count,
        time_to_send_s=parsed_args.send_for,
        interval_s=parsed_args.send_interval,
        group_id=parsed_args.send_to_group,
        send_to_macs=parsed_args.send_to_mac,
        header_group=[],
        header_infras=[],
        topomap_timeout=[]
    )
    if parsed_args.new_group is not None:
        for (group,) in parsed_args.new_group:
            try:
                args.add_groups.append(group)
            except ValueError:
                parser.error(f"--new-group: failed to parse {group}")
    if parsed_args.rm_group is not None:
        for (group,) in parsed_args.rm_group:
            args.del_groups.append(group)
    if parsed_args.assign_to_group is not None:
        try:
            for group, mac in parsed_args.assign_to_group:
                args.assign_to_group.append((group, MacRange(mac)))
        except ValueError:
            parser.error(
                f"--assign-to-group: failed to parse MAC range \"{mac}\"")
    if parsed_args.remove_from_group is not None:
        try:
            for group, mac in parsed_args.remove_from_group:
                args.remove_from_group.append((group, MacRange(mac)))
        except ValueError:
            parser.error(
                f"--remove-from-group: failed to parse MAC range \"{mac}\"")
    if parsed_args.group_header is not None:
        for (group,) in parsed_args.group_header:
            try:
                args.header_group.append(group)
            except ValueError:
                parser.error(f"--group-header: failed to parse {group}")
    if parsed_args.infras_header is not None:
        try:
            for group, mac in parsed_args.infras_header:
                args.header_infras.append((group, MacRange(mac)))
        except ValueError:
            parser.error(
                f"--infras-header: failed to parse MAC range \"{mac}\"")
    if parsed_args.topomap_generate_timeout is not None:
        for (group, timeout_value) in parsed_args.topomap_generate_timeout:
            try:
                args.topomap_timeout.append((group, timeout_value))
            except ValueError:
                parser.error(f"--topomap-generate-timeout: failed to parse {group}")

    return args
