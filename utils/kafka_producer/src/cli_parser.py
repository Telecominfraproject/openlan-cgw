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

    parser.add_argument("-g", "--new-group", metavar=("GROUP-ID", "SHARD-ID", "NAME"),
                        nargs=3, action="append",
                        help="create a new group")
    parser.add_argument("-G", "--rm-group", metavar=("GROUP-ID"),
                        nargs=1, action="append",
                        help="delete an existing group")
    parser.add_argument("-d", "--assign-to-group", metavar=("GROUP-ID", "MAC-RANGE"),
                        nargs=2, action="append",
                        help="add a range of mac addrs to a group")
    parser.add_argument("-D", "--remove-from-group", metavar=("GROUP-ID", "MAC-RANGE"),
                        nargs=2, action="append",
                        help="remove mac addrs from a group")
    parser.add_argument("-T", "--topic", default="CnC",
                        help="kafka topic (default: \"CnC\")")
    parser.add_argument("-s", "--bootstrap-server", metavar="ADDRESS", default="172.20.10.136:9092",
                        help="kafka address (default: \"172.20.10.136:9092\")")
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
                        help="range of mac addrs that will be receiving the messages")

    parsed_args = parser.parse_args()

    if parsed_args.send_message is not None and (
        parsed_args.send_to_group is None or
        parsed_args.send_to_mac is None
    ):
        parser.error("--send-message requires --send-to-group and --send-to-mac")

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
    )
    if parsed_args.new_group is not None:
        for group, shard, name in parsed_args.new_group:
            try:
                args.add_groups.append((group, int(shard), name))
            except ValueError:
                parser.error(f"--new-group: failed to parse shard id \"{shard}\"")
    if parsed_args.rm_group is not None:
        for (group,) in parsed_args.rm_group:
            args.del_groups.append(group)
    if parsed_args.assign_to_group is not None:
        try:
            for group, mac in parsed_args.assign_to_group:
                args.assign_to_group.append((group, MacRange(mac)))
        except ValueError:
            parser.error(f"--assign-to-group: failed to parse MAC range \"{mac}\"")
    if parsed_args.remove_from_group is not None:
        try:
            for group, mac in parsed_args.remove_from_group:
                args.remove_from_group.append((group, MacRange(mac)))
        except ValueError:
            parser.error(f"--remove-from-group: failed to parse MAC range \"{mac}\"")

    return args
