#!/usr/bin/env python3
from src.cli_parser import parse_args, Args
from src.producer import Producer
from src.log import logger


def main(args: Args):
    producer = Producer(args.db, args.topic)
    if args.add_groups or args.del_groups:
        producer.handle_group_creation(args.add_groups, args.del_groups)
    if args.assign_to_group or args.remove_from_group:
        producer.handle_device_assignment(
            args.assign_to_group, args.remove_from_group)
    if args.message:
        producer.handle_device_messages(args.message, args.group_id, args.send_to_macs,
                                        args.count, args.time_to_send_s, args.interval_s)
    if args.header_group or args.header_infras:
        producer.handle_cloud_header(args.header_group, args.header_infras)


if __name__ == "__main__":
    try:
        args = parse_args()
        main(args)
    except KeyboardInterrupt:
        logger.warn("exiting...")
