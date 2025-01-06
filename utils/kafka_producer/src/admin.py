from kafka import KafkaAdminClient
from .utils import Message

__all__ = ['Message']


class Admin:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.connection = None

    def connect(self):
        """Connect to the Kafka."""
        try:
            self.connection = KafkaAdminClient(
                bootstrap_servers=f'{self.host}:{self.port}')
            print("Connection successful")
        except:
            print("Error: Unable to connect to the kafka.")

    def disconnect(self) -> None:
        """Close the Kafka connection."""

        if self.is_connected() is False:
            return
        self.connection.close()
        self.connection = None
        print("admin: disconnected from kafka")

    def is_connected(self) -> bool:
        """Check if the Kafka connection established."""
        return self.connection is not None

    def get_topic_partitions_for_cgw_id(self, topic: str, group: list, cgw_id: int) -> list:
        """Returns list of partitions assigned to specific CGW shard ID for specific topic."""
        partitions_list = []

        description = self.connection.describe_consumer_groups(group)

        for group_info in description:
            for member in group_info.members:
                if member.client_id == f'CGW{cgw_id}':
                    partitions = member.member_assignment.partitions()

                    for partition in partitions:
                        if partition.topic == topic:
                            part_id = partition.partition
                            partitions_list.append(part_id)

        return partitions_list
