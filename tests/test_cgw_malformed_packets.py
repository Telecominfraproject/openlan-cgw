import pytest
import uuid

from kafka_producer.src.utils import MalformedMessage


class TestCgwMalformedPackets:
    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_malformed_infra_group_add(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'kafka consumer is not connected to Kafka'

        uuid_val = uuid.uuid4()
        expected_uuid = uuid.UUID('00000000-0000-0000-0000-000000000000')
        group_id = 100

        message = MalformedMessage()

        # Create single group
        test_context.kafka_producer.conn.send(test_context.default_producer_topic(),
                                              message.group_create(uuid_val.int), bytes(str(group_id), encoding="utf-8"))
        ret_msg = test_context.kafka_consumer.get_result_msg(expected_uuid.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infra_message_enqueue_response')

        if ret_msg.value['success'] is True:
            raise Exception(
                'Infra group create passed while expected to be failed! Malformed packet was sent!')

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_malformed_infra_group_add_to_shard(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'kafka consumer is not connected to Kafka'

        expected_uuid = uuid.UUID('00000000-0000-0000-0000-000000000000')
        uuid_val = uuid.uuid4()
        group_id = 100

        message = MalformedMessage()
        default_shard_id = test_context.default_shard_id()

        # Create single group
        test_context.kafka_producer.conn.send(test_context.default_producer_topic(),
                                              message.group_create_to_shard(default_shard_id, uuid_val.int), bytes(str(group_id), encoding="utf-8"))
        ret_msg = test_context.kafka_consumer.get_result_msg(expected_uuid.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infra_message_enqueue_response')

        if ret_msg.value['success'] is True:
            raise Exception(
                'Infra group create passed while expected to be failed! Malformed packet was sent!')

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_malformed_infra_group_del(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'kafka consumer is not connected to Kafka'

        expected_uuid = uuid.UUID('00000000-0000-0000-0000-000000000000')
        uuid_val = uuid.uuid4()
        group_id = 100

        message = MalformedMessage()

        # Create single group
        test_context.kafka_producer.conn.send(test_context.default_producer_topic(),
                                              message.group_delete(uuid_val.int), bytes(str(group_id), encoding="utf-8"))
        ret_msg = test_context.kafka_consumer.get_result_msg(expected_uuid.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infra_message_enqueue_response')

        if ret_msg.value['success'] is True:
            raise Exception(
                'Infra group delete passed while expected to be failed! Malformed packet was sent!')

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_malformed_infras_add(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'kafka consumer is not connected to Kafka'

        expected_uuid = uuid.UUID('00000000-0000-0000-0000-000000000000')
        uuid_val = uuid.uuid4()
        group_id = 100

        message = MalformedMessage()
        infra_mac = "11-22-33-44-55-66"

        # Create single group
        test_context.kafka_producer.conn.send(test_context.default_producer_topic(),
                                              message.add_dev_to_group([infra_mac], uuid_val.int), bytes(str(group_id), encoding="utf-8"))
        ret_msg = test_context.kafka_consumer.get_result_msg(expected_uuid.int)
        if not ret_msg:
            print('Failed to receive infas add result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive infas add result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infra_message_enqueue_response')

        if ret_msg.value['success'] is True:
            raise Exception(
                'Infras add passed while expected to be failed! Malformed packet was sent!')

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_malformed_infras_del(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'kafka consumer is not connected to Kafka'

        expected_uuid = uuid.UUID('00000000-0000-0000-0000-000000000000')
        uuid_val = uuid.uuid4()
        group_id = 100

        message = MalformedMessage()
        infra_mac = "11-22-33-44-55-66"

        # Create single group
        test_context.kafka_producer.conn.send(test_context.default_producer_topic(),
                                              message.remove_dev_from_group([infra_mac], uuid_val.int), bytes(str(group_id), encoding="utf-8"))
        ret_msg = test_context.kafka_consumer.get_result_msg(expected_uuid.int)
        if not ret_msg:
            print('Failed to receive infas del result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive infas del result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infra_message_enqueue_response')

        if ret_msg.value['success'] is True:
            raise Exception(
                'Infras del passed while expected to be failed! Malformed packet was sent!')

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_malformed_infra_msg(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'kafka consumer is not connected to Kafka'

        expected_uuid = uuid.UUID('00000000-0000-0000-0000-000000000000')
        uuid_val = uuid.uuid4()
        group_id = 100

        message = MalformedMessage()
        infra_mac = "11-22-33-44-55-66"

        # Create single group
        test_context.kafka_producer.conn.send(test_context.default_producer_topic(),
                                              message.to_device(infra_mac, uuid_val.int), bytes(str(group_id), encoding="utf-8"))
        ret_msg = test_context.kafka_consumer.get_result_msg(expected_uuid.int)
        if not ret_msg:
            print('Failed to receive infa message result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infa message result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infra_message_enqueue_response')

        if ret_msg.value['success'] is True:
            raise Exception(
                'Infra message passed while expected to be failed! Malformed packet was sent!')
