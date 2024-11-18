import pytest
import uuid
import random

from metrics import cgw_metrics_get_active_shards_num, \
    cgw_metrics_get_groups_assigned_num


class TestCgwInfraGroup:
    @pytest.mark.usefixtures("test_context",
                            "cgw_probe",
                            "kafka_probe")
    def test_single_infra_group_add_del(self, test_context):
        assert test_context.kafka_producer.is_connected(),\
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(),\
            f'Cannot create default group: kafka consumer is not connected to Kafka'

        assert cgw_metrics_get_active_shards_num() == 1

        uuid_val = uuid.uuid4()
        group_id = 100

        # Create single group
        test_context.kafka_producer.handle_single_group_create(str(group_id), uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive create group result when expected')

        assert (ret_msg.value['type'] == 'infrastructure_group_create_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group creation failed!')

        assert cgw_metrics_get_groups_assigned_num() == 1

        # Delete single group
        uuid_val = uuid.uuid4()

        test_context.kafka_producer.handle_single_group_delete(str(group_id), uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] == 'infrastructure_group_delete_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group creation failed!')

        assert cgw_metrics_get_groups_assigned_num() == 0


    @pytest.mark.usefixtures("test_context",
                            "cgw_probe",
                            "kafka_probe")
    def test_multiple_infra_group_add_del(self, test_context):
        assert test_context.kafka_producer.is_connected(),\
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(),\
            f'Cannot create default group: kafka consumer is not connected to Kafka'

        assert cgw_metrics_get_active_shards_num() == 1

        groups_num = random.randint(1, 10)

        for group in range(0, groups_num):
            uuid_val = uuid.uuid4()
            group_id = (100 + group)

            # Create single group
            test_context.kafka_producer.handle_single_group_create(str(group_id), uuid_val.int)
            ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
            if not ret_msg:
                print('Failed to receive create group result, was expecting ' + str(uuid_val.int) + ' uuid reply')
                raise Exception('Failed to receive create group result when expected')

            assert (ret_msg.value['type'] == 'infrastructure_group_create_response')
            assert (int(ret_msg.value['infra_group_id']) == group_id)
            assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

            if ret_msg.value['success'] is False:
                print(ret_msg.value['error_message'])
                raise Exception('Infra group creation failed!')

            assert cgw_metrics_get_groups_assigned_num() == (group + 1)

        for group in range(0, groups_num):
            # Delete single group
            uuid_val = uuid.uuid4()
            group_id = (100 + group)

            test_context.kafka_producer.handle_single_group_delete(str(group_id), uuid_val.int)
            ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
            if not ret_msg:
                print('Failed to receive delete group result, was expecting ' + str(uuid_val.int) + ' uuid reply')
                raise Exception('Failed to receive delete group result when expected')

            assert (ret_msg.value['type'] == 'infrastructure_group_delete_response')
            assert (int(ret_msg.value['infra_group_id']) == group_id)
            assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

            if ret_msg.value['success'] is False:
                print(ret_msg.value['error_message'])
                raise Exception('Infra group creation failed!')

            assert cgw_metrics_get_groups_assigned_num() == (groups_num - (group + 1))


    @pytest.mark.usefixtures("test_context",
                            "cgw_probe",
                            "kafka_probe")
    def test_create_existing_infra_group(self, test_context):
        assert test_context.kafka_producer.is_connected(),\
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(),\
            f'Cannot create default group: kafka consumer is not connected to Kafka'

        assert cgw_metrics_get_active_shards_num() == 1

        uuid_val = uuid.uuid4()
        group_id = 100

        # Create single group
        test_context.kafka_producer.handle_single_group_create(str(group_id), uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive create group result when expected')

        assert (ret_msg.value['type'] == 'infrastructure_group_create_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group creation failed!')

        assert cgw_metrics_get_groups_assigned_num() == 1

        # Try to create the same group
        test_context.kafka_producer.handle_single_group_create(str(group_id), uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive create group result when expected')

        assert (ret_msg.value['type'] == 'infrastructure_group_create_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        # Expected request to be failed
        if ret_msg.value['success'] is True:
            raise Exception('Infra group creation completed, while expected to be failed!')

        assert cgw_metrics_get_groups_assigned_num() == 1

        # Delete single group
        uuid_val = uuid.uuid4()

        test_context.kafka_producer.handle_single_group_delete(str(group_id), uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] == 'infrastructure_group_delete_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group creation failed!')

        assert cgw_metrics_get_groups_assigned_num() == 0


    @pytest.mark.usefixtures("test_context",
                            "cgw_probe",
                            "kafka_probe")
    def test_remove_not_existing_infra_group(self, test_context):
        assert test_context.kafka_producer.is_connected(),\
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(),\
            f'Cannot create default group: kafka consumer is not connected to Kafka'

        assert cgw_metrics_get_active_shards_num() == 1

        group_id = 100
        uuid_val = uuid.uuid4()

        test_context.kafka_producer.handle_single_group_delete(str(group_id), uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] == 'infrastructure_group_delete_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is True:
            raise Exception('Infra group deletion completed, while expected to be failed!')

        assert cgw_metrics_get_groups_assigned_num() == 0


    @pytest.mark.usefixtures("test_context",
                            "cgw_probe",
                            "kafka_probe")
    def test_single_infra_group_add_del_to_shard(self, test_context):
        assert test_context.kafka_producer.is_connected(),\
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(),\
            f'Cannot create default group: kafka consumer is not connected to Kafka'

        assert cgw_metrics_get_active_shards_num() == 1

        uuid_val = uuid.uuid4()
        group_id = 100
        shard_id = 0

        # Create single group
        test_context.kafka_producer.handle_single_group_create_to_shard(str(group_id), shard_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive create group result when expected')

        assert (ret_msg.value['type'] == 'infrastructure_group_create_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group creation failed!')

        assert cgw_metrics_get_groups_assigned_num() == 1

        # Delete single group
        uuid_val = uuid.uuid4()

        test_context.kafka_producer.handle_single_group_delete(str(group_id), uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] == 'infrastructure_group_delete_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group creation failed!')

        assert cgw_metrics_get_groups_assigned_num() == 0


    @pytest.mark.usefixtures("test_context",
                            "cgw_probe",
                            "kafka_probe")
    def test_single_infra_group_add_to_not_existing_shard(self, test_context):
        assert test_context.kafka_producer.is_connected(),\
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(),\
            f'Cannot create default group: kafka consumer is not connected to Kafka'

        assert cgw_metrics_get_active_shards_num() == 1

        uuid_val = uuid.uuid4()
        group_id = 100
        shard_id = 2

        # Create single group
        test_context.kafka_producer.handle_single_group_create_to_shard(str(group_id), shard_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive create group result when expected')

        assert (ret_msg.value['type'] == 'infrastructure_group_create_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is True:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group creation completed, while expected to be failed!')

        assert cgw_metrics_get_groups_assigned_num() == 0
