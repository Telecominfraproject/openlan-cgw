import pytest
import uuid
import random

from metrics import cgw_metrics_get_groups_assigned_num, \
    cgw_metrics_get_groups_capacity, \
    cgw_metrics_get_groups_threshold


class TestCgwInfraGroup:
    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_single_infra_group_add_del(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

        uuid_val = uuid.uuid4()
        group_id = 100

        # Create single group
        test_context.kafka_producer.handle_single_group_create(
            str(group_id), uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group create failed!')

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infra group create failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infra group create failed!')

        # Validate group
        assert group_info_psql[0] == int(
            group_info_redis.get('gid')) == group_id

        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 1

        # Delete single group
        uuid_val = uuid.uuid4()

        test_context.kafka_producer.handle_single_group_delete(
            str(group_id), uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group delete failed!')

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate group removed from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        assert group_info_redis == {}

        # Validate group removed from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        assert group_info_redis == {}

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_multiple_infra_group_add_del(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

        groups_num = random.randint(1, 10)

        for group in range(0, groups_num):
            uuid_val = uuid.uuid4()
            group_id = (100 + group)

            # Create single group
            test_context.kafka_producer.handle_single_group_create(
                str(group_id), uuid_val.int, default_shard_id)
            ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
            if not ret_msg:
                print('Failed to receive create group result, was expecting ' +
                      str(uuid_val.int) + ' uuid reply')
                raise Exception(
                    'Failed to receive create group result when expected')

            assert (ret_msg.value['type'] ==
                    'infrastructure_group_create_response')
            assert (int(ret_msg.value['infra_group_id']) == group_id)
            assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

            if ret_msg.value['success'] is False:
                print(ret_msg.value['error_message'])
                raise Exception('Infra group create failed!')

            # Get group info from Redis
            group_info_redis = test_context.redis_client.get_infrastructure_group(
                group_id)
            if not group_info_redis:
                print(f'Failed to get group {group_id} info from Redis!')
                raise Exception('Infra group create failed!')

            # Get group info from PSQL
            group_info_psql = test_context.psql_client.get_infrastructure_group(
                group_id)
            if not group_info_psql:
                print(f'Failed to get group {group_id} info from PSQL!')
                raise Exception('Infra group create failed!')

            # Validate group
            assert group_info_psql[0] == int(
                group_info_redis.get('gid')) == group_id

            shard_info = test_context.redis_client.get_shard(default_shard_id)
            if not shard_info:
                print(
                    f'Failed to get shard {default_shard_id} info from Redis!')
                raise Exception(
                    f'Failed to get shard {default_shard_id} info from Redis!!')

            # Validate number of assigned groups
            assert int(shard_info.get('assigned_groups_num')
                       ) == cgw_metrics_get_groups_assigned_num() == (group + 1)

        # Make sure assigned groups number from CGW side is expected
        assert cgw_metrics_get_groups_assigned_num() == groups_num

        for group in range(0, groups_num):
            # Delete single group
            uuid_val = uuid.uuid4()
            group_id = (100 + group)

            test_context.kafka_producer.handle_single_group_delete(
                str(group_id), uuid_val.int)
            ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
            if not ret_msg:
                print('Failed to receive delete group result, was expecting ' +
                      str(uuid_val.int) + ' uuid reply')
                raise Exception(
                    'Failed to receive delete group result when expected')

            assert (ret_msg.value['type'] ==
                    'infrastructure_group_delete_response')
            assert (int(ret_msg.value['infra_group_id']) == group_id)
            assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

            if ret_msg.value['success'] is False:
                print(ret_msg.value['error_message'])
                raise Exception('Infra group delete failed!')

            # Get shard info from Redis
            shard_info = test_context.redis_client.get_shard(default_shard_id)
            if not shard_info:
                print(
                    f'Failed to get shard {default_shard_id} info from Redis!')
                raise Exception(
                    f'Failed to get shard {default_shard_id} info from Redis!')

            # Validate group removed from Redis
            group_info_redis = test_context.redis_client.get_infrastructure_group(
                group_id)
            assert group_info_redis == {}

            # Validate group removed from PSQL
            group_info_psql = test_context.psql_client.get_infrastructure_group(
                group_id)
            assert group_info_redis == {}

            # Validate number of assigned groups
            assert int(shard_info.get('assigned_groups_num')) == cgw_metrics_get_groups_assigned_num(
            ) == (groups_num - (group + 1))

        # Make sure after clean-up assigned group num is zero
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_create_existing_infra_group(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

        uuid_val = uuid.uuid4()
        group_id = 100

        # Create single group
        test_context.kafka_producer.handle_single_group_create(
            str(group_id), uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group create failed!')

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infra group create failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infra group create failed!')

        # Validate group
        assert group_info_psql[0] == int(
            group_info_redis.get('gid')) == group_id

        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 1

        # Try to create the same group
        test_context.kafka_producer.handle_single_group_create(
            str(group_id), uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        # Expected request to be failed
        if ret_msg.value['success'] is True:
            raise Exception(
                'Infra group create completed, while expected to be failed!')

        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 1

        # Delete single group
        uuid_val = uuid.uuid4()

        test_context.kafka_producer.handle_single_group_delete(
            str(group_id), uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group delete failed!')

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate group removed from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        assert group_info_redis == {}

        # Validate group removed from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        assert group_info_redis == {}

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_remove_not_existing_infra_group(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

        group_id = 100
        uuid_val = uuid.uuid4()

        test_context.kafka_producer.handle_single_group_delete(
            str(group_id), uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is True:
            raise Exception(
                'Infra group delete completed, while expected to be failed!')

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_single_infra_group_add_del_to_shard(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

        uuid_val = uuid.uuid4()
        group_id = 100

        # Create single group
        test_context.kafka_producer.handle_single_group_create(
            str(group_id), uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group create failed!')

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infra group create failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infra group create failed!')

        # Validate group
        assert group_info_psql[0] == int(
            group_info_redis.get('gid')) == group_id

        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 1

        # Delete single group
        uuid_val = uuid.uuid4()

        test_context.kafka_producer.handle_single_group_delete(
            str(group_id), uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group delete failed!')

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate group removed from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        assert group_info_redis == {}

        # Validate group removed from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        assert group_info_redis == {}

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_multiple_infra_group_add_del_to_shard(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

        groups_num = random.randint(1, 10)

        for group in range(0, groups_num):
            uuid_val = uuid.uuid4()
            group_id = (100 + group)

            # Create single group
            test_context.kafka_producer.handle_single_group_create(
                str(group_id), uuid_val.int, default_shard_id)
            ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
            if not ret_msg:
                print('Failed to receive create group result, was expecting ' +
                      str(uuid_val.int) + ' uuid reply')
                raise Exception(
                    'Failed to receive create group result when expected')

            assert (ret_msg.value['type'] ==
                    'infrastructure_group_create_response')
            assert (int(ret_msg.value['infra_group_id']) == group_id)
            assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

            if ret_msg.value['success'] is False:
                print(ret_msg.value['error_message'])
                raise Exception('Infra group create failed!')

            # Get group info from Redis
            group_info_redis = test_context.redis_client.get_infrastructure_group(
                group_id)
            if not group_info_redis:
                print(f'Failed to get group {group_id} info from Redis!')
                raise Exception('Infra group create failed!')

            # Get group info from PSQL
            group_info_psql = test_context.psql_client.get_infrastructure_group(
                group_id)
            if not group_info_psql:
                print(f'Failed to get group {group_id} info from PSQL!')
                raise Exception('Infra group create failed!')

            # Validate group
            assert group_info_psql[0] == int(
                group_info_redis.get('gid')) == group_id

            shard_info = test_context.redis_client.get_shard(default_shard_id)
            if not shard_info:
                print(
                    f'Failed to get shard {default_shard_id} info from Redis!')
                raise Exception(
                    f'Failed to get shard {default_shard_id} info from Redis!!')

            # Validate number of assigned groups
            assert int(shard_info.get('assigned_groups_num')
                       ) == cgw_metrics_get_groups_assigned_num() == (group + 1)

        # Make sure assigned groups number from CGW side is expected
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == groups_num

        for group in range(0, groups_num):
            # Delete single group
            uuid_val = uuid.uuid4()
            group_id = (100 + group)

            test_context.kafka_producer.handle_single_group_delete(
                str(group_id), uuid_val.int)
            ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
            if not ret_msg:
                print('Failed to receive delete group result, was expecting ' +
                      str(uuid_val.int) + ' uuid reply')
                raise Exception(
                    'Failed to receive delete group result when expected')

            assert (ret_msg.value['type'] ==
                    'infrastructure_group_delete_response')
            assert (int(ret_msg.value['infra_group_id']) == group_id)
            assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

            if ret_msg.value['success'] is False:
                print(ret_msg.value['error_message'])
                raise Exception('Infra group delete failed!')

            # Get shard info from Redis
            shard_info = test_context.redis_client.get_shard(default_shard_id)
            if not shard_info:
                print(
                    f'Failed to get shard {default_shard_id} info from Redis!')
                raise Exception(
                    f'Failed to get shard {default_shard_id} info from Redis!')

            # Validate group removed from Redis
            group_info_redis = test_context.redis_client.get_infrastructure_group(
                group_id)
            assert group_info_redis == {}

            # Validate group removed from PSQL
            group_info_psql = test_context.psql_client.get_infrastructure_group(
                group_id)
            assert group_info_redis == {}

            # Validate number of assigned groups
            assert int(shard_info.get('assigned_groups_num')) == cgw_metrics_get_groups_assigned_num(
            ) == (groups_num - (group + 1))

        # Make sure after clean-up assigned group num is zero
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_single_infra_group_add_to_not_existing_shard(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

        uuid_val = uuid.uuid4()
        group_id = 100
        shard_id = 2

        # Create single group
        test_context.kafka_producer.handle_single_group_create(
            str(group_id), uuid_val.int, shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (int(ret_msg.value['infra_group_id']) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is True:
            print(ret_msg.value['error_message'])
            raise Exception(
                'Infra group create completed, while expected to be failed!')

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate group removed from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        assert group_info_redis == {}

        # Validate group removed from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        assert group_info_redis == {}

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_infra_group_capacity_overflow(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

        groups_capacity = cgw_metrics_get_groups_capacity()
        groups_threshold = cgw_metrics_get_groups_threshold()

        groups_num = (groups_capacity + groups_threshold)
        # Create maximum allowed groups number
        for group in range(0, groups_num):
            uuid_val = uuid.uuid4()
            group_id = (100 + group)

            # Create single group
            test_context.kafka_producer.handle_single_group_create(
                str(group_id), uuid_val.int, default_shard_id)
            ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
            if not ret_msg:
                print('Failed to receive create group result, was expecting ' +
                      str(uuid_val.int) + ' uuid reply')
                raise Exception(
                    'Failed to receive create group result when expected')

            assert (ret_msg.value['type'] ==
                    'infrastructure_group_create_response')
            assert (int(ret_msg.value['infra_group_id']) == group_id)
            assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

            if ret_msg.value['success'] is False:
                print(ret_msg.value['error_message'])
                raise Exception('Infra group create failed!')

            # Get group info from Redis
            group_info_redis = test_context.redis_client.get_infrastructure_group(
                group_id)
            if not group_info_redis:
                print(f'Failed to get group {group_id} info from Redis!')
                raise Exception('Infra group create failed!')

            # Get group info from PSQL
            group_info_psql = test_context.psql_client.get_infrastructure_group(
                group_id)
            if not group_info_psql:
                print(f'Failed to get group {group_id} info from PSQL!')
                raise Exception('Infra group create failed!')

            # Validate group
            assert group_info_psql[0] == int(
                group_info_redis.get('gid')) == group_id

            shard_info = test_context.redis_client.get_shard(default_shard_id)
            if not shard_info:
                print(
                    f'Failed to get shard {default_shard_id} info from Redis!')
                raise Exception(
                    f'Failed to get shard {default_shard_id} info from Redis!!')

            # Validate number of assigned groups
            assert int(shard_info.get('assigned_groups_num')
                       ) == cgw_metrics_get_groups_assigned_num() == (group + 1)

        # Make sure we reach MAX groups number assigned to CGW
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == groups_num

        # Try to create additional group to simulate group capacity overflow
        group_to_fail_id = 2024
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_group_create(
            str(group_to_fail_id), uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (int(ret_msg.value['infra_group_id']) == group_to_fail_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

        if ret_msg.value['success'] is True:
            print(ret_msg.value['error_message'])
            raise Exception(
                'Infra group create completed, while expected to be failed due to capacity overflow!')

        # Validate group removed from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_to_fail_id)
        assert group_info_redis == {}

        # Validate group removed from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_to_fail_id)
        assert group_info_redis == {}

        # Get shard info
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!!')

        # Double check groups number assigned to CGW
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == groups_num

        # Cleanup all the rest groups
        for group in range(0, groups_num):
            # Delete single group
            uuid_val = uuid.uuid4()
            group_id = (100 + group)

            test_context.kafka_producer.handle_single_group_delete(
                str(group_id), uuid_val.int)
            ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
            if not ret_msg:
                print('Failed to receive delete group result, was expecting ' +
                      str(uuid_val.int) + ' uuid reply')
                raise Exception(
                    'Failed to receive delete group result when expected')

            assert (ret_msg.value['type'] ==
                    'infrastructure_group_delete_response')
            assert (int(ret_msg.value['infra_group_id']) == group_id)
            assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

            if ret_msg.value['success'] is False:
                print(ret_msg.value['error_message'])
                raise Exception('Infra group delete failed!')

            # Get shard info from Redis
            shard_info = test_context.redis_client.get_shard(default_shard_id)
            if not shard_info:
                print(
                    f'Failed to get shard {default_shard_id} info from Redis!')
                raise Exception(
                    f'Failed to get shard {default_shard_id} info from Redis!')

            # Validate group removed from Redis
            group_info_redis = test_context.redis_client.get_infrastructure_group(
                group_id)
            assert group_info_redis == {}

            # Validate group removed from PSQL
            group_info_psql = test_context.psql_client.get_infrastructure_group(
                group_id)
            assert group_info_redis == {}

            # Validate number of assigned groups
            assert int(shard_info.get('assigned_groups_num')) == cgw_metrics_get_groups_assigned_num(
            ) == (groups_num - (group + 1))

        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0
