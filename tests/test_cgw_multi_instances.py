import ssl
import json
import time
import pytest
import uuid

from kafka_producer.src.admin import Message
from client_simulator.src.simulation_runner import Device as DeviceSimulator

from metrics import cgw_metrics_get_groups_assigned_num, \
    cgw_metrics_get_group_infras_assigned_num, \
    cgw_metrics_get_active_shards_num


class TestCgwMultiInstances:
    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "kafka_admin_probe",
                             "redis_probe",
                             "psql_probe")
    def test_relay_infra_add_del(self, test_context):
        """
        This test case verify CGW message relaying mechanism
        1) Create group 100 on Shard ID 0
        2) Calculate Shard ID to Kafka partition map
        3) Send infra assign message with group id 100 to Shard id (N),
            where N - is number of running CGW instances - 1
        4) Infra assign message receive Shard ID that is not group 100 owner
        5) Expected message to be relayed to Shard ID 0
        6) Validate reported Shard ID from infra add response message
        7) Repeat steps 3-6 for infra deassign message
        """

        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        assert test_context.kafka_admin.is_connected(), \
            f'Kafka admin is not connected to Kafka'

        active_shards_num = cgw_metrics_get_active_shards_num()

        # This test-case require at least 2 CGW instances
        # To avoid test failure in single CGW env. - make force test passed
        if active_shards_num <= 1:
            pytest.skip(
                f"Number of CGW instances not enough to proceed with test! Expected > 2, actually running - {active_shards_num}. Skip test.")

        default_shard_id = test_context.default_shard_id()

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        # assert int(shard_info.get('assigned_groups_num')) == cgw_metrics_get_groups_assigned_num() == 0

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
        # assert int(shard_info.get('assigned_groups_num')) == cgw_metrics_get_groups_assigned_num() == 1

        # Get highest CGW ID assigned partition
        partitions = test_context.kafka_admin.get_topic_partitions_for_cgw_id(
            'CnC', ['CGW'], (active_shards_num - 1))
        assert len(partitions) > 0

        # Infra add to Group
        # Send message to CGW that does not own group - to force CGW message relay mechanism
        infra_mac = "11-22-33-44-55-66"
        message = Message()
        test_context.kafka_producer.conn.send(test_context.default_producer_topic(), message.add_dev_to_group(
            str(group_id), [infra_mac], uuid_val.int), bytes(str(group_id), encoding="utf-8"),  partition=partitions[0])

        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra assign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra assign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra assign failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_add_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        # We don't expect to have even a single 'failed_infra',
        # because the overall command succeeded
        assert (len(list(ret_msg.value["failed_infras"])) == 0)
        assert (ret_msg.value["reporter_shard_id"] == default_shard_id)

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infra assign failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infra assign failed!')

        # Validate infras assigned number
        assert int(group_info_redis.get('infras_assigned')
                   ) == cgw_metrics_get_group_infras_assigned_num(group_id) == 1

        # Get infra info from Redis Infra Cache
        infra_info_redis = test_context.redis_client.get_infra(
            default_shard_id, infra_mac)
        if not infra_info_redis:
            print(f'Failed to get infra {infra_mac} info from Redis!')
            raise Exception('Infra assign failed!')

        # Get infra info from PSQL
        infra_info_psql = test_context.psql_client.get_infra(infra_mac)
        if not infra_info_psql:
            print(f'Failed to get infra {infra_mac} info from PSQL!')
            raise Exception('Infra assign failed!')

        # Validate infra assigned group id
        assert infra_info_psql[1] == int(
            infra_info_redis.get('group_id')) == group_id

        # Infra del
        # Send message to CGW that does not own group - to force CGW message relay mechanism
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.conn.send(test_context.default_producer_topic(),
                                              message.remove_dev_from_group(str(group_id), [infra_mac], uuid_val.int), bytes(str(group_id), encoding="utf-8"),  partition=partitions[0])
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra deassign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra deassign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra deassign failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_del_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        # We don't expect to have even a single 'failed_infra',
        # because the overall command succeeded
        assert (len(list(ret_msg.value["failed_infras"])) == 0)
        assert (ret_msg.value["reporter_shard_id"] == default_shard_id)

        # Validate infra removed from Redis Infra Cache
        infra_info_redis = test_context.redis_client.get_infra(
            default_shard_id, infra_mac)
        assert infra_info_redis == None

        # Validate infra removed from PSQL
        infra_info_psql = test_context.psql_client.get_infra(infra_mac)
        assert infra_info_psql == None

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infra deassign failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infra deassign failed!')

        # Validate number of assigned infra number
        assert int(group_info_redis.get('infras_assigned')
                   ) == cgw_metrics_get_group_infras_assigned_num(group_id) == 0

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
        # assert int(shard_info.get('assigned_groups_num')) == cgw_metrics_get_groups_assigned_num() == 0

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "kafka_admin_probe",
                             "redis_probe",
                             "psql_probe")
    def test_single_group_to_shard_0_send_to_shard_1(self, test_context):
        """
        This test case verify CGW interconnection and group create/delete mechanism
        1) Calculate Shard ID to Kafka partition map
        2) Prepare group create to Shard ID 0 (group_id = 100) message, but send it to Kafka partition for Shard ID 1
        3) Expected message to be processed by Shard ID 1
        4) Validate reported Shard ID from group create response message (Expected to be Shard ID 1)
        5) Repeat steps 2-5 for group delete message
        """

        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        assert test_context.kafka_admin.is_connected(), \
            f'Kafka admin is not connected to Kafka'

        active_shards_num = cgw_metrics_get_active_shards_num()

        # This test-case require at least 2 CGW instances
        # To avoid test failure in single CGW env. - make force test passed
        if active_shards_num <= 1:
            pytest.skip(
                f"Number of CGW instances not enough to proceed with test! Expected > 2, actually running - {active_shards_num}. Skip test.")

        default_shard_id = test_context.default_shard_id()
        expected_reporter_shard_id: int = 1

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        # assert int(shard_info.get('assigned_groups_num')) == cgw_metrics_get_groups_assigned_num() == 0

        uuid_val = uuid.uuid4()
        group_id = 100

        # Get highest CGW ID assigned partition
        partitions = test_context.kafka_admin.get_topic_partitions_for_cgw_id(
            'CnC', ['CGW'], expected_reporter_shard_id)
        assert len(partitions) > 0

        message = Message()

        # Create single group
        test_context.kafka_producer.conn.send(test_context.default_producer_topic(),
                                              message.group_create_to_shard(str(group_id), default_shard_id, uuid_val.int), bytes(str(group_id), encoding="utf-8"),  partition=partitions[0])
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
        assert (ret_msg.value["reporter_shard_id"]
                == expected_reporter_shard_id)

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
        assert int(group_info_redis.get('shard_id')) == default_shard_id

        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!!')

        # Validate number of assigned groups
        # assert int(shard_info.get('assigned_groups_num')) == cgw_metrics_get_groups_assigned_num() == 1

        # Delete single group
        uuid_val = uuid.uuid4()

        test_context.kafka_producer.conn.send(test_context.default_producer_topic(),
                                              message.group_delete(str(group_id), uuid_val.int), bytes(str(group_id), encoding="utf-8"),  partition=partitions[0])
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
        assert (ret_msg.value["reporter_shard_id"]
                == expected_reporter_shard_id)

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
        # assert int(shard_info.get('assigned_groups_num')) == cgw_metrics_get_groups_assigned_num() == 0

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "kafka_admin_probe",
                             "redis_probe",
                             "psql_probe")
    def test_single_group_to_shard_1_send_to_shard_1(self, test_context):
        """
        This test case verify CGW interconnection and group create/delete mechanism
        1) Calculate Shard ID to Kafka partition map
        2) Prepare group create to Shard ID 1 (group_id = 100) message, send it to Kafka partition for Shard ID 1
        3) Expected message to be processed by Shard ID 1
        4) Validate reported Shard ID from group create response message (Expected to be Shard ID 1)
        5) Repeat steps 2-5 for group delete message
        """

        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        assert test_context.kafka_admin.is_connected(), \
            f'Kafka admin is not connected to Kafka'

        active_shards_num = cgw_metrics_get_active_shards_num()

        # This test-case require at least 2 CGW instances
        # To avoid test failure in single CGW env. - make force test passed
        if active_shards_num <= 1:
            pytest.skip(
                f"Number of CGW instances not enough to proceed with test! Expected > 2, actually running - {active_shards_num}. Skip test.")

        shard_id = 1
        expected_reporter_shard_id: int = 1

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(shard_id)
        if not shard_info:
            print(f'Failed to get shard {shard_id} info from Redis!')
            raise Exception(f'Failed to get shard {shard_id} info from Redis!')

        # Validate number of assigned groups
        # assert int(shard_info.get('assigned_groups_num')) == cgw_metrics_get_groups_assigned_num() == 0

        uuid_val = uuid.uuid4()
        group_id = 100

        # Get highest CGW ID assigned partition
        partitions = test_context.kafka_admin.get_topic_partitions_for_cgw_id(
            'CnC', ['CGW'], expected_reporter_shard_id)
        assert len(partitions) > 0

        message = Message()

        # Create single group
        test_context.kafka_producer.conn.send(test_context.default_producer_topic(),
                                              message.group_create_to_shard(str(group_id), shard_id, uuid_val.int), bytes(str(group_id), encoding="utf-8"),  partition=partitions[0])
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
        assert (ret_msg.value["reporter_shard_id"]
                == expected_reporter_shard_id)

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
        assert int(group_info_redis.get('shard_id')) == shard_id

        shard_info = test_context.redis_client.get_shard(shard_id)
        if not shard_info:
            print(f'Failed to get shard {shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {shard_id} info from Redis!!')

        # Validate number of assigned groups
        # assert int(shard_info.get('assigned_groups_num')) == cgw_metrics_get_groups_assigned_num() == 1

        # Delete single group
        uuid_val = uuid.uuid4()

        test_context.kafka_producer.conn.send(test_context.default_producer_topic(),
                                              message.group_delete(str(group_id), uuid_val.int), bytes(str(group_id), encoding="utf-8"),  partition=partitions[0])
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
        assert (ret_msg.value["reporter_shard_id"]
                == expected_reporter_shard_id)

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group delete failed!')

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(shard_id)
        if not shard_info:
            print(f'Failed to get shard {shard_id} info from Redis!')
            raise Exception(f'Failed to get shard {shard_id} info from Redis!')

        # Validate group removed from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        assert group_info_redis == {}

        # Validate group removed from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        assert group_info_redis == {}

        # Validate number of assigned groups
        # assert int(shard_info.get('assigned_groups_num')) == cgw_metrics_get_groups_assigned_num() == 0

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "kafka_admin_probe",
                             "redis_probe",
                             "psql_probe")
    def test_single_group_to_shard_any_send_to_shard_1(self, test_context):
        """
        This test case verify CGW interconnection and group create/delete mechanism
        1) Calculate Shard ID to Kafka partition map
        2) Prepare group create (group_id = 100) message (Shard ID -> not specified), send it to Kafka partition for Shard ID 1
        3) Expected message to be processed by Shard ID 1
        4) Validate reported Shard ID from group create response message (Expected to be Shard ID 1)
        5) Repeat steps 2-5 for group delete message
        """

        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        assert test_context.kafka_admin.is_connected(), \
            f'Kafka admin is not connected to Kafka'

        active_shards_num = cgw_metrics_get_active_shards_num()

        # This test-case require at least 2 CGW instances
        # To avoid test failure in single CGW env. - make force test passed
        if active_shards_num <= 1:
            pytest.skip(
                f"Number of CGW instances not enough to proceed with test! Expected > 2, actually running - {active_shards_num}. Skip test.")

        expected_reporter_shard_id: int = 1

        uuid_val = uuid.uuid4()
        group_id = 100

        # Get highest CGW ID assigned partition
        partitions = test_context.kafka_admin.get_topic_partitions_for_cgw_id(
            'CnC', ['CGW'], expected_reporter_shard_id)
        assert len(partitions) > 0

        message = Message()

        # Create single group
        test_context.kafka_producer.conn.send(test_context.default_producer_topic(),
                                              message.group_create(str(group_id), uuid_val.int), bytes(str(group_id), encoding="utf-8"),  partition=partitions[0])
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
        assert (ret_msg.value["reporter_shard_id"]
                == expected_reporter_shard_id)

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

        # Delete single group
        uuid_val = uuid.uuid4()

        test_context.kafka_producer.conn.send(test_context.default_producer_topic(),
                                              message.group_delete(str(group_id), uuid_val.int), bytes(str(group_id), encoding="utf-8"),  partition=partitions[0])
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
        assert (ret_msg.value["reporter_shard_id"]
                == expected_reporter_shard_id)

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra group delete failed!')

        # Validate group removed from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        assert group_info_redis == {}

        # Validate group removed from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        assert group_info_redis == {}

        # Validate number of assigned groups
        # assert int(shard_info.get('assigned_groups_num')) == cgw_metrics_get_groups_assigned_num() == 0

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_foreign_infra_connection(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        active_shards_num = cgw_metrics_get_active_shards_num()

        # This test-case require at least 2 CGW instances
        # To avoid test failure in single CGW env. - make force test passed
        if active_shards_num <= 1:
            pytest.skip(
                f"Number of CGW instances not enough to proceed with test! Expected > 2, actually running - {active_shards_num}. Skip test.")

        default_shard_id = test_context.default_shard_id()

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        #assert int(shard_info.get('assigned_groups_num')
        #           ) == cgw_metrics_get_groups_assigned_num() == 0

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
        # assert int(shard_info.get('assigned_groups_num')
        #            ) == cgw_metrics_get_groups_assigned_num() == 1

        # Infra add
        infra_mac = "11-22-33-44-55-66"
        test_context.kafka_producer.handle_single_device_assign(
            str(group_id), infra_mac, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra assign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra assign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            print(ret_msg.value["failed_infras"])
            raise Exception('Infra assign failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_add_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        # We don't expect to have even a single 'failed_infra',
        # because the overall command succeeded
        assert (len(list(ret_msg.value["failed_infras"])) == 0)

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infra assign failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infra assign failed!')

        # Validate infras assigned number
        assert int(group_info_redis.get('infras_assigned')
                   ) == cgw_metrics_get_group_infras_assigned_num(group_id) == 1

        # Get infra info from Redis Infra Cache
        infra_info_redis = test_context.redis_client.get_infra(
            default_shard_id, infra_mac)
        if not infra_info_redis:
            print(f'Failed to get infra {infra_mac} info from Redis!')
            raise Exception('Infra assign failed!')

        # Get infra info from PSQL
        infra_info_psql = test_context.psql_client.get_infra(infra_mac)
        if not infra_info_psql:
            print(f'Failed to get infra {infra_mac} info from PSQL!')
            raise Exception('Infra assign failed!')

        # Validate infra assigned group id
        assert infra_info_psql[1] == int(
            infra_info_redis.get('group_id')) == group_id

        # Get second shard info from Redis
        destination_shard_id = 1
        destination_shard_info = test_context.redis_client.get_shard(destination_shard_id)
        if not destination_shard_info:
            print(f'Failed to get shard {destination_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {destination_shard_id} info from Redis!')

        destination_shard_host = 'localhost'
        destination_wss_port = destination_shard_info.get('wss_port')
        # Create sim device - force connection to second shard
        # As the shard was aleady assigned to first shard
        # it will simulate foreign infra connection!
        # Connect infra to CGW
        device = DeviceSimulator(
            mac=infra_mac,
            server=f'wss://{destination_shard_host}:{destination_wss_port}',
            ca_cert='./ca-certs/ca.crt',
            msg_interval=10, msg_size=1024,
            client_cert='./certs/base.crt', client_key='./certs/base.key', check_cert=False,
            start_event=None, stop_event=None)

        # Server cert CN? don't care, ignore
        device.ssl_context.check_hostname = False
        device.ssl_context.verify_mode = ssl.CERT_NONE

        # Tweak connect message to change initial FW version:
        # Any latter steps might want to change it to something else
        # (test capabilities change, for example);
        # However, we're making a fixture, hence all values must be the same
        # on the initial step.
        connect_msg = json.loads(device.messages.connect)
        connect_msg['params']['capabilities']['platform'] = "ap"
        connect_msg['params']['firmware'] = "Test_FW_A"
        connect_msg['params']['uuid'] = 1
        device.messages.connect = json.dumps(connect_msg)

        device.connect()
        device.send_hello(device._socket)

        # Validate Kafka messages
        # Expected to receive:
        #   1. Foreign Infra Connection - from Conenction topic
        #   2. Infra Join               - from Connection topic
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'foreign_infra_connection')
        if ret_msg is None:
            print('Failed to receive foreign infra connection message!')
            raise Exception(
                'Failed to receive foreign infra connection message!')

        assert ret_msg.topic == 'Connection'
        assert ret_msg.value['type'] == 'foreign_infra_connection'

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'Connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Validate Device socket recv message
        # Expected to receive foerign connection message
        wss_recv_msg = device.get_single_message(device._socket)

        assert wss_recv_msg is not None, \
            f'Failed to receive any message while expected to'

        assert wss_recv_msg["type"] == "foreign_connection"
        assert wss_recv_msg["infra_group_infra"] == infra_mac
        assert wss_recv_msg["destination_shard_host"] == shard_info.get('server_host')
        assert int(wss_recv_msg["destination_wss_port"]) == int(shard_info.get('wss_port'))

        # Send disconnect
        device.disconnect()

        # Valiudate Kafka result message
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'Connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            str(group_id), infra_mac, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra deassign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra deassign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra deassign failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_del_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        # We don't expect to have even a single 'failed_infra',
        # because the overall command succeeded
        assert (len(list(ret_msg.value["failed_infras"])) == 0)

        # Validate infra removed from Redis Infra Cache
        infra_info_redis = test_context.redis_client.get_infra(
            default_shard_id, infra_mac)
        assert infra_info_redis == None

        # Validate infra removed from PSQL
        infra_info_psql = test_context.psql_client.get_infra(infra_mac)
        assert infra_info_psql == None

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infra deassign failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infra deassign failed!')

        # Validate number of assigned infra number
        assert int(group_info_redis.get('infras_assigned')
                   ) == cgw_metrics_get_group_infras_assigned_num(group_id) == 0

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
        # assert int(shard_info.get('assigned_groups_num')
        #            ) == cgw_metrics_get_groups_assigned_num() == 0
