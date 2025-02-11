import pytest
import uuid
import time

from metrics import cgw_metrics_get_groups_assigned_num, \
    cgw_metrics_get_group_infras_assigned_num, \
    cgw_metrics_get_connections_num


class TestCgwInfraEvents:
    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_infra_state_event(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        test_context.device_sim.send_state_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_state_event_message')
        if ret_msg is None:
            print('Failed to receive infra state event message!')
            raise Exception(
                'Failed to receive infra state event message!')

        assert ret_msg.topic == 'state'
        assert ret_msg.value['type'] == 'infrastructure_state_event_message'
        assert ret_msg.value['event_type'] == 'state'

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
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
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_infra_healthcheck_event(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        test_context.device_sim.send_healthcheck_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_state_event_message')
        if ret_msg is None:
            print('Failed to receive infra state event message!')
            raise Exception(
                'Failed to receive infra state event message!')

        assert ret_msg.topic == 'state'
        assert ret_msg.value['type'] == 'infrastructure_state_event_message'
        assert ret_msg.value['event_type'] == 'healthcheck'

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
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
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_infra_crashlog_event(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        test_context.device_sim.send_crashlog_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is None:
            print('Failed to receive infra realtime event message!')
            raise Exception(
                'Failed to receive infra realtime event message!')

        assert ret_msg.topic == 'infra_realtime'
        assert ret_msg.value['type'] == 'infrastructure_realtime_event_message'
        assert ret_msg.value['event_type'] == 'crashlog'

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
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
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_infra_rebootlog_event(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        test_context.device_sim.send_rebootlog_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is None:
            print('Failed to receive infra realtime event message!')
            raise Exception(
                'Failed to receive infra realtime event message!')

        assert ret_msg.topic == 'infra_realtime'
        assert ret_msg.value['type'] == 'infrastructure_realtime_event_message'
        assert ret_msg.value['event_type'] == 'rebootLog'

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
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
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_infra_cfgpending_event(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        test_context.device_sim.send_cfgpending_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is None:
            print('Failed to receive infra realtime event message!')
            raise Exception(
                'Failed to receive infra realtime event message!')

        assert ret_msg.topic == 'infra_realtime'
        assert ret_msg.value['type'] == 'infrastructure_realtime_event_message'
        assert ret_msg.value['event_type'] == 'cfgpending'

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
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
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_infra_ping_event(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        test_context.device_sim.send_ping_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is None:
            print('Failed to receive infra realtime event message!')
            raise Exception(
                'Failed to receive infra realtime event message!')

        assert ret_msg.topic == 'infra_realtime'
        assert ret_msg.value['type'] == 'infrastructure_realtime_event_message'
        assert ret_msg.value['event_type'] == 'ping'

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
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
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_infra_recovery_event(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        test_context.device_sim.send_recovery_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is None:
            print('Failed to receive infra realtime event message!')
            raise Exception(
                'Failed to receive infra realtime event message!')

        assert ret_msg.topic == 'infra_realtime'
        assert ret_msg.value['type'] == 'infrastructure_realtime_event_message'
        assert ret_msg.value['event_type'] == 'recovery'

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
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
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_infra_log_event(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        test_context.device_sim.send_log_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is None:
            print('Failed to receive infra realtime event message!')
            raise Exception(
                'Failed to receive infra realtime event message!')

        assert ret_msg.topic == 'infra_realtime'
        assert ret_msg.value['type'] == 'infrastructure_realtime_event_message'
        assert ret_msg.value['event_type'] == 'log'

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
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
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_infra_event(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        # Reuse existing event: client.join
        test_context.device_sim.send_join(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is None:
            print('Failed to receive infra realtime event message!')
            raise Exception(
                'Failed to receive infra realtime event message!')

        assert ret_msg.topic == 'infra_realtime'
        assert ret_msg.value['type'] == 'infrastructure_realtime_event_message'
        assert ret_msg.value['event_type'] == 'realtime_event'

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
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
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_infra_alarm_event(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        test_context.device_sim.send_alarm_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is None:
            print('Failed to receive infra realtime event message!')
            raise Exception(
                'Failed to receive infra realtime event message!')

        assert ret_msg.topic == 'infra_realtime'
        assert ret_msg.value['type'] == 'infrastructure_realtime_event_message'
        assert ret_msg.value['event_type'] == 'alarm'

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
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
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_infra_wifiscan_event(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        test_context.device_sim.send_wifiscan_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is None:
            print('Failed to receive infra realtime event message!')
            raise Exception(
                'Failed to receive infra realtime event message!')

        assert ret_msg.topic == 'infra_realtime'
        assert ret_msg.value['type'] == 'infrastructure_realtime_event_message'
        assert ret_msg.value['event_type'] == 'wifiscan'

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
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
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_infra_deviceupdate_event(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        test_context.device_sim.send_deviceupdate_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is None:
            print('Failed to receive infra realtime event message!')
            raise Exception(
                'Failed to receive infra realtime event message!')

        assert ret_msg.topic == 'infra_realtime'
        assert ret_msg.value['type'] == 'infrastructure_realtime_event_message'
        assert ret_msg.value['event_type'] == 'deviceupdate'

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
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
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_infra_venue_broadcast_event(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        test_context.device_sim.send_venue_broadcast_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is None:
            print('Failed to receive infra realtime event message!')
            raise Exception(
                'Failed to receive infra realtime event message!')

        assert ret_msg.topic == 'infra_realtime'
        assert ret_msg.value['type'] == 'infrastructure_realtime_event_message'
        assert ret_msg.value['event_type'] == 'venue_broadcast'

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
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
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_infra_join_leave_events(self, test_context):
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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (ret_msg.topic == 'cnc_res')

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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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
        assert (ret_msg.topic == 'cnc_res')
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        # Get message from Kafka
        # Expected to get 2 events
        # 1. Infra Join
        # 2. Capabilities change event
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_group_infra_capabilities_changed')
        if ret_msg is None:
            print('Failed to receive infra capabilities change event message!')
            raise Exception(
                'Failed to receive infra capabilities change event message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infrastructure_group_infra_capabilities_changed'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive infra leave message!')
            raise Exception(
                'Failed to receive infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra deassign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra deassign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra deassign failed!')

        assert ret_msg.topic == 'cnc_res'
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
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert ret_msg.topic == 'cnc_res'
        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_unassigned_infra_join_leave_events(self, test_context):
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        # Get message from Kafka
        # Expected to get single event - unassigned infra join
        # Capabilities change event - MUST NOT BE SENT - infra was unknown!
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'unassigned_infra_join')
        if ret_msg is None:
            print('Failed to receive unassigned infra join message!')
            raise Exception(
                'Failed to receive unassigned infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'unassigned_infra_join'

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_group_infra_capabilities_changed')
        if ret_msg is not None:
            print('Received unexpected infra capabilities change event message!')
            raise Exception(
                'Received unexpected infra capabilities change event message!')

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'unassigned_infra_leave')
        if ret_msg is None:
            print('Failed to receive unassigned infra leave message!')
            raise Exception(
                'Failed to receive unassigned infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'unassigned_infra_leave'

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_infra_becomes_unassigned_events(self, test_context):
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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (ret_msg.topic == 'cnc_res')

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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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
        assert (ret_msg.topic == 'cnc_res')
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        # Get message from Kafka
        # Expected to get 2 events
        # 1. Infra Join
        # 2. Capabilities change event
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_group_infra_capabilities_changed')
        if ret_msg is None:
            print('Failed to receive infra capabilities change event message!')
            raise Exception(
                'Failed to receive infra capabilities change event message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infrastructure_group_infra_capabilities_changed'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            group_id, infra_mac, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra deassign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra deassign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra deassign failed!')

        assert ret_msg.topic == 'cnc_res'
        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_del_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        # We don't expect to have even a single 'failed_infra',
        # because the overall command succeeded
        assert (len(list(ret_msg.value["failed_infras"])) == 0)

        # Validate infra exist in Redis Infra Cache
        infra_info_redis = test_context.redis_client.get_infra(
            default_shard_id, infra_mac)
        assert infra_info_redis != None

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

        # Expected to receive Unassigned infra join message as connection still alive
        # While infra was removed from group
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'unassigned_infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'unassigned_infra_leave')
        if ret_msg is None:
            print('Failed to receive unassigned infra leave message!')
            raise Exception(
                'Failed to receive unassigned infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'unassigned_infra_leave'

        # Delete single group
        uuid_val = uuid.uuid4()

        test_context.kafka_producer.handle_single_group_delete(
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert ret_msg.topic == 'cnc_res'
        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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
    def test_infra_becomes_unassigned_group_removed_events(self, test_context):
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
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (ret_msg.topic == 'cnc_res')

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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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
        assert (ret_msg.topic == 'cnc_res')
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        # Get message from Kafka
        # Expected to get 2 events
        # 1. Infra Join
        # 2. Capabilities change event
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_group_infra_capabilities_changed')
        if ret_msg is None:
            print('Failed to receive infra capabilities change event message!')
            raise Exception(
                'Failed to receive infra capabilities change event message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infrastructure_group_infra_capabilities_changed'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        # Delete single group
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_group_delete(
            group_id, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive delete group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive delete group result when expected')

        assert ret_msg.topic == 'cnc_res'
        assert (ret_msg.value['type'] ==
                'infrastructure_group_delete_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
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

        # Expected to receive Unassigned infra join message as connection still alive
        # While group was removed
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'unassigned_infra_join')
        if ret_msg is None:
            print('Failed to receive unassigned infra join message!')
            raise Exception(
                'Failed to receive unassigned infra join message!')

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'unassigned_infra_leave')
        if ret_msg is None:
            print('Failed to receive unassigned infra leave message!')
            raise Exception(
                'Failed to receive unassigned infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'unassigned_infra_leave'

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_unassigned_infra_becomes_assigned_events(self, test_context):
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

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        # Get message from Kafka
        # Expected to get 2 events
        # 1. Unassigned Infra Join
        # 2. Capabilities change event
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'unassigned_infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'unassigned_infra_join'

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        uuid_val = uuid.uuid4()
        group_id = 100

        # Create single group
        test_context.kafka_producer.handle_single_group_create(
            group_id, uuid_val.int, default_shard_id)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if not ret_msg:
            print('Failed to receive create group result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive create group result when expected')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_create_response')
        assert (ret_msg.value['infra_group_id'] == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (ret_msg.topic == 'cnc_res')

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

        # Infra add
        infra_mac = test_context.default_dev_sim_mac()
        test_context.kafka_producer.handle_single_device_assign(
            group_id, infra_mac, uuid_val.int)
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
        assert (ret_msg.topic == 'cnc_res')
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

        # Infra appear as assigned - expected to received "infra_join"
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_join')
        if ret_msg is None:
            print('Failed to receive infra join message!')
            raise Exception(
                'Failed to receive infra join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_join'

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infra_leave')
        if ret_msg is None:
            print('Failed to receive unassigned infra leave message!')
            raise Exception(
                'Failed to receive unassigned infra leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'infra_leave'

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_unassigned_infra_events(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard {default_shard_id} info from Redis!')
            raise Exception(
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Connect infra to CGW
        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'unassigned_infra_join')
        if ret_msg is None:
            print('Failed to receive infra unassigned join message!')
            raise Exception(
                'Failed to receive infra unassigned join message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'unassigned_infra_join'

        # Send all know for now infra state/realtime event
        # Validate - for unassigned infra NB SHOULD NOT receive any!

        # Send state event
        test_context.device_sim.send_state_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_state_event_message')
        if ret_msg is not None:
            print('Received infra state event message for unassigned infra!')
            raise Exception(
                'Received infra state event message for unassigned infra!')

        # Send Healthcheck event
        test_context.device_sim.send_healthcheck_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_state_event_message')
        if ret_msg is not None:
            print('Received infra state event message for unassigned infra!')
            raise Exception(
                'Received infra state event message for unassigned infra!')

        # Send Crashlog event
        test_context.device_sim.send_crashlog_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is not None:
            print('Received infra realtime event message for unassigned infra!')
            raise Exception(
                'Received infra realtime event message for unassigned infra!')

        # Send rebootlog event
        test_context.device_sim.send_rebootlog_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is not None:
            print('Received infra realtime event message for unassigned infra!')
            raise Exception(
                'Received infra realtime event message for unassigned infra!')

        # Send cfgpending event
        test_context.device_sim.send_cfgpending_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is not None:
            print('Received infra realtime event message for unassigned infra!')
            raise Exception(
                'Received infra realtime event message for unassigned infra!')

        # Send ping event
        test_context.device_sim.send_ping_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is not None:
            print('Received infra realtime event message for unassigned infra!')
            raise Exception(
                'Received infra realtime event message for unassigned infra!')

        # Send ping event
        test_context.device_sim.send_recovery_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is not None:
            print('Received infra realtime event message for unassigned infra!')
            raise Exception(
                'Received infra realtime event message for unassigned infra!')

        # Send log event
        test_context.device_sim.send_log_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is not None:
            print('Received infra realtime event message for unassigned infra!')
            raise Exception(
                'Received infra realtime event message for unassigned infra!')

        # Send event: client.join
        test_context.device_sim.send_join(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is not None:
            print('Received infra realtime event message for unassigned infra!')
            raise Exception(
                'Received infra realtime event message for unassigned infra!')

        # Send alarm event
        test_context.device_sim.send_alarm_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is not None:
            print('Received infra realtime event message for unassigned infra!')
            raise Exception(
                'Received infra realtime event message for unassigned infra!')

        # Send wifiscan event
        test_context.device_sim.send_wifiscan_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is not None:
            print('Received infra realtime event message for unassigned infra!')
            raise Exception(
                'Received infra realtime event message for unassigned infra!')

        # Send deviceupdate event
        test_context.device_sim.send_deviceupdate_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is not None:
            print('Received infra realtime event message for unassigned infra!')
            raise Exception(
                'Received infra realtime event message for unassigned infra!')

        # Send deviceupdate event
        test_context.device_sim.send_venue_broadcast_event(
            test_context.device_sim._socket)

        # Get message from Kafka
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'infrastructure_realtime_event_message')
        if ret_msg is not None:
            print('Received infra realtime event message for unassigned infra!')
            raise Exception(
                'Received infra realtime event message for unassigned infra!')

        # Simulate infra leave
        test_context.device_sim.disconnect()
        ret_msg = test_context.kafka_consumer.get_msg_by_type(
            'unassigned_infra_leave')
        if ret_msg is None:
            print('Failed to receive infra unassigned leave message!')
            raise Exception(
                'Failed to receive infra unassigned leave message!')

        assert ret_msg.topic == 'connection'
        assert ret_msg.value['type'] == 'unassigned_infra_leave'
