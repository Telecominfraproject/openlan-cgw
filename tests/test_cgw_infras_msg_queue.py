import pytest
import uuid
import random
from randmac import RandMac
import time

from metrics import cgw_metrics_get_active_shards_num, \
    cgw_metrics_get_groups_assigned_num, \
    cgw_metrics_get_group_infras_assigned_num, \
    cgw_metrics_get_group_infras_capacity, \
    cgw_metrics_get_connections_num


class TestCgwInfrasMsgQueue:
    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe",
                             "kafka_default_infra_group",
                             "kafka_default_infra",
                             "device_sim_connect",
                             "device_sim_send_ucentral_connect")
    def test_infra_msg_reboot(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        # Simulate at least 1 sec sleep before checking metrics
        # Without it, tests sometimes can fail
        # NOTE: more complex tests might avoid waiting
        # by making sure to wait / recv the infra_join msg.
        time.sleep(1)

        assert cgw_metrics_get_active_shards_num() >= 1
        assert cgw_metrics_get_connections_num() == 1

        shard_info = test_context.redis_client.get_shard(0)
        if not shard_info:
            print(f'Failed to get shard 0 info from Redis!')
            raise Exception('Failed to get shard 0 info from Redis!')

        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 1

        uuid_val = uuid.uuid4()
        request_id = random.randint(1, 100)
        default_group = test_context.default_kafka_group()
        default_infra_mac = test_context.default_dev_sim_mac()

        msg = test_context.kafka_producer.device_message_reboot(
            default_infra_mac, id=request_id)
        test_context.kafka_producer.handle_single_device_message(
            msg, default_group, default_infra_mac, uuid_val.int)
        wss_recv_msg = test_context.device_sim.get_single_message(
            test_context.device_sim._socket)
        kafka_ret_msg = test_context.kafka_consumer.get_result_msg(
            uuid_val.int)
        assert wss_recv_msg is not None, \
            f'Failed to receive any message while expected to'

        assert wss_recv_msg["method"] == msg["method"] == "reboot"
        assert wss_recv_msg["id"] == msg["id"]
        assert wss_recv_msg["params"]["serial"] == msg["params"]["serial"]

        reboot_response = test_context.device_sim.messages.from_json(
            test_context.device_sim.messages.reboot_response)
        reboot_response["result"]["id"] = wss_recv_msg["id"]

        test_context.device_sim._socket.send(
            test_context.device_sim.messages.to_json(reboot_response))
        kafka_result_msg = test_context.kafka_consumer.get_infra_request_result_msg(
            uuid_val.int)

        assert kafka_result_msg is not None, \
            f'Expected to received enqueue request result, found none!'

        assert kafka_result_msg.value['success'], \
            f'Expected result message to have status successful (success=True)!'

        assert kafka_result_msg.value["id"] == request_id

        test_context.device_sim.disconnect()

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 0
        assert test_context.device_sim._socket is None, \
            f"Expected websocket connection to be NULL after disconnect."

        # Chill for a while before reconnecting
        time.sleep(2)

        test_context.device_sim.connect()
        test_context.device_sim.send_hello(test_context.device_sim._socket)

        # Simulate at least 1 sec sleep before checking metrics
        time.sleep(1)
        assert cgw_metrics_get_connections_num() == 1
        assert test_context.device_sim._socket is not None, \
            f"Expected websocket connection NOT to be NULL after reconnect."

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_infra_msg_reboot_invalid_gid(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard info from Redis
        shard_info = test_context.redis_client.get_shard(default_shard_id)
        if not shard_info:
            print(f'Failed to get shard 0 info from Redis!')
            raise Exception('Failed to get shard 0 info from Redis!')

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

        uuid_val = uuid.uuid4()
        request_id = random.randint(1, 100)
        message_group_id = 200

        msg = test_context.kafka_producer.device_message_reboot(
            infra_mac, id=request_id)
        test_context.kafka_producer.handle_single_device_message(
            msg, message_group_id, infra_mac, uuid_val.int)
        wss_recv_msg = test_context.device_sim.get_single_message(
            test_context.device_sim._socket)

        assert wss_recv_msg is None, \
            f'Unexpectedly infra receive message'

        # Get expected message
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra enqueue result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra enqueue result when expected')

        assert ret_msg.value['success'] is False

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infra_message_enqueue_response')
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))

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
