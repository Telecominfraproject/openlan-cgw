import pytest
import uuid
import random
from randmac import RandMac
import time

from metrics import cgw_metrics_get_active_shards_num, \
    cgw_metrics_get_groups_assigned_num, \
    cgw_metrics_get_group_infras_assigned_num, \
    cgw_metrics_get_group_ifras_capacity, \
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
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Cannot create default group: kafka consumer is not connected to Kafka'

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
