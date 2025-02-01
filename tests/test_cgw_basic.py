import pytest
import json
import random

from metrics import cgw_metrics_get_connections_num, \
    cgw_metrics_get_groups_assigned_num, \
    cgw_metrics_get_group_infras_assigned_num


class TestCgwBasic:
    # Base test:
    # - test_context can be created - 'tests core' alloc / create
    # - tests can connect to kafka broker
    # - CGW is up
    # - PostgreSQL is up
    # - Redis is up
    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "psql_probe",
                             "redis_probe")
    def test_basic_probe(self, test_context):
        pass

    # Base test:
    # - tests kafka client can create / receive messages through kafka bus
    # - test infra group can be successfully created
    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "psql_probe",
                             "redis_probe",
                             "kafka_default_infra_group")
    def test_kafka_sanity(self, test_context):
        pass

    # Base test:
    # - test infra can be added successfully to the default infra group
    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "psql_probe",
                             "redis_probe",
                             "kafka_default_infra_group",
                             "kafka_default_infra")
    def test_kafka_basic(self, test_context):
        pass

    # Base test:
    # - certificates can be found / Used
    # - device sim can connect to CGW
    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "device_sim_connect")
    def test_device_sim_sanity(self, test_context):
        pass

    # Base test:
    # - device sim can send connect message to cgw
    # - kafka client can verify (pull msgs from kafka bus)
    #   that simulator's indeed connected
    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "device_sim_connect",
                             "device_sim_send_ucentral_connect")
    def test_device_sim_base(self, test_context):
        pass

    # Base test:
    # - unassigned infra connects to CGW, and kafka sim can validate it
    #   through the <infra_join> msg
    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "device_sim_connect",
                             "device_sim_send_ucentral_connect")
    def test_unassigned_infra_base(self, test_context):
        join_message_received = False
        infra_is_unassigned = False
        messages = test_context.kafka_consumer.get_msgs()
        msg_mac = test_context.default_dev_sim_mac()

        assert messages, \
            f"Failed to receive any messages (events) from sim-device, while expected connect / infra_join"

        if not messages:
            raise Exception(
                'Failed to receive infra assign result when expected')

        # Expecting TWO messages to be present in the message list
        for message in messages:
            if message.value['type'] == 'infra_join' and message.key == b'0' and message.value['infra_group_infra'] == msg_mac:
                join_message_received = True
                continue

            if message.value['type'] == 'unassigned_infra_join' and message.key == b'0' and message.value['infra_group_infra'] == msg_mac:
                infra_is_unassigned = True
                continue

        assert cgw_metrics_get_connections_num() == 1

        assert (join_message_received == False), \
            f"Found 'infra_join' message for default infra MAC, when expected 'unassigned_infra_join' only to be received"

        assert infra_is_unassigned, \
            f"Failed to find unassigned 'unassigned_infra_join' message for default infra MAC"

    # Base test:
    # - assigned infra connects to CGW, and kafka sim can validate it
    #   through the <infra_join> msg + kafka key

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "psql_probe",
                             "redis_probe",
                             "kafka_default_infra_group",
                             "kafka_default_infra",
                             "device_sim_connect",
                             "device_sim_send_ucentral_connect")
    def test_assigned_infra_base(self, test_context):
        join_message_received = False
        infra_is_assigned = False
        messages = test_context.kafka_consumer.get_msgs()
        msg_mac = test_context.default_dev_sim_mac()
        default_group = test_context.default_kafka_group()

        assert messages, \
            f"Failed to receive any messages (events) from sim-device, while expected connect / infra_join"

        if not messages:
            raise Exception(
                'Failed to receive infra assign result when expected')

        # We can deduce whether infra's assigned by inspecting a single msg
        for message in messages:
            if message.value['type'] == 'infra_join' and message.value['infra_group_infra'] == msg_mac:
                join_message_received = True
                if int(message.key) == default_group and int(message.value['infra_group_id']) == default_group:
                    infra_is_assigned = True
                break

        assert cgw_metrics_get_groups_assigned_num() == 1
        assert cgw_metrics_get_connections_num() == 1
        assert cgw_metrics_get_group_infras_assigned_num(
            default_group) == 1

        assert join_message_received, \
            f"Failed to find 'infra_join' message for default infra MAC"

        assert infra_is_assigned, \
            f"While detected join message for default infra MAC, expected it to be assigned to group (key != default group id)"
