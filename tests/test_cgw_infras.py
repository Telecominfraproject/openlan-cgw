import pytest
import uuid

from metrics import cgw_metrics_get_active_shards_num, \
    cgw_metrics_get_groups_assigned_num, \
    cgw_metrics_get_group_infras_assigned_num


class TestCgwInfra:
    @pytest.mark.usefixtures("test_context",
                            "cgw_probe",
                            "kafka_probe")
    def test_single_infra_add_del(self, test_context):
        assert test_context.kafka_producer.is_connected(),\
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(),\
            f'Cannot create default group: kafka consumer is not connected to Kafka'

        assert cgw_metrics_get_active_shards_num() == 1
        assert cgw_metrics_get_groups_assigned_num() == 0

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

        # Infra add
        infra_mac = "11-22-33-44-55-66"
        test_context.kafka_producer.handle_single_device_assign(str(group_id), infra_mac, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra assign result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive infra assign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra assign failed!')

        assert (ret_msg.value['type'] == 'infrastructure_group_infras_add_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (ret_msg.value["infra_group_infras"][0] == infra_mac)
        assert cgw_metrics_get_group_infras_assigned_num(group_id) == 1

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(str(group_id), infra_mac, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra deassign result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive infra deassign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra deassign failed!')

        assert (ret_msg.value['type'] == 'infrastructure_group_infras_del_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (ret_msg.value["infra_group_infras"][0] == infra_mac)
        assert cgw_metrics_get_group_infras_assigned_num(group_id) == 0

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
    def test_single_infra_add_not_existing_group(self, test_context):
        assert test_context.kafka_producer.is_connected(),\
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(),\
            f'Cannot create default group: kafka consumer is not connected to Kafka'

        assert cgw_metrics_get_active_shards_num() == 1
        assert cgw_metrics_get_groups_assigned_num() == 0

        uuid_val = uuid.uuid4()
        group_id = 100

        # Infra add
        infra_mac = "11-22-33-44-55-66"
        test_context.kafka_producer.handle_single_device_assign(str(group_id), infra_mac, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra assign result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive infra assign result when expected')

        if ret_msg.value['success'] is True:
            raise Exception('Infra assign completed, while expected to be failed!')

        assert (ret_msg.value['type'] == 'infrastructure_group_infras_add_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (ret_msg.value["infra_group_infras"][0] == infra_mac)
        assert cgw_metrics_get_group_infras_assigned_num(group_id) == 0
        assert cgw_metrics_get_groups_assigned_num() == 0


    @pytest.mark.usefixtures("test_context",
                            "cgw_probe",
                            "kafka_probe")
    def test_single_infra_del_not_existing_group(self, test_context):
        assert test_context.kafka_producer.is_connected(),\
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(),\
            f'Cannot create default group: kafka consumer is not connected to Kafka'

        assert cgw_metrics_get_active_shards_num() == 1
        assert cgw_metrics_get_groups_assigned_num() == 0

        uuid_val = uuid.uuid4()
        group_id = 100

        # Infra add
        infra_mac = "11-22-33-44-55-66"
        test_context.kafka_producer.handle_single_device_deassign(str(group_id), infra_mac, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra deassign result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive infra deassign result when expected')

        if ret_msg.value['success'] is True:
            raise Exception('Infra deassign completed, while expected to be failed!')

        assert (ret_msg.value['type'] == 'infrastructure_group_infras_del_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (ret_msg.value["infra_group_infras"][0] == infra_mac)
        assert cgw_metrics_get_group_infras_assigned_num(group_id) == 0
        assert cgw_metrics_get_groups_assigned_num() == 0


    @pytest.mark.usefixtures("test_context",
                            "cgw_probe",
                            "kafka_probe")
    def test_single_infra_del_existing_group_not_existing_infra(self, test_context):
        assert test_context.kafka_producer.is_connected(),\
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(),\
            f'Cannot create default group: kafka consumer is not connected to Kafka'

        assert cgw_metrics_get_active_shards_num() == 1
        assert cgw_metrics_get_groups_assigned_num() == 0

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

        # Infra del
        infra_mac = "11-22-33-44-55-66"
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(str(group_id), infra_mac, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra deassign result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive infra deassign result when expected')

        if ret_msg.value['success'] is True:
            raise Exception('Infra deassign completed, while expected to be failed!')

        assert (ret_msg.value['type'] == 'infrastructure_group_infras_del_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (ret_msg.value["infra_group_infras"][0] == infra_mac)
        assert cgw_metrics_get_group_infras_assigned_num(group_id) == 0
 
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
    def test_multiple_infra_add_del(self, test_context):
        assert test_context.kafka_producer.is_connected(),\
            f'Cannot create default group: kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(),\
            f'Cannot create default group: kafka consumer is not connected to Kafka'

        assert cgw_metrics_get_active_shards_num() == 1
        assert cgw_metrics_get_groups_assigned_num() == 0

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

        # Infras add
        infra_macs = "11-22-33-44-55-XX"
        test_context.kafka_producer.handle_single_device_assign(str(group_id), infra_macs, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra assign result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive infra assign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra assign failed!')

        assert (ret_msg.value['type'] == 'infrastructure_group_infras_add_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        # assert (ret_msg.value["infra_group_infras"][0] == infra_mac)
        assert cgw_metrics_get_group_infras_assigned_num(group_id) == 256

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(str(group_id), infra_macs, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra deassign result, was expecting ' + str(uuid_val.int) + ' uuid reply')
            raise Exception('Failed to receive infra deassign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infra deassign failed!')

        assert (ret_msg.value['type'] == 'infrastructure_group_infras_del_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        #assert (ret_msg.value["infra_group_infras"][0] == infra_mac)
        assert cgw_metrics_get_group_infras_assigned_num(group_id) == 0

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