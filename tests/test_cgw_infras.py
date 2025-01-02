import pytest
import uuid
import random
from randmac import RandMac

from metrics import cgw_metrics_get_groups_assigned_num, \
    cgw_metrics_get_group_infras_assigned_num, \
    cgw_metrics_get_group_ifras_capacity


class TestCgwInfra:
    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_single_infra_add_del(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard infro from Redis
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
        # because the overall command succeded
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
        # because the overall command succeded
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
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe")
    def test_single_infra_add_not_existing_group(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard infro from Redis
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

        if ret_msg.value['success'] is True:
            raise Exception(
                'Infra assign completed, while expected to be failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_add_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (ret_msg.value["failed_infras"][0] == infra_mac)
        # We expect a single 'failed_infra' in response
        assert (len(list(ret_msg.value["failed_infras"])) == 1)
        assert cgw_metrics_get_group_infras_assigned_num(group_id) == 0

        # Get shard infro from Redis
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
                             "redis_probe")
    def test_single_infra_del_not_existing_group(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard infro from Redis
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

        # Infra del
        infra_mac = "11-22-33-44-55-66"
        test_context.kafka_producer.handle_single_device_deassign(
            str(group_id), infra_mac, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra deassign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra deassign result when expected')

        if ret_msg.value['success'] is True:
            raise Exception(
                'Infra deassign completed, while expected to be failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_del_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (ret_msg.value["failed_infras"][0] == infra_mac)
        # We expect a single 'failed_infra' in response
        assert (len(list(ret_msg.value["failed_infras"])) == 1)
        assert cgw_metrics_get_group_infras_assigned_num(group_id) == 0

        # Get shard infro from Redis
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
    def test_single_infra_del_existing_group_not_existing_infra(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard infro from Redis
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
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 1

        # Infra del
        infra_mac = "11-22-33-44-55-66"
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_single_device_deassign(
            str(group_id), infra_mac, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra deassign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra deassign result when expected')

        if ret_msg.value['success'] is True:
            raise Exception(
                'Infra deassign completed, while expected to be failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_del_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (ret_msg.value["failed_infras"][0] == infra_mac)
        # We expect a single 'failed_infra' in response
        assert (len(list(ret_msg.value["failed_infras"])) == 1)

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

        # Validate number of assigned infras
        assert group_info_psql[2] == int(group_info_redis.get(
            'infras_assigned')) == cgw_metrics_get_group_infras_assigned_num(group_id) == 0

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
    def test_multiple_infras_add_del(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard infro from Redis
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
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 1

        # Infra add
        infras_num = random.randint(1, 10)
        infras_mac = set()
        while infras_mac.__len__() != infras_num:
            infras_mac.add(str(RandMac(mac="00-00-00-00-00-00")))

        infras_mac_list = list(infras_mac)
        test_context.kafka_producer.handle_multiple_devices_assign(
            str(group_id), infras_mac_list, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra assign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra assign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infras assign failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_add_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        # We don't expect to have even a single 'failed_infra',
        # because the overall command succeded
        assert (len(list(ret_msg.value["failed_infras"])) == 0)

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infras assign failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infras assign failed!')

        # Get infras from Redis Infra Cache
        redis_infras_list = []
        for infra_mac in infras_mac_list:
            infra_info_redis = test_context.redis_client.get_infra(
                default_shard_id, infra_mac)
            if not infra_info_redis:
                print(f'Failed to get infra {infra_mac} info from Redis!')
                raise Exception('Infras assign failed!')
            else:
                redis_infras_list.append(infra_info_redis)

        # Get infras from PSQL
        psql_infras_info = test_context.psql_client.get_infras_by_group_id(
            group_id)
        if not psql_infras_info:
            print(
                f'Failed to get infras {infra_mac} info from PSQL, group ID {group_id}!')
            raise Exception('Infras assign failed!')

        # Validate infras assigned number
        assert redis_infras_list.__len__() == psql_infras_info.__len__() == int(group_info_redis.get(
            'infras_assigned')) == cgw_metrics_get_group_infras_assigned_num(group_id) == infras_num

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_multiple_devices_deassign(
            str(group_id), infras_mac_list, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra deassign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infras deassign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infras deassign failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_del_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        # We don't expect to have even a single 'failed_infra',
        # because the overall command succeded
        assert (len(list(ret_msg.value["failed_infras"])) == 0)
        assert cgw_metrics_get_group_infras_assigned_num(group_id) == 0

        # Get infras from Redis Infra Cache
        for infra_mac in infras_mac_list:
            infra_info_redis = test_context.redis_client.get_infra(
                default_shard_id, infra_mac)
            if infra_info_redis != None:
                print(f'Unexpectedly get infra {infra_mac} info from Redis!')
                raise Exception('Infras deassign failed!')

        # Get infras from PSQL
        psql_infras_info = test_context.psql_client.get_infras_by_group_id(
            group_id)
        if psql_infras_info != []:
            print(
                f'Unexpectedly get infras {infra_mac} from PSQL, group ID {group_id}!')
            raise Exception('Infras deassign failed!')

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infras deassign failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infras deassign failed!')

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
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_infras_capacity_overflow(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard infro from Redis
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
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 1

        # Infra add
        infras_num = cgw_metrics_get_group_ifras_capacity()
        infras_mac = set()
        while infras_mac.__len__() != infras_num:
            infras_mac.add(str(RandMac(mac="00-00-00-00-00-00")))

        infras_mac_list = list(infras_mac)
        test_context.kafka_producer.handle_multiple_devices_assign(
            str(group_id), infras_mac_list, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra assign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra assign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infras assign failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_add_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        # We don't expect to have even a single 'failed_infra',
        # because the overall command succeded
        assert (len(list(ret_msg.value["failed_infras"])) == 0)

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infras assign failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infras assign failed!')

        # Get infras from Redis Infra Cache
        redis_infras_list = []
        for infra_mac in infras_mac_list:
            infra_info_redis = test_context.redis_client.get_infra(
                default_shard_id, infra_mac)
            if not infra_info_redis:
                print(f'Failed to get infra {infra_mac} info from Redis!')
                raise Exception('Infras assign failed!')
            else:
                redis_infras_list.append(infra_info_redis)

        # Get infras from PSQL
        psql_infras_info = test_context.psql_client.get_infras_by_group_id(
            group_id)
        if not psql_infras_info:
            print(
                f'Failed to get infras {infra_mac} info from PSQL, group ID {group_id}!')
            raise Exception('Infras assign failed!')

        # Validate infras assigned number
        assert redis_infras_list.__len__() == psql_infras_info.__len__() == int(group_info_redis.get(
            'infras_assigned')) == cgw_metrics_get_group_infras_assigned_num(group_id) == infras_num

        # Add few more infra to simulate capacity overflow
        uuid_val = uuid.uuid4()
        new_infras_mac = ["11-22-33-44-55-66", "66-55-44-33-22-11"]
        test_context.kafka_producer.handle_multiple_devices_assign(
            str(group_id), new_infras_mac, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra assign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra assign result when expected')

        if ret_msg.value['success'] is True:
            raise Exception(
                'Infra assign completed, while expected to be failed due to ifras capacity overflow!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_add_response')
        assert (str(ret_msg.value['error_message']) ==
                f"Failed to create few MACs from infras list (partial create), gid {group_id}, uuid {str(uuid_val)}")
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        # We expect the 'failed_infra' to match 1:1 the list we've provided,
        # as they all should fail to be added due to overflow
        assert (ret_msg.value["failed_infras"] == new_infras_mac)

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infras assign failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infras assign failed!')

        # Get infras from Redis Infra Cache
        redis_infras_list = []
        for infra_mac in infras_mac_list:
            infra_info_redis = test_context.redis_client.get_infra(
                default_shard_id, infra_mac)
            if not infra_info_redis:
                print(f'Failed to get infra {infra_mac} info from Redis!')
                raise Exception('Infras assign failed!')
            else:
                redis_infras_list.append(infra_info_redis)

        # Get infras from PSQL
        psql_infras_info = test_context.psql_client.get_infras_by_group_id(
            group_id)
        if not psql_infras_info:
            print(
                f'Failed to get infras {infra_mac} info from PSQL, group ID {group_id}!')
            raise Exception('Infras assign failed!')

        # Validate infras assigned number
        assert redis_infras_list.__len__() == psql_infras_info.__len__() == int(group_info_redis.get(
            'infras_assigned')) == cgw_metrics_get_group_infras_assigned_num(group_id) == infras_num

        # Infra del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_multiple_devices_deassign(
            str(group_id), infras_mac_list, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra deassign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra deassign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infras deassign failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_del_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        # We don't expect to have even a single 'failed_infra',
        # because the overall command succeded
        assert (len(list(ret_msg.value["failed_infras"])) == 0)

        # Get infras from Redis Infra Cache
        for infra_mac in infras_mac_list:
            infra_info_redis = test_context.redis_client.get_infra(
                default_shard_id, infra_mac)
            if infra_info_redis != None:
                print(f'Unexpectedly get infra {infra_mac} info from Redis!')
                raise Exception('Infras deassign failed!')

        # Get infras from PSQL
        psql_infras_info = test_context.psql_client.get_infras_by_group_id(
            group_id)
        if psql_infras_info != []:
            print(
                f'Unexpectedly get infras {infra_mac} from PSQL, group ID {group_id}!')
            raise Exception('Infras deassign failed!')

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infras deassign failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infras deassign failed!')

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
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0

    # Test scenario: add infras with MACs (A, B, C, D, E, F, G)
    # First : add MACs (A, B, C) - expected all should be added succesfully
    # Second: add MACs (D, A, B) - expected partial create (dup MACs at the end)    - only MAC (D) should be added succesfully
    # Third : add MACs (A, B, E) - expected partial create (dup MACs at the begin)  - only MAC (E) should be added succesfully
    # Forth : add MACs (F, A, G) - expected partial create (dup MACs at the middle) - only MAC (F, D) should be added succesfully
    # We want to make sure that - does not matter where in infra MACs list duplicate infra MACs exists - it wount affect infra add

    @pytest.mark.usefixtures("test_context",
                             "cgw_probe",
                             "kafka_probe",
                             "redis_probe",
                             "psql_probe")
    def test_partial_infras_add(self, test_context):
        assert test_context.kafka_producer.is_connected(), \
            f'Kafka producer is not connected to Kafka'

        assert test_context.kafka_consumer.is_connected(), \
            f'Kafka consumer is not connected to Kafka'

        default_shard_id = test_context.default_shard_id()

        # Get shard infro from Redis
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
                f'Failed to get shard {default_shard_id} info from Redis!')

        # Validate number of assigned groups
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 1

        # Infras add (A, B, C)
        infras_mac_list = ["11-22-33-44-55-01",
                           "11-22-33-44-55-02", "11-22-33-44-55-03"]
        test_context.kafka_producer.handle_multiple_devices_assign(
            str(group_id), infras_mac_list, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra assign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra assign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infras assign failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_add_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        # We don't expect to have even a single 'failed_infra',
        # because the overall command succeded
        assert (len(list(ret_msg.value["failed_infras"])) == 0)

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infras assign failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infras assign failed!')

        # Get infras from Redis Infra Cache
        redis_infras_list = []
        for infra_mac in infras_mac_list:
            infra_info_redis = test_context.redis_client.get_infra(
                default_shard_id, infra_mac)
            if not infra_info_redis:
                print(f'Failed to get infra {infra_mac} info from Redis!')
                raise Exception('Infras assign failed!')
            else:
                redis_infras_list.append(infra_info_redis)

        # Get infras from PSQL
        psql_infras_info = test_context.psql_client.get_infras_by_group_id(
            group_id)
        if not psql_infras_info:
            print(
                f'Failed to get infras {infra_mac} info from PSQL, group ID {group_id}!')
            raise Exception('Infras assign failed!')

        # Validate infras assigned number
        assert redis_infras_list.__len__() == psql_infras_info.__len__() == int(group_info_redis.get(
            'infras_assigned')) == cgw_metrics_get_group_infras_assigned_num(group_id) == 3

        # Infras add (D, A, B)
        infras_mac_list_new = ["11-22-33-44-55-04",
                               "11-22-33-44-55-01", "11-22-33-44-55-02"]
        test_context.kafka_producer.handle_multiple_devices_assign(
            str(group_id), infras_mac_list_new, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra assign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra assign result when expected')

        if ret_msg.value['success'] is True:
            print(ret_msg.value['error_message'])
            raise Exception(
                'Infras assign completed, while expected to be failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_add_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (list(ret_msg.value["failed_infras"]) == [
                "11-22-33-44-55-01", "11-22-33-44-55-02"])

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infras assign failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infras assign failed!')

        # Get infras from Redis Infra Cache
        infras_expected_to_be_installed = list(
            set(infras_mac_list + infras_mac_list_new))
        redis_infras_list = []
        for infra_mac in infras_expected_to_be_installed:
            infra_info_redis = test_context.redis_client.get_infra(
                default_shard_id, infra_mac)
            if not infra_info_redis:
                print(f'Failed to get infra {infra_mac} info from Redis!')
                raise Exception('Infras assign failed!')
            else:
                redis_infras_list.append(infra_info_redis)

        # Get infras from PSQL
        psql_infras_info = test_context.psql_client.get_infras_by_group_id(
            group_id)
        if not psql_infras_info:
            print(
                f'Failed to get infras {infra_mac} info from PSQL, group ID {group_id}!')
            raise Exception('Infras assign failed!')

        # Validate infras assigned number
        assert redis_infras_list.__len__() == psql_infras_info.__len__() == int(group_info_redis.get(
            'infras_assigned')) == cgw_metrics_get_group_infras_assigned_num(group_id) == 4

        # Infras add (A, B, E)
        infras_mac_list_new = ["11-22-33-44-55-01",
                               "11-22-33-44-55-02", "11-22-33-44-55-05"]
        test_context.kafka_producer.handle_multiple_devices_assign(
            str(group_id), infras_mac_list_new, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra assign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra assign result when expected')

        if ret_msg.value['success'] is True:
            print(ret_msg.value['error_message'])
            raise Exception(
                'Infras assign completed, while expected to be failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_add_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (list(ret_msg.value["failed_infras"]) == [
                "11-22-33-44-55-01", "11-22-33-44-55-02"])

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infras assign failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infras assign failed!')

        # Get infras from Redis Infra Cache
        infras_expected_to_be_installed = list(
            set(infras_expected_to_be_installed + infras_mac_list_new))
        redis_infras_list = []
        for infra_mac in infras_expected_to_be_installed:
            infra_info_redis = test_context.redis_client.get_infra(
                default_shard_id, infra_mac)
            if not infra_info_redis:
                print(f'Failed to get infra {infra_mac} info from Redis!')
                raise Exception('Infras assign failed!')
            else:
                redis_infras_list.append(infra_info_redis)

        # Get infras from PSQL
        psql_infras_info = test_context.psql_client.get_infras_by_group_id(
            group_id)
        if not psql_infras_info:
            print(
                f'Failed to get infras {infra_mac} info from PSQL, group ID {group_id}!')
            raise Exception('Infras assign failed!')

        # Validate infras assigned number
        assert redis_infras_list.__len__() == psql_infras_info.__len__() == int(group_info_redis.get(
            'infras_assigned')) == cgw_metrics_get_group_infras_assigned_num(group_id) == 5

        # Infras add (F, A, G)
        infras_mac_list_new = ["11-22-33-44-55-06",
                               "11-22-33-44-55-01", "11-22-33-44-55-07"]
        test_context.kafka_producer.handle_multiple_devices_assign(
            str(group_id), infras_mac_list_new, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra assign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infra assign result when expected')

        if ret_msg.value['success'] is True:
            print(ret_msg.value['error_message'])
            raise Exception(
                'Infras assign completed, while expected to be failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_add_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        assert (list(ret_msg.value["failed_infras"]) == ["11-22-33-44-55-01"])

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infras assign failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infras assign failed!')

        # Get infras from Redis Infra Cache
        infras_expected_to_be_installed = list(
            set(infras_expected_to_be_installed + infras_mac_list_new))
        redis_infras_list = []
        for infra_mac in infras_expected_to_be_installed:
            infra_info_redis = test_context.redis_client.get_infra(
                default_shard_id, infra_mac)
            if not infra_info_redis:
                print(f'Failed to get infra {infra_mac} info from Redis!')
                raise Exception('Infras assign failed!')
            else:
                redis_infras_list.append(infra_info_redis)

        # Get infras from PSQL
        psql_infras_info = test_context.psql_client.get_infras_by_group_id(
            group_id)
        if not psql_infras_info:
            print(
                f'Failed to get infras {infra_mac} info from PSQL, group ID {group_id}!')
            raise Exception('Infras assign failed!')

        # Validate infras assigned number
        assert redis_infras_list.__len__() == psql_infras_info.__len__() == int(group_info_redis.get(
            'infras_assigned')) == cgw_metrics_get_group_infras_assigned_num(group_id) == 7

        # Infras del
        uuid_val = uuid.uuid4()
        test_context.kafka_producer.handle_multiple_devices_deassign(
            str(group_id), infras_expected_to_be_installed, uuid_val.int)
        ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val.int)
        if ret_msg is None:
            print('Failed to receive infra deassign result, was expecting ' +
                  str(uuid_val.int) + ' uuid reply')
            raise Exception(
                'Failed to receive infras deassign result when expected')

        if ret_msg.value['success'] is False:
            print(ret_msg.value['error_message'])
            raise Exception('Infras deassign failed!')

        assert (ret_msg.value['type'] ==
                'infrastructure_group_infras_del_response')
        assert (int(ret_msg.value["infra_group_id"]) == group_id)
        assert ((uuid.UUID(ret_msg.value['uuid']).int) == (uuid_val.int))
        # We don't expect to have even a single 'failed_infra',
        # because the overall command succeded
        assert (len(list(ret_msg.value["failed_infras"])) == 0)

        # Get infras from Redis Infra Cache
        for infra_mac in infras_mac_list:
            infra_info_redis = test_context.redis_client.get_infra(
                default_shard_id, infra_mac)
            if infra_info_redis != None:
                print(f'Unexpectedly get infra {infra_mac} info from Redis!')
                raise Exception('Infras deassign failed!')

        # Get infras from PSQL
        psql_infras_info = test_context.psql_client.get_infras_by_group_id(
            group_id)
        if psql_infras_info != []:
            print(
                f'Unexpectedly get infras {infra_mac} from PSQL, group ID {group_id}!')
            raise Exception('Infras deassign failed!')

        # Get group info from Redis
        group_info_redis = test_context.redis_client.get_infrastructure_group(
            group_id)
        if not group_info_redis:
            print(f'Failed to get group {group_id} info from Redis!')
            raise Exception('Infras deassign failed!')

        # Get group info from PSQL
        group_info_psql = test_context.psql_client.get_infrastructure_group(
            group_id)
        if not group_info_psql:
            print(f'Failed to get group {group_id} info from PSQL!')
            raise Exception('Infras deassign failed!')

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
        assert int(shard_info.get('assigned_groups_num')
                   ) == cgw_metrics_get_groups_assigned_num() == 0
