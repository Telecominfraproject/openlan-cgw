import pytest
import ssl
import json
import time
from client_simulator.src.simulation_runner import Device as DeviceSimulator
from kafka_producer.src.producer import Producer as KafkaProducer
from kafka_producer.src.consumer import Consumer as KafkaConsumer
from psql_client.psql_client import PostgreSQLClient as PSQLClient
from redis_client.redis_client import RedisClient as RedisClient
import requests
from typing import List, Tuple
import random


# Device connection, kafka wrappers etc
class TestContext:
    @staticmethod
    def default_dev_sim_mac() -> str:
        return "02-00-00-00-00-00"

    @staticmethod
    def default_kafka_group() -> str:
        return '9999'

    def __init__(self):
        device = DeviceSimulator(
                mac=self.default_dev_sim_mac(),
                server='wss://localhost:15002',
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

        self.device_sim = device

        producer = KafkaProducer(db='localhost:9092', topic='CnC')
        consumer = KafkaConsumer(db='localhost:9092', topic='CnC_Res', consumer_timeout=12000)

        self.kafka_producer = producer
        self.kafka_consumer = consumer

        psql_client = PSQLClient(host="localhost", port=5432, database="cgw", user="cgw", password="123")
        self.psql_client = psql_client

        redis_client = RedisClient(host="localhost", port=6379)
        self.redis_client = redis_client

@pytest.fixture(scope='function')
def test_context():
    ctx = TestContext()

    yield ctx

    ctx.device_sim.disconnect()

    # Let's make sure we destroy all groups after we're done with tests.
    if ctx.kafka_producer.is_connected():
        # 1. Destroy default group
        ctx.kafka_producer.handle_single_group_delete(ctx.default_kafka_group())
        # 2. Get all groups from Redis or PostgeSQL
        groups_list = ctx.psql_client.get_all_infrastructure_groups()

        if groups_list != None:
            # 3. Interate over groups
            for group in groups_list:
                # 4. Send group_del request
                ctx.kafka_producer.handle_single_group_delete(str(group[0]))

    # We have to clear any messages after done working with kafka
    if ctx.kafka_consumer.is_connected():
        ctx.kafka_consumer.flush()

    ctx.kafka_producer.disconnect()
    ctx.kafka_consumer.disconnect()

    ctx.psql_client.disconnect()
    ctx.redis_client.disconnect()

@pytest.fixture(scope='function')
def cgw_probe(test_context):
    try:
        r = requests.get("http://localhost:8080/health")
        print("CGW status: " + str(r.status_code) + ', txt:' + r.text)
        assert r is not None and r.status_code == 200, \
                f"CGW is in a bad state (health != 200), can't proceed"
    except:
        raise Exception('CGW health fetch failed (Not running?)')

@pytest.fixture(scope='function')
def kafka_probe(test_context):
    try:
        test_context.kafka_producer.connect()
        test_context.kafka_consumer.connect()
    except:
        raise Exception('Failed to connect to kafka broker! Either CnC, CnC_Res topics are unavailable, or broker is down (not running)')

    # Let's make sure default group is always deleted.
    test_context.kafka_producer.handle_single_group_delete(test_context.default_kafka_group())

    # We have to clear any messages before we can work with kafka
    test_context.kafka_consumer.flush()

@pytest.fixture(scope='function')
def device_sim_connect(test_context):
    # Make sure we initiate connect;
    # If this thing throws - any tests that depend on this ficture would fail.
    test_context.device_sim.connect()

@pytest.fixture(scope='function')
def psql_probe(test_context):
    try:
        test_context.psql_client.connect()
    except:
        raise Exception('Failed to connect to PSQL DB!')

@pytest.fixture(scope='function')
def redis_probe(test_context):
    try:
        test_context.redis_client.connect()
    except:
        raise Exception('Failed to connect to Redis DB!')

@pytest.fixture(scope='function')
def device_sim_reconnect(test_context):
    assert test_context.device_sim._socket is not None, \
        f"Expected websocket connection to execute a reconnect while socket is not connected!"

    time.sleep(1)
    test_context.device_sim.disconnect()
    assert test_context.device_sim._socket is None, \
        f"Expected websocket connection to be NULL after disconnect."
    time.sleep(1)

    test_context.device_sim.connect()
    assert test_context.device_sim._socket is not None, \
        f"Expected websocket connection NOT to be NULL after reconnect."

@pytest.fixture(scope='function')
def device_sim_send_ucentral_connect(test_context):
    assert test_context.device_sim._socket is not None, \
        f"Expected websocket connection to send a connect ucentral event while socket is not connected!"

    test_context.device_sim.send_hello(test_context.device_sim._socket)


@pytest.fixture(scope='function')
def kafka_default_infra_group(test_context):
    assert test_context.kafka_producer.is_connected(),\
            f'Cannot create default group: kafka producer is not connected to Kafka'

    assert test_context.kafka_consumer.is_connected(),\
            f'Cannot create default group: kafka consumer is not connected to Kafka'

    uuid_val = random.randint(1, 100)
    default_group = test_context.default_kafka_group()

    test_context.kafka_producer.handle_single_group_create(default_group, uuid_val)
    ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val)
    if not ret_msg:
        print('Failed to receive create group result, was expecting ' + str(uuid_val) + ' uuid reply')
        raise Exception('Failed to receive create group result when expected')

    if ret_msg.value['success'] is False:
        print(ret_msg.value['error_message'])
        raise Exception('Default infra group creation failed!')

    group_info = test_context.psql_client.get_infrastructure_group(int(default_group))
    if not group_info:
        print(f'Failed to get group {default_group} from PSQL!')
        raise Exception('Default infra group creation failed!')

    assert group_info[0] == int(default_group)

    group_info = test_context.redis_client.get_infrastructure_group(int(default_group))
    if not group_info:
        print(f'Failed to get group {default_group} from Redis!')
        raise Exception('Default infra group creation failed!')

    assert group_info.get('gid') == default_group

@pytest.fixture(scope='function')
def kafka_default_infra(test_context):
    assert test_context.kafka_producer.is_connected(),\
            f'Cannot create default group: kafka producer is not connected to Kafka'

    assert test_context.kafka_consumer.is_connected(),\
            f'Cannot create default group: kafka consumer is not connected to Kafka'

    uuid_val = random.randint(1, 100)
    default_group = test_context.default_kafka_group()
    default_infra_mac = test_context.default_dev_sim_mac()

    test_context.kafka_producer.handle_single_device_assign(default_group, default_infra_mac, uuid_val)
    ret_msg = test_context.kafka_consumer.get_result_msg(uuid_val)
    if ret_msg is None:
        print('Failed to receive infra assign result, was expecting ' + str(uuid_val) + ' uuid reply')
        raise Exception('Failed to receive infra assign result when expected')

    if ret_msg.value['success'] is False:
        print(ret_msg.value['error_message'])
        raise Exception('Default infra assign failed!')

    infra_info = test_context.psql_client.get_infra(default_infra_mac)
    if not infra_info:
        print(f'Failed to get infra {default_infra_mac} from PSQL!')
        raise Exception('Default infra assign failed!')

    db_mac = infra_info[0]
    db_mac = db_mac.replace(":", "-", 5)
    assert db_mac == default_infra_mac

    infra_info = test_context.redis_client.get_infra(0, default_infra_mac)
    if not infra_info:
        print(f'Failed to get infra {default_infra_mac} from Redis!')
        raise Exception('Default infra assign failed!')
    
    assert infra_info.get('group_id') == int(default_group)
