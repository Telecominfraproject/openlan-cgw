import argparse
import os
import subprocess
import shutil
from typing import Final

from jinja2 import Environment, FileSystemLoader

# CGW Docker image & container params
DEFAULT_NUMBER_OF_CGW_INSTANCES: Final[int] = 1
DOCKER_COMPOSE_TEMPLATE_FILE_NAME: Final[str] = "docker-compose-template.yml.j2"
DOCKER_COMPOSE_MULTI_CGW_FILE_NAME: Final[str] = "docker-compose-multi-cgw.yml"
BROKER_CLIENT_PROPERTIES_TEMPLATE_FILE_NAME: Final[str] = "client.properties.j2"
BROKER_CLIENT_PROPERTIES_FILE_NAME: Final[str] = "client.properties"
DEFAULT_BROKER_CERTS_PATH: Final[str] = "/bitnami/kafka/config/certs"
DEFAULT_BROKER_CONFIG_PATH: Final[str] = "/opt/bitnami/kafka/config"
# Kafka broker cert & private key
DEFAULT_BROKER_SERVER_CERT: Final[str] = "kafka.keystore.pem"
DEFAULT_BROKER_SERVER_KEY: Final[str] = "kafka.keystore.key"
# Kafka broker cert to validate client certificates
DEFAULT_BROKER_CLIENT_CERT: Final[str] = "kafka.truststore.pem"
CGW_IMAGE_BASE_NAME: Final[str] = "openlan-cgw-img"
CGW_CONTAINER_BASE_NAME: Final[str] = "openlan_cgw"

# CGW params
DEFAULT_CGW_BASE_ID: Final[int] = 0
DEFAULT_LOG_LEVEL: Final[str] = "debug"

# CGW groups & group infras params
DEFAULT_GROUPS_CAPACITY: Final[int] = 1000
DEFAULT_GROUPS_THRESHOLD: Final[int] = 50
DEFAULT_GROUP_INFRAS_CAPACITY: Final[int] = 2000

# GRPC params
DEFAULT_GRPC_LISTENING_IP: Final[str] = "0.0.0.0"
DEFAULT_GRPC_LISTENING_BASE_PORT: Final[int] = 50051
DEFAULT_GRPC_PUBLIC_BASE_PORT: Final[int] = 50051
DEFAULT_GRPC_PUBLIC_HOST: Final[str] = "openlan_cgw"

# WSS params
DEFAULT_WSS_IP: Final[str] = "0.0.0.0"
DEFAULT_WSS_BASE_PORT: Final[int] = 15002
DEFAULT_WSS_T_NUM: Final[int] = 4
DEFAULT_WSS_CAS: Final[str] = "cas.pem"
DEFAULT_WSS_CERT: Final[str] = "cert.pem"
DEFAULT_WSS_KEY: Final[str] = "key.pem"

# Kafka params
DEFAULT_KAFKA_HOST: Final[str] = "docker-broker-1"
DEFAULT_KAFKA_PORT: Final[int] = 9092
DEFAULT_KAFKA_CONSUME_TOPIC: Final[str] = "CnC"
DEFAULT_KAFKA_PRODUCE_TOPIC: Final[str] = "CnC_Res"
DEFAULT_KAFKA_TLS: Final[str] = "no"
DEFAULT_KAFKA_CERT: Final[str] = "kafka.truststore.pem"

# DB params
DEFAULT_DB_HOST: Final[str] = "docker-postgresql-1"
DEFAULT_DB_PORT: Final[int] = 5432
DEFAULT_DB_NAME: Final[str] = "cgw"
DEFAULT_DB_USER: Final[str] = "cgw"
DEFAULT_DB_PASW: Final[str] = "123"
DEFAULT_DB_TLS: Final[str] = "no"

# Redis params
DEFAULT_REDIS_HOST: Final[str] = "docker-redis-1"
DEFAULT_REDIS_PORT: Final[int] = 6379
DEFAULT_REDIS_TLS: Final[str] = "no"
DEFAULT_REDIS_USERNAME: Final[str] = ""
DEFAULT_REDIS_PASSWORD: Final[str] = ""

# Metrics params
DEFAULT_METRICS_BASE_PORT: Final[int] = 8080

# TLS params: cert volumes
DEFAULT_CERTS_PATH = "../cert_generator/certs/server/"
DEFAULT_CLIENT_CERTS_PATH = "../cert_generator/certs/client/"

CONTAINER_CERTS_VOLUME: Final[str] = "/etc/cgw/certs"
CONTAINER_NB_INFRA_CERTS_VOLUME: Final[str] = "/etc/cgw/nb_infra/certs"

# Cert & key files name
DEFAULT_CERT_GENERATOR_PATH = "../cert_generator"

DEFAULT_WSS_CAS = "cas.pem"
DEFAULT_WSS_CERT = "cert.pem"
DEFAULT_WSS_KEY = "key.pem"
DEFAULT_CLIENT_CERT = "base.crt"
DEFAULT_CLIENT_KEY = "base.key"

# TLS params
DEFAULT_NB_INFRA_TLS: Final[str] = "no"
DEFAULT_ALLOW_CERT_MISMATCH: Final[str] = "yes"

# UCentral params
DEFAULT_UCENTRAL_AP_DATAMODEL_URI: Final[str] = "https://raw.githubusercontent.com/Telecominfraproject/wlan-ucentral-schema/main/ucentral.schema.json"
DEFAULT_UCENTRAL_SWITCH_DATAMODEL_URI: Final[str] = "https://raw.githubusercontent.com/Telecominfraproject/ols-ucentral-schema/main/ucentral.schema.json"


def get_realpath(base_path) -> str:
    """
    Get absolute path from base
    """
    return str(os.path.realpath(base_path))


# Certificates update
def certificates_update(certs_path: str = DEFAULT_CERTS_PATH, client_certs_path: str = DEFAULT_CLIENT_CERTS_PATH):
    """
    Generate server & client certificates
    """
    missing_files = any(
        not os.path.isfile(os.path.join(certs_path, file))
        for file in [DEFAULT_WSS_CERT, DEFAULT_WSS_KEY, DEFAULT_WSS_CAS, DEFAULT_KAFKA_CERT, DEFAULT_BROKER_CLIENT_CERT, DEFAULT_BROKER_SERVER_CERT, DEFAULT_BROKER_SERVER_KEY]
    ) or any(
        not os.path.isfile(os.path.join(client_certs_path, file))
        for file in [DEFAULT_CLIENT_CERT, DEFAULT_CLIENT_KEY]
    )

    if missing_files:
        print(
            f"WARNING: At specified path {certs_path}, either CAS, CERT, or KEY is missing!")
        print(
            f"WARNING: Changing source folder for certificates to default: {client_certs_path} and generating self-signed...")

        cert_gen_path = get_realpath(DEFAULT_CERT_GENERATOR_PATH)

        # Clean up old certificates
        cert_subfolders = ["ca", "server", "client"]
        for subfolder in cert_subfolders:
            cert_folder = os.path.join(cert_gen_path, "certs", subfolder)
            for file in os.listdir(cert_folder):
                if file.endswith((".crt", ".key")):
                    os.remove(os.path.join(cert_folder, file))

        # Generate new certificates
        try:
            # Save current working directory
            original_dir = os.getcwd()

            os.chdir(cert_gen_path)
            print(f"Changed directory to: {os.getcwd()}")

            cert_gen_script = "./generate_certs.sh"

            subprocess.run([cert_gen_script, "-a"], check=True)
            subprocess.run([cert_gen_script, "-s"], check=True)
            subprocess.run([cert_gen_script, "-c", "1", "-m",
                           "02:00:00:00:00:00"], check=True)

            # Copy generated certificates to default paths
            shutil.copy(os.path.join(cert_gen_path, "certs", "ca", "ca.crt"), os.path.join(
                DEFAULT_CERTS_PATH, DEFAULT_WSS_CAS))
            shutil.copy(os.path.join(cert_gen_path, "certs", "server", "gw.crt"), os.path.join(
                DEFAULT_CERTS_PATH, DEFAULT_WSS_CERT))
            shutil.copy(os.path.join(cert_gen_path, "certs", "server", "gw.key"), os.path.join(
                DEFAULT_CERTS_PATH, DEFAULT_WSS_KEY))
            shutil.copy(os.path.join(cert_gen_path, "certs", "ca", "ca.crt"), os.path.join(
                DEFAULT_CERTS_PATH, DEFAULT_KAFKA_CERT))
            shutil.copy(os.path.join(cert_gen_path, "certs", "ca", "ca.crt"), os.path.join(
                DEFAULT_CERTS_PATH, DEFAULT_BROKER_CLIENT_CERT))
            shutil.copy(os.path.join(cert_gen_path, "certs", "server", "gw.crt"), os.path.join(
                DEFAULT_CERTS_PATH, DEFAULT_BROKER_SERVER_CERT))
            shutil.copy(os.path.join(cert_gen_path, "certs", "server", "gw.key"), os.path.join(
                DEFAULT_CERTS_PATH, DEFAULT_BROKER_SERVER_KEY))

            # Kafka needs key with -rw-r--r-- permission
            os.chmod(os.path.join(
                DEFAULT_CERTS_PATH, DEFAULT_BROKER_SERVER_KEY), int('644', base=8))

            for client_file in os.listdir(os.path.join(cert_gen_path, "certs", "client")):
                if client_file.endswith(".crt"):
                    shutil.copy(
                        os.path.join(cert_gen_path, "certs",
                                     "client", client_file),
                        os.path.join(DEFAULT_CLIENT_CERTS_PATH,
                                     DEFAULT_CLIENT_CERT)
                    )
                elif client_file.endswith(".key"):
                    shutil.copy(
                        os.path.join(cert_gen_path, "certs",
                                     "client", client_file),
                        os.path.join(DEFAULT_CLIENT_CERTS_PATH,
                                     DEFAULT_CLIENT_KEY)
                    )

            print("Generating self-signed certificates done!")
        except subprocess.CalledProcessError as e:
            print(f"Error while generating certificates: {e}")
        finally:
            # Change back to the original directory
            os.chdir(original_dir)
            print(f"Returned to original directory: {os.getcwd()}")


# Jinja2 template generator
def get_cgw_image_base_name() -> str:
    """
    Returns CGW Docker image base name
    """
    return CGW_IMAGE_BASE_NAME


def get_cgw_image_tag() -> str:
    """
    Returns CGW Docker image tag
    """
    tag = None

    try:
        # Check if there are any uncommitted changes (ignoring untracked files)
        status_output = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        ).stdout.strip()

        # Get the short commit hash
        commit_hash = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        ).stdout.strip()

        # Append '-dirty' if there are uncommitted changes
        if status_output:
            tag = f"{commit_hash}-dirty"
        else:
            tag = commit_hash

    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr.strip()}")

    return tag


def get_cgw_container_base_name() -> str:
    """
    Returns CGW Docker container base name
    """
    return CGW_CONTAINER_BASE_NAME


def get_cgw_instances_num() -> int:
    """
    Returns CGW instances number from env. variable,
    or default value "DEFAULT_NUMBER_OF_CGW_INSTANCES"
    """

    # Number of clients from an environment variable or fallback to default
    number_of_cgw_instances = int(
        os.getenv("CGW_INSTANCES_NUM", DEFAULT_NUMBER_OF_CGW_INSTANCES))

    return number_of_cgw_instances


def remove_docker_compose_multi_cgw_file(docker_compose_multi_cgw_file: str = DOCKER_COMPOSE_MULTI_CGW_FILE_NAME) -> int:
    """
    Remove "docker-compose-multi-cgw.yml" file
    """

    if os.path.isfile(docker_compose_multi_cgw_file):
        try:
            os.remove(docker_compose_multi_cgw_file)
        except Exception as e:
            print(
                f"Error: Filed to remove file {docker_compose_multi_cgw_file}! Error: {e}")


def generate_docker_compose_file(instances_num: int,
                                 docker_compose_template_file: str = DOCKER_COMPOSE_TEMPLATE_FILE_NAME,
                                 docker_compose_multi_cgw_file: str = DOCKER_COMPOSE_MULTI_CGW_FILE_NAME):
    """
    Generate docker compose file based on template
    """

    # 1. Get CGW image name
    image_name = get_cgw_image_base_name()

    # 2. Get CGW image tag
    image_tag = get_cgw_image_tag()

    # 3. Get CGW container name
    container_name = get_cgw_container_base_name()

    # 4. Get certs realpath
    certs_realpath = get_realpath(DEFAULT_CERTS_PATH)

    print(f'Generate Docker Compose file!')
    print(f'\tNumber of CGW instances: {instances_num}')
    print(f'\tCGW image name         : {image_name}')
    print(f'\tCGW image tag          : {image_tag}')
    print(f'\tCGW container name     : {container_name}')

    # 4. Load the Jinja2 template
    env = Environment(loader=FileSystemLoader(searchpath="."))
    template = env.get_template(docker_compose_template_file)

    # 5. Render the template with the variable
    output = template.render(cgw_instances_num=instances_num,
                             cgw_image_name=image_name,
                             cgw_image_tag=image_tag,
                             cgw_container_name=container_name,
                             cgw_base_id=DEFAULT_CGW_BASE_ID,
                             cgw_grpc_listening_ip=DEFAULT_GRPC_LISTENING_IP,
                             cgw_grpc_listening_base_port=DEFAULT_GRPC_LISTENING_BASE_PORT,
                             cgw_grpc_public_host=DEFAULT_GRPC_PUBLIC_HOST,
                             cgw_grpc_public_base_port=DEFAULT_GRPC_PUBLIC_BASE_PORT,
                             cgw_db_host=DEFAULT_DB_HOST,
                             cgw_db_port=DEFAULT_DB_PORT,
                             cgw_db_name=DEFAULT_DB_NAME,
                             cgw_db_username=DEFAULT_DB_USER,
                             cgw_db_password=DEFAULT_DB_PASW,
                             cgw_db_tls=DEFAULT_DB_TLS,
                             cgw_kafka_host=DEFAULT_KAFKA_HOST,
                             cgw_kafka_port=DEFAULT_KAFKA_PORT,
                             cgw_kafka_consumer_topic=DEFAULT_KAFKA_CONSUME_TOPIC,
                             cgw_kafka_producer_topic=DEFAULT_KAFKA_PRODUCE_TOPIC,
                             cgw_kafka_tls=DEFAULT_KAFKA_TLS,
                             cgw_kafka_cert=DEFAULT_KAFKA_CERT,
                             cgw_log_level=DEFAULT_LOG_LEVEL,
                             cgw_redis_host=DEFAULT_REDIS_HOST,
                             cgw_redis_port=DEFAULT_REDIS_PORT,
                             cgw_redis_tls=DEFAULT_REDIS_TLS,
                             cgw_redis_username=DEFAULT_REDIS_USERNAME,
                             cgw_redis_password=DEFAULT_REDIS_PASSWORD,
                             cgw_metrics_base_port=DEFAULT_METRICS_BASE_PORT,
                             cgw_wss_ip=DEFAULT_WSS_IP,
                             cgw_wss_base_port=DEFAULT_WSS_BASE_PORT,
                             cgw_wss_cas=DEFAULT_WSS_CAS,
                             cgw_wss_cert=DEFAULT_WSS_CERT,
                             cgw_wss_key=DEFAULT_WSS_KEY,
                             cgw_wss_t_num=DEFAULT_WSS_T_NUM,
                             cgw_ucentral_ap_datamodel_uri=DEFAULT_UCENTRAL_AP_DATAMODEL_URI,
                             cgw_ucentral_switch_datamodel_uri=DEFAULT_UCENTRAL_SWITCH_DATAMODEL_URI,
                             cgw_groups_capacity=DEFAULT_GROUPS_CAPACITY,
                             cgw_groups_threshold=DEFAULT_GROUPS_THRESHOLD,
                             cgw_group_infras_capacity=DEFAULT_GROUP_INFRAS_CAPACITY,
                             cgw_allow_certs_mismatch=DEFAULT_ALLOW_CERT_MISMATCH,
                             cgw_nb_infra_tls=DEFAULT_NB_INFRA_TLS,
                             container_certs_volume=CONTAINER_CERTS_VOLUME,
                             container_nb_infra_certs_volume=CONTAINER_NB_INFRA_CERTS_VOLUME,
                             default_certs_path=certs_realpath,
                             broker_certs_path=DEFAULT_BROKER_CERTS_PATH,
                             broker_server_cert=DEFAULT_BROKER_SERVER_CERT,
                             broker_server_key=DEFAULT_BROKER_SERVER_KEY,
                             broker_client_cert=DEFAULT_BROKER_CLIENT_CERT,
                             broker_config_path=DEFAULT_BROKER_CONFIG_PATH,
                             client_properties_file=BROKER_CLIENT_PROPERTIES_FILE_NAME)

    # 6. Save the rendered template as docker-compose.yml
    with open(docker_compose_multi_cgw_file, "w") as f:
        f.write(output)


def remove_broker_client_properties_file(broker_client_properties_file: str = BROKER_CLIENT_PROPERTIES_FILE_NAME) -> int:
    """
    Remove "client.properties" file
    """

    if os.path.isfile(broker_client_properties_file):
        try:
            os.remove(broker_client_properties_file)
        except Exception as e:
            print(
                f"Error: Filed to remove file {broker_client_properties_file}! Error: {e}")


def generate_broker_client_properties_file(broker_client_properties_template_file: str = BROKER_CLIENT_PROPERTIES_TEMPLATE_FILE_NAME,
                                           broker_client_properties_file: str = BROKER_CLIENT_PROPERTIES_FILE_NAME,
                                           broker_certs_path: str = DEFAULT_BROKER_CERTS_PATH,
                                           broker_client_cert: str = DEFAULT_KAFKA_CERT):
    """
    Generate broker client.properties file based on template
    """

    print(f'Generate broker client.properties file!')
    print(f'\tKafka host        : {DEFAULT_KAFKA_HOST}')
    print(f'\tKafka port        : {DEFAULT_KAFKA_PORT}')
    print(f'\tBroker certs path : {broker_certs_path}')
    print(f'\tBroker client cert: {broker_client_cert}')

    # 1. Load the Jinja2 template
    env = Environment(loader=FileSystemLoader(searchpath="."))
    template = env.get_template(broker_client_properties_template_file)

    # 2. Render the template with the variable
    output = template.render(cgw_kafka_host=DEFAULT_KAFKA_HOST,
                             cgw_kafka_port=DEFAULT_KAFKA_PORT,
                             broker_certs_path=broker_certs_path,
                             broker_client_cert=broker_client_cert)

    # 3. Save the rendered template as docker-compose.yml
    with open(broker_client_properties_file, "w") as f:
        f.write(output)


def docker_compose_up(docker_compose_file: str = "docker-compose.yml"):
    """
    Runs `docker compose up` with the specified docker-compose file.

    :param compose_file: Path to the docker-compose file (optional).
    """

    if docker_compose_file:
        if not os.path.isfile(docker_compose_file):
            print(
                f"Error: The specified compose file '{docker_compose_file}' does not exist.")
            return
        cmd = ["docker", "compose", "--file", docker_compose_file, "up", "-d"]
    else:
        cmd = ["docker", "compose", "up", "-d"]

    try:
        print(f"Running command: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
        print("Docker Compose started successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to run docker compose up. {e}")


def docker_compose_down(docker_compose_file: str = "docker-compose.yml"):
    """
    Runs `docker compose down` with the specified docker-compose file.

    :param compose_file: Path to the docker-compose file (optional).
    """

    if docker_compose_file:
        if not os.path.isfile(docker_compose_file):
            print(
                f"The specified compose file '{docker_compose_file}' does not exist.")
            return
        cmd = ["docker", "compose", "--file", docker_compose_file, "down"]
    else:
        cmd = ["docker", "compose", "down"]

    try:
        print(f"Running command: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
        print("Docker Compose stopped successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to run docker compose down. {e}")


if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(
        description="Demo application to parse arguments.")

    # Add arguments
    parser.add_argument("--start", action="store_true",
                        help="Stop all Docker Composes. Clean up and generate new compose file. Start Docker Compose.")
    parser.add_argument("--stop", action="store_true",
                        help="Stop all Docker Composes.")
    parser.add_argument("--generate-compose", action="store_true",
                        help="Generate new Docker Compose file.")
    parser.add_argument("--generate-broker-client-properties", action="store_true",
                        help="Generate new broker client.properties file.")

    # Parse the arguments
    args = parser.parse_args()

    if args.start or args.stop:
        # 1. Try to stop default docker compose
        docker_compose_down()

        # 2. Try to stop multi cgw docker compose
        docker_compose_down(DOCKER_COMPOSE_MULTI_CGW_FILE_NAME)

    if args.start or args.generate_compose or args.generate_broker_client_properties:
        # 3. Remove old multi cgw docker compose file
        if args.start or args.generate_compose:
            remove_docker_compose_multi_cgw_file()

        # 4. Remove old broker client.properties file
        if args.start or args.generate_broker_client_properties:
            remove_broker_client_properties_file()

        # 5. Update Certificates
        certificates_update()

        # 6. Generate new multi cgw docker compose file
        if args.start or args.generate_compose:
            generate_docker_compose_file(get_cgw_instances_num())

        # 7. Generate new broker client.properties file
        if args.start or args.generate_broker_client_properties:
            generate_broker_client_properties_file()

    if args.start:
        # 8. Try to start multi cgw docker compose
        docker_compose_up(DOCKER_COMPOSE_MULTI_CGW_FILE_NAME)
