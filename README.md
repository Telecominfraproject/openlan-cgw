# openlan-cgw - What is it?
Cloud GateWay (CGW) is a Rust-based implementation of the uCentral-protocol-based Gateway layer (link).
CGW, like OWGW, manages device (Access Points and OpenLan switches) that implement and abide the uCentral protocol.
The main reasoning behind a new implementation of the GW is the horizontal scalability.
# Dependencies (runtime)
CGW requires a set of tools and services to operate and function. Some of them are embedded into the application itself and require no external utilities,
while others are required to be running for the CGW to operate.

**NOTE**: while runtime CGW depends on services like kafka, redis and PSQL, the *make* / *make all* targets
would build a complete out-of-the-box setup with default configs and container params:
- Kafka, Redis, PSQL containers would be created and attached to default - automatically created - *docker_cgw_multi_instances_network* network;
  All three (and one additional - *init-broker-container* - needed for kafka topics initialization) are all part of single docker compose file.
- CGW, while also part of the same docker compose file, yet is being partially generated.
  The reason, is that multiple CGW instances can be created within single compose-file,
and thus container details are being generated.

More information about the compose generation can be found in the
'Automated multi-CGW instances start/stop with Docker Compose' topic.

## gRPC
CGW utilizes gRPC to communicate with other CGW instances (referred to as Shards). This functionality does not depend on some external third party services.
## Kafka
CGW uses Kafka as a main North-Bound API layer for communication with NB services. CnC topic is used for commands and requests handling, CnC_Res is used to send replies/results back (CGW reads CnC and writes into CnC_Res).
### Requirements
It's required for the Kafka to have the following topics pre-made upon CGW launch:
1. "CnC"     - Kafka consumer topic
2. "CnC_Res" - Kafka producer topic
## PSQL
Application utilizes relational DB (PSQL) to store registered Infrastructure Groups as well as registered Infrastructures.
### Requirements
1. It's required for the PSQL to have the following tables pre-made upon CGW launch:
```
CREATE TABLE infrastructure_groups
(
id INT PRIMARY KEY,
reserved_size INT,
actual_size INT
);
CREATE TABLE infras
(
mac MACADDR PRIMARY KEY,
infra_group_id INT,
FOREIGN KEY(infra_group_id) REFERENCES infrastructure_groups(id) ON DELETE CASCADE
);
```
2. Default user 'cgw' and password '123' is assumed, but it can be changed through the env variables.
## Redis
fast in-memory DB that CGW uses to store all needed runtime information (InfraGroup assigned CGW id, remote CGW info - IP, gRPC port etc)
# Building
*NOTE:* The following target builds CGW and also starts up required services with default config and params
```console
$ make all
```
Two new docker images will be generated on host system:
**openlan_cgw** - image that holds CGW application itself
**cgw_build_env** - building environment docker image that is used for generating openlan_cgw
# Running
The following script can be used to launch the CGW app
```console
$ make
```
Command creates and executed (starts) docker container group consisting of cgw services
as well as third part depending services (redis, kafka, psql)

To stop the container from running (remove it) use the following cmd:
```console
$ make stop
```
Running application with default arguments might not be desired behavior.
And thus the run script utilizes the following list of *environment* variables that you can define before running it to alternate behavior of the app.
The following list is a list of environment variables you can define to configure cgw-app behavior in certain way:
```
CGW_ID                            - Shard ID
CGW_GROUPS_CAPACITY               - The CGW instance groups capacity
CGW_GROUPS_THRESHOLD              - The CGW instance groups threshold
CGW_GROUP_INFRAS_CAPACITY         - The devices capacity for group
CGW_GRPC_LISTENING_IP             - IP to bind gRPC server to (listens for gRPC requests from remote CGWs)
CGW_GRPC_LISTENING_PORT           - Port to bind gRPC server to (listens for gRPC requests from remote CGWs)
CGW_GRPC_PUBLIC_HOST              - IP or hostname for Redis record (remote CGWs will connect to this particular shard through provided host record;
                                    it's up to deployment config whether remote CGW#1 will be able to access this CGW#0, for example, through provided hostname/IP)
CGW_GRPC_PUBLIC_PORT              - PORT for Redis record
CGW_WSS_IP                        - IP to bind websocket server to (listens for incoming WSS connections from underlying devices - infrastructures)
CGW_WSS_PORT                      - PORT to bind WSS server to
CGW_WSS_CAS                       - Web socket CAS certificate file name
CGW_WSS_CERT                      - Web socket server certificate file name
CGW_WSS_KEY                       - Web socket server private key file name
CGW_KAFKA_HOST                    - IP or hostname of remote KAFKA server to connect to (NB API)
CGW_KAFKA_PORT                    - PORT of remote KAFKA server to connect to
CGW_KAFKA_TLS                     - Utilize TLS connection with Kafka broker
CGW_KAFKA_CERT                    - Client certificate to validate Kafka broker
CGW_DB_HOST                       - IP or hostname of remote database server to connect to
CGW_DB_PORT                       - PORT of remote database server to connect to
CGW_DB_USER                       - PSQL DB username (credentials) to use upon connect to DB
CGW_DB_PASSWORD                   - PSQL DB password (credentials) to use upon connect to DB
CGW_DB_TLS                        - Utilize TLS connection with DB server
CGW_REDIS_HOST                    - IP or hostname of remote redis-db server to connect to
CGW_REDIS_PORT                    - PORT of remote redis-db server to connect to
CGW_REDIS_USERNAME                - REDIS username (credentials) to use upon connect to
CGW_REDIS_PASSWORD                - REDIS password (credentials) to use upon connect to
CGW_REDIS_TLS                     - Utilize TLS connection with REDIS server
CGW_LOG_LEVEL                     - Log level to start CGW application with (debug, info)
CGW_METRICS_PORT                  - PORT of metrics to connect to
CGW_CERTS_PATH                    - Path to certificates located on host machine
CGW_ALLOW_CERT_MISMATCH           - Allow client certificate CN and device MAC address mismatch (used for OWLS)
CGW_NB_INFRA_CERTS_DIR            - Path to NB infrastructure (Redis, PostgreSQL) certificates located on host machine
CGW_NB_INFRA_TLS                  - Utilize TLS connection with NB infrastructure (Redis, PostgreSQL)
                                    If set enabled - the CGW_DB_TLS and CGW_REDIS_TLS values will be ignored and
                                    the TLS connection will be used for Redis and PostgreSQL connection
CGW_UCENTRAL_AP_DATAMODEL_URI     - Path to AP Config message JSON Validation schema:
                                    1. URI in format: "http[s]://<path>", e.g https://somewhere.com/schema.json
                                    2. Path to local file: "<path>", e.g /etc/host/schema.json
CGW_UCENTRAL_SWITCH_DATAMODEL_URI - Path to Switch Config message JSON Validation schema
```

Example of properly configured list of env variables to start CGW:
```console
$ export | grep CGW
declare -x CGW_DB_HOST="localhost"
declare -x CGW_DB_PORT="5432"
declare -x CGW_DB_USERNAME="cgw"
declare -x CGW_DB_PASSWORD="123"
declare -x CGW_DB_TLS="no"
declare -x CGW_GRPC_LISTENING_IP="127.0.0.1"
declare -x CGW_GRPC_LISTENING_PORT="50051"
declare -x CGW_GRPC_PUBLIC_HOST="localhost"
declare -x CGW_GRPC_PUBLIC_PORT="50051"
declare -x CGW_ID="0"
declare -x CGW_KAFKA_HOST="localhost"
declare -x CGW_KAFKA_PORT="9092"
declare -x CGW_KAFKA_TLS="no"
declare -x CGW_KAFKA_CERT="kafka.truststore.pem"
declare -x CGW_LOG_LEVEL="debug"
declare -x CGW_REDIS_HOST="localhost"
declare -x CGW_REDIS_PORT="6379"
declare -x CGW_REDIS_USERNAME="cgw"
declare -x CGW_REDIS_PASSWORD="123"
declare -x CGW_REDIS_TLS="no"
declare -x CGW_METRICS_PORT="8080"
declare -x CGW_WSS_IP="0.0.0.0"
declare -x CGW_WSS_PORT="15002"
declare -x CGW_WSS_CAS="cas.pem"
declare -x CGW_WSS_CERT="cert.pem"
declare -x CGW_WSS_KEY="key.pem"
declare -x CGW_CERTS_PATH="/etc/ssl/certs"
declare -x CGW_ALLOW_CERT_MISMATCH="no"
declare -x CGW_NB_INFRA_CERTS_PATH="/etc/nb_infra_certs"
declare -x CGW_NB_INFRA_TLS="no"
declare -x CGW_UCENTRAL_AP_DATAMODEL_URI="https://raw.githubusercontent.com/Telecominfraproject/wlan-ucentral-schema/main/ucentral.schema.json"
declare -x CGW_UCENTRAL_SWITCH_DATAMODEL_URI="https://raw.githubusercontent.com/Telecominfraproject/ols-ucentral-schema/main/ucentral.schema.json"
declare -x CGW_GROUPS_CAPACITY=1000
declare -x CGW_GROUPS_THRESHOLD=50
declare -x CGW_GROUP_INFRAS_CAPACITY=2000
```
# Certificates
The CGW uses two different sets of certificate configuration:
1. AP/Switch connectivity (southbound)
2. Infrastructure connectivity (northbound)

The AP/Switch connectivity uses a number of certificates to provide security (mTLS).
There are 2 types of certificates required for a normal deployment:
1. Server certificates
2. Client certificates

The certificates are accessible from CGW docker container via volume: [/etc/cgw/certs]

There are several environment variable to configure certificates path and names to be used within CGW:
1. CGW_WSS_CERT - CGW WSS Certificate
2. CGW_WSS_KEY - CGW WSS Private Key
3. CGW_WSS_CAS - Chain certificates to validate client (root/issuer)
4. CGW_CERTS_PATH - path to certificates located on host machine

The infrastructure connectivity use root certs store - the directory with trusted certificates
The environment variable to configure certificates path:
1. CGW_NB_INFRA_CERTS_PATH - path to certificates located on host machine

# Automated Testing
Automated python-based tests are located inside the *tests* directory.
Currently, tests should be run manually by changing PWD to *tests* and launching helper script *run.sh*:
```console
cd ./test
./run.sh
```
or using make target (added for convenience):
```console
make run-tests
```
*NOTE:* currently, tests are not running inside a container.
To make sure tests can communicate with CGW-environment, tests are currently
reaching environment through ports exposed to host system.
e.g. for WSS - tests try to reach 'wss://localhost:15002' by default and so on.

# Automated multi-CGW instances start/stop with Docker Compose
Automated multi-CGW start/stop based on "docker-compose-template.yml.j2" file located inside the *utils/docker* directory.
To bring-up multiple (minimum 1) CGW instances we use templated "docker-compose-template.yml.j2" file.
The "StartMultiCGW.py" script located inside the *utils/docker* directory used to:
  1. Stop all running Docker Composes.
  2. Update/generate certificates
  3. Generate "docker-compose-multi-cgw.yml" Docker Compose file that includes:
    - Kafka service
    - Redis service
    - PostgreSQL service
    - [N] CGW instances
  4. Start Docker Compose using generated "docker-compose-multi-cgw.yml" file.

The "CGW_INSTANCES_NUM" environment variable is used to specify number of CGW instances.
Makefile has 2 targets to start/stop Docker Compose
  - start-multi-cgw
  - stop-multi-cgw
