# openlan-cgw - What is it?
Cloud GateWay (CGW) is a Rust-based implementation of the uCentral-protocol-based Gateway layer (link).
CGW, like OWGW, manages device (Access Points and OpenLan switches) that implement and abide the uCentral protocol.
The main reasoning behind a new implementation of the GW is the horizontal scalability.
# Dependencies (runtime)
CGW requires a set of tools and services to operate and function. Some of them are embedded into the application itself and require no external utilities,
while others are required to be running for the CGW to operate.
## gRPC
CGW utilizes gRPC to communicate with other CGW instances (referred to as Shards). This functionality does not depend on some external thirdparty services.
## Kafka
CGW uses Kafka as a main North-Bound API layer for communication with NB services. CnC topic is used for commands and requests handling, CnC_Res is used to send replies/results back (CGW reads CnC and writes into CnC_Res).
### Requirements
It's required for the Kafka to have the following topics premade upon CGW launch:
1. "CnC"     - Kafka consumer topic
2. "CnC_Res" - Kafka producer topic
## PSQL
Application utilizes relational DB (PSQL) to store registered Infrastructure Groups as well as registered Infrastructures.
### Requirements
1. It's required for the PSQL to have the following tables premade upon CGW launch:
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
```console
$ make all
```
Two new docker images will be generated on host system:
**openlan_cgw** - image that holds CGW application itself
**cgw_build_env** - building enviroment docker image that is used for generating openlan_cgw
# Running
The following script can be used to launch the CGW app
```console
$ make run
```
Command creates and executed (starts) docker container name 'openlan_cgw'
To stop the container from running (remove it) use the following cmd:
```console
$ make stop
```
Running application with default arguments might not be desired behavior.
And thus the run script utilizes the following list of *enviroment* variables that you can define before running it to alternate behavior of the app.
The following list is a list of enviroment variables you can define to configure cgw-app behavior in certain way:
```
CGW_GRPC_IP             - IP to bind gRPC server to (listens for gRPC requests from remote CGWs)
CGW_GRPC_PORT           - PORT to bind gRPC server to
CGW_WSS_IP              - IP to bind websocket server to (listens for incoming WSS connections from underlying devices - infrastructures)
CGW_WSS_PORT            - PORT to bind WSS server to
CGW_WSS_CAS             - Web socket CAS certificate file name
CGW_WSS_CERT            - Web socket server certificate file name
CGW_WSS_KEY             - Web socket server private key file name
CGW_KAFKA_IP            - IP of remote KAFKA server to connect to (NB API)
CGW_KAFKA_PORT          - PORT of remote KAFKA server to connect to
CGW_DB_IP               - IP of remote database server to connect to
CGW_DB_PORT             - PORT of remote database server to connect to
CGW_DB_USER             - PSQL DB username (credentials) to use upon connect to DB
CGW_DB_PASS             - PSQL DB password (credentials) to use upon connect to DB
CGW_REDIS_DB_IP         - IP of remote redis-db server to connect to
CGW_REDIS_DB_PORT       - PORT of remote redis-db server to connect to
CGW_LOG_LEVEL           - Log level to start CGW application with (debug, info)
CGW_CERTS_PATH          - Path to certificates located on host machine
CGW_ALLOW_CERT_MISMATCH - Allow client certificate CN and device MAC address mismatch (used for OWLS)
```

Example of properly configured list of env variables to start CGW:
```console
$ export | grep CGW
declare -x CGW_DB_IP="127.0.0.1"           # PSQL server is located at the local host
declare -x CGW_DB_PORT="5432"
declare -x CGW_DB_USERNAME="cgw"           # PSQL login credentials (username) default 'cgw' will be used
declare -x CGW_DB_PASS="123"               # PSQL login credentials (password) default '123' will be used
declare -x CGW_GRPC_IP="127.0.0.1"         # Local default subnet is 127.0.0.1/24
declare -x CGW_GRPC_PORT="50051"
declare -x CGW_ID="1"
declare -x CGW_KAFKA_IP="127.0.0.1"        # Kafka is located at the local host
declare -x CGW_KAFKA_PORT="9092"
declare -x CGW_LOG_LEVEL="debug"
declare -x CGW_REDIS_DB_IP="127.0.0.1"     # Redis server can be found at the local host
declare -x CGW_WSS_IP="0.0.0.0"            # Accept WSS connections at all interfaces / subnets
declare -x CGW_WSS_PORT="15002"
declare -x CGW_WSS_CAS="cas.pem"
declare -x CGW_WSS_CERT="cert.pem"
declare -x CGW_WSS_KEY="key.pem"
declare -x CGW_CERTS_PATH="/etc/ssl/certs" # Path to certificates located on host machine
declare -x CGW_ALLOW_CERT_MISMATCH="no"    # Allow client certificate CN and device MAC address mismatch
```
# Certificates
The CGW uses a number of certificates to provide security.
There are 2 types of certificates required for a normal deployment:
1. Server certificates
2. Client certificates

The certificates are accessible from CGW docker container via volume: [/etc/cgw/certs]

There are several environment variable to configure certificates path and names to be used within CGW:
1. CGW_WSS_CERT - CGW WSS Certificate
2. CGW_WSS_KEY - CGW WSS Private Key
3. CGW_WSS_CAS - Chain certificates to validate client (root/issuer)
4. CGW_CERTS_PATH - path to certificates located on host machine
