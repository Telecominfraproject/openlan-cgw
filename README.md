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
## PSQL
Application utilizes relational DB (PSQL) to store registered Infrastructure Groups as well as registered Infrastructures.
## Redis
fast in-memory DB that CGW uses to store all needed runtime information (InfraGroup assigned CGW id, remote CGW info - IP, gRPC port etc)
# Building
Before building CGW you must put cert+key pair into the src folder, named *localhost.crt* and *localhost.key*.
These steps are not part of the build, and crt+key pair should exist upon running the build command.
Key and certificate will be used by the CGW internally to validate incoming WSS connections.
```console
$ make all
```
The output (CGW binaries) is then put into the ./output directory.
# Running
The following script can be used to launch the CGW app
```console
$ make run
```
Running application with default arguments might not be desired behavior.
And thus the run script utilizes the following list of *enviroment* variables that you can define before running it to alternate behavior of the app.
The following list is a list of enviroment variables you can define to configure cgw-app behavior in certain way:
```
CGW_GRPC_IP - IP to bind gRPC server to (listens for gRPC requests from remote CGWs)
CGW_GRPC_PORT - PORT to bind gRPC server to
CGW_WSS_IP - IP to bind websocket server to (listens for incoming WSS connections from underlying devices - infrastructures)
CGW_WSS_PORT - PORT to bind WSS server to
CGW_KAFKA_IP - IP of remote KAFKA server to connect to (NB API)
CGW_KAFKA_PORT - PORT of remote KAFKA server to connect to
CGW_DB_IP - IP of remote database server to connect to
CGW_DB_PORT - PORT of remote database server to connect to
CGW_REDIS_DB_IP - IP of remote redis-db server to connect to
CGW_REDIS_DB_PORT - PORT of remote redis-db server to connect to
CGW_LOG_LEVEL - log level to start CGW application with (debug, info)
```

Example of properly configured list of env variables to start CGW:
```console
$ export | grep CGW
declare -x CGW_DB_IP="172.20.10.136"       # PSQL server is at xxx.136
declare -x CGW_DB_PORT="5432"
declare -x CGW_GRPC_IP="172.20.10.153"     # local default subnet is 172.20.10.0/24
declare -x CGW_GRPC_PORT="50051"
declare -x CGW_ID="1"
declare -x CGW_KAFKA_IP="172.20.10.136"    # kafka is located at the xxx.136 host
declare -x CGW_KAFKA_PORT="9092"
declare -x CGW_LOG_LEVEL="debug"
declare -x CGW_REDIS_DB_IP="172.20.10.136" # redis server can be found at the xxx.136 host
declare -x CGW_WSS_IP="0.0.0.0"            # accept WSS connections at all interfaces / subnets
declare -x CGW_WSS_PORT="15002"
```
# Certificates
<TBD>
