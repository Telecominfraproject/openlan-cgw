#!/bin/bash

DEFAULT_ID=0
DEFAULT_LOG_LEVEL="debug"
DEFAULT_GROUPS_CAPACITY=1000
DEFAULT_GROUPS_THRESHOLD=50
DEFAULT_GROUP_INFRAS_CAPACITY=2000

# By default - use default subnet's SRC ip to listen to gRPC requests
DEFAULT_GRPC_LISTENING_IP="0.0.0.0"
DEFAULT_GRPC_LISTENING_PORT=50051
DEFAULT_GRPC_PUBLIC_HOST="openlan_cgw"
DEFAULT_GRPC_PUBLIC_PORT=50051

# By default - listen to all interfaces
DEFAULT_WSS_IP="0.0.0.0"
DEFAULT_WSS_PORT=15002
DEFAULT_WSS_T_NUM=4

DEFAULT_CERTS_PATH="`realpath ./utils/cert_generator/certs/server/`"
DEFAULT_CLIENT_CERTS_PATH="`realpath ./utils/cert_generator/certs/client/`"
DEFAULT_WSS_CAS="cas.pem"
DEFAULT_WSS_CERT="cert.pem"
DEFAULT_WSS_KEY="key.pem"
DEFAULT_CLIENT_CERT="base.crt"
DEFAULT_CLIENT_KEY="base.key"

DEFAULT_KAFKA_HOST="docker-broker-1"
DEFAULT_KAFKA_PORT=9092
DEFAULT_KAFKA_CONSUME_TOPIC="cnc"
DEFAULT_KAFKA_PRODUCE_TOPIC="cnc_res"
DEFAULT_KAFKA_TLS="no"
DEFAULT_KAFKA_CERT="kafka.truststore.pem"

DEFAULT_DB_HOST="docker-postgresql-1"
DEFAULT_DB_PORT=5432
DEFAULT_DB_NAME="cgw"
DEFAULT_DB_USER="cgw"
DEFAULT_DB_PASW="123"
DEFAULT_DB_TLS="no"

DEFAULT_REDIS_HOST="docker-redis-1"
DEFAULT_REDIS_PORT=6379
DEFAULT_REDIS_TLS="no"

DEFAULT_METRICS_PORT=8080

CONTAINER_CERTS_VOLUME="/etc/cgw/certs"
CONTAINER_NB_INFRA_CERTS_VOLUME="/etc/cgw/nb_infra/certs"
DEFAULT_NB_INFRA_TLS="no"

DEFAULT_ALLOW_CERT_MISMATCH="yes"

DEFAULT_UCENTRAL_AP_DATAMODEL_URI="https://raw.githubusercontent.com/Telecominfraproject/wlan-ucentral-schema/main/ucentral.schema.json"
DEFAULT_UCENTRAL_SWITCH_DATAMODEL_URI="https://raw.githubusercontent.com/Telecominfraproject/ols-ucentral-schema/main/ucentral.schema.json"

DEFAULT_PROXY_MODE="yes"

export CGW_LOG_LEVEL="${CGW_LOG_LEVEL:-$DEFAULT_LOG_LEVEL}"
export CGW_ID="${CGW_ID:-$DEFAULT_ID}"
export CGW_GROUPS_CAPACITY="${CGW_GROUPS_CAPACITY:-$DEFAULT_GROUPS_CAPACITY}"
export CGW_GROUPS_THRESHOLD="${CGW_GROUPS_THRESHOLD:-$DEFAULT_GROUPS_THRESHOLD}"
export CGW_GROUP_INFRAS_CAPACITY="${CGW_GROUP_INFRAS_CAPACITY:-$DEFAULT_GROUP_INFRAS_CAPACITY}"
export CGW_WSS_IP="${CGW_WSS_IP:-$DEFAULT_WSS_IP}"
export CGW_WSS_PORT="${CGW_WSS_PORT:-$DEFAULT_WSS_PORT}"
export DEFAULT_WSS_THREAD_NUM="${DEFAULT_WSS_THREAD_NUM:-$DEFAULT_WSS_T_NUM}"
export CGW_WSS_CAS="${CGW_WSS_CAS:-$DEFAULT_WSS_CAS}"
export CGW_WSS_CERT="${CGW_WSS_CERT:-$DEFAULT_WSS_CERT}"
export CGW_WSS_KEY="${CGW_WSS_KEY:-$DEFAULT_WSS_KEY}"
export CGW_GRPC_PUBLIC_HOST="${CGW_GRPC_PUBLIC_HOST:-$DEFAULT_GRPC_PUBLIC_HOST}"
export CGW_GRPC_PUBLIC_PORT="${CGW_GRPC_PUBLIC_PORT:-$DEFAULT_GRPC_PUBLIC_PORT}"
export CGW_GRPC_LISTENING_IP="${CGW_GRPC_LISTENING_IP:-$DEFAULT_GRPC_LISTENING_IP}"
export CGW_GRPC_LISTENING_PORT="${CGW_GRPC_LISTENING_PORT:-$DEFAULT_GRPC_LISTENING_PORT}"
export CGW_KAFKA_HOST="${CGW_KAFKA_HOST:-$DEFAULT_KAFKA_HOST}"
export CGW_KAFKA_PORT="${CGW_KAFKA_PORT:-$DEFAULT_KAFKA_PORT}"
export CGW_KAFKA_CONSUME_TOPIC="${CGW_KAFKA_CONSUME_TOPIC:-$DEFAULT_KAFKA_CONSUME_TOPIC}"
export CGW_KAFKA_PRODUCE_TOPIC="${CGW_KAFKA_PRODUCE_TOPIC:-$DEFAULT_KAFKA_PRODUCE_TOPIC}"
export CGW_KAFKA_TLS="${CGW_KAFKA_TLS:-$DEFAULT_KAFKA_TLS}"
export CGW_KAFKA_CERT="${CGW_KAFKA_CERT:-$DEFAULT_KAFKA_CERT}"
export CGW_DB_HOST="${CGW_DB_HOST:-$DEFAULT_DB_HOST}"
export CGW_DB_PORT="${CGW_DB_PORT:-$DEFAULT_DB_PORT}"
export CGW_DB_NAME="${CGW_DB_NAME:-$DEFAULT_DB_NAME}"
export CGW_DB_USERNAME="${CGW_DB_USER:-$DEFAULT_DB_USER}"
export CGW_DB_PASSWORD="${CGW_DB_PASS:-$DEFAULT_DB_PASW}"
export CGW_DB_TLS="${CGW_DB_TLS:-$DEFAULT_DB_TLS}"
export CGW_REDIS_HOST="${CGW_REDIS_HOST:-$DEFAULT_REDIS_HOST}"
export CGW_REDIS_PORT="${CGW_REDIS_PORT:-$DEFAULT_REDIS_PORT}"
export CGW_REDIS_TLS="${CGW_REDIS_TLS:-$DEFAULT_REDIS_TLS}"
export CGW_METRICS_PORT="${CGW_METRICS_PORT:-$DEFAULT_METRICS_PORT}"
export CGW_CERTS_PATH="${CGW_CERTS_PATH:-$DEFAULT_CERTS_PATH}"
export CGW_ALLOW_CERT_MISMATCH="${CGW_ALLOW_CERT_MISMATCH:-$DEFAULT_ALLOW_CERT_MISMATCH}"
export CGW_NB_INFRA_CERTS_PATH="${CGW_NB_INFRA_CERTS_PATH:-$DEFAULT_CERTS_PATH}"
export CGW_NB_INFRA_TLS="${CGW_NB_INFRA_TLS:-$DEFAULT_NB_INFRA_TLS}"
export CGW_UCENTRAL_AP_DATAMODEL_URI="${CGW_UCENTRAL_AP_DATAMODEL_URI:-$DEFAULT_UCENTRAL_AP_DATAMODEL_URI}"
export CGW_UCENTRAL_SWITCH_DATAMODEL_URI="${CGW_UCENTRAL_SWITCH_DATAMODEL_URI:-$DEFAULT_UCENTRAL_SWITCH_DATAMODEL_URI}"
export RUST_BACKTRACE=1
export CGW_PROXY_MODE="${CGW_PROXY_MODE:-$DEFAULT_PROXY_MODE}"

if [ -z "${CGW_REDIS_USERNAME}" ]; then
	export CGW_REDIS_USERNAME="${CGW_REDIS_USERNAME}"
fi

if [ -z "${CGW_REDIS_PASSWORD}" ]; then
	export CGW_REDIS_PASSWORD="${CGW_REDIS_PASSWORD}"
fi

if	[ ! -f $CGW_CERTS_PATH/$CGW_WSS_CERT ] ||
	[ ! -f $CGW_CERTS_PATH/$CGW_WSS_KEY ] ||
	[ ! -f $CGW_CERTS_PATH/$CGW_WSS_CAS ] ||
	[ ! -f $CGW_CERTS_PATH/$CGW_KAFKA_CERT] ||
	[ ! -f $DEFAULT_CLIENT_CERTS_PATH/$DEFAULT_CLIENT_CERT ] ||
	[ ! -f $DEFAULT_CLIENT_CERTS_PATH/$DEFAULT_CLIENT_KEY ]; then
	echo "WARNING: at specified path $CGW_CERTS_PATH either CAS, CERT or KEY is missing!"
	echo "WARNING: changing source folder for certificates to default: $DEFAULT_CERTS_PATH and generating self-signed..."
	export CGW_CERTS_PATH="$DEFAULT_CERTS_PATH";
	export CGW_WSS_CAS="$DEFAULT_WSS_CAS"
	export CGW_WSS_CERT="$DEFAULT_WSS_CERT"
	export CGW_WSS_KEY="$DEFAULT_WSS_KEY"
	export CGW_NB_INFRA_CERTS_PATH="$DEFAULT_CERTS_PATH"
	export CGW_KAFKA_CERT="$DEFAULT_KAFKA_CERT"

	cd ./utils/cert_generator/ && \
		rm ./certs/ca/*crt 2>&1 >/dev/null; \
		rm ./certs/ca/*key 2>&1 >/dev/null; \
		rm ./certs/server/*crt 2>&1 >/dev/null; \
		rm ./certs/server/*key 2>&1 >/dev/null; \
		rm ./certs/client/*crt 2>&1 >/dev/null; \
		rm ./certs/client/*key 2>&1 >/dev/null; \
		./generate_certs.sh -a && \
		./generate_certs.sh -s && \
		./generate_certs.sh -c 1 -m 02:00:00:00:00:00 && \
		cp ./certs/ca/ca.crt $DEFAULT_CERTS_PATH/$DEFAULT_WSS_CAS && \
		cp ./certs/ca/ca.crt $DEFAULT_CERTS_PATH/$DEFAULT_KAFKA_CERT && \
		cp ./certs/server/gw.crt $DEFAULT_CERTS_PATH/$DEFAULT_WSS_CERT && \
		cp ./certs/server/gw.key $DEFAULT_CERTS_PATH/$DEFAULT_WSS_KEY && \
		cp ./certs/client/*crt $DEFAULT_CLIENT_CERTS_PATH/$DEFAULT_CLIENT_CERT && \
		cp ./certs/client/*key $DEFAULT_CLIENT_CERTS_PATH/$DEFAULT_CLIENT_KEY && \
		chmod 644 $DEFAULT_CERTS_PATH/$DEFAULT_KAFKA_CERT && \
		echo "Generating self-signed certificates done!"
fi

echo "Starting CGW..."
echo "CGW LOG LEVEL                     : $CGW_LOG_LEVEL"
echo "CGW ID                            : $CGW_ID"
echo "CGW GROUPS CAPACITY/THRESHOLD     : $CGW_GROUPS_CAPACITY:$CGW_GROUPS_THRESHOLD"
echo "CGW GROUP INFRAS CAPACITY         : $CGW_GROUP_INFRAS_CAPACITY"
echo "CGW WSS THREAD NUM                : $DEFAULT_WSS_THREAD_NUM"
echo "CGW WSS IP/PORT                   : $CGW_WSS_IP:$CGW_WSS_PORT"
echo "CGW WSS CAS                       : $CGW_WSS_CAS"
echo "CGW WSS CERT                      : $CGW_WSS_CERT"
echo "CGW WSS KEY                       : $CGW_WSS_KEY"
echo "CGW GRPC PUBLIC HOST/PORT         : $CGW_GRPC_PUBLIC_HOST:$CGW_GRPC_PUBLIC_PORT"
echo "CGW GRPC LISTENING IP/PORT        : $CGW_GRPC_LISTENING_IP:$CGW_GRPC_LISTENING_PORT"
echo "CGW KAFKA HOST/PORT               : $CGW_KAFKA_HOST:$CGW_KAFKA_PORT"
echo "CGW KAFKA TOPIC                   : $CGW_KAFKA_CONSUME_TOPIC:$CGW_KAFKA_PRODUCE_TOPIC"
echo "CGW KAFKA TLS                     : $CGW_KAFKA_TLS"
echo "CGW KAFKA CERT                    : $CGW_KAFKA_CERT"
echo "CGW DB NAME                       : $CGW_DB_NAME"
echo "CGW DB HOST/PORT                  : $CGW_DB_HOST:$CGW_DB_PORT"
echo "CGW DB TLS                        : $CGW_DB_TLS"
echo "CGW REDIS HOST/PORT               : $CGW_REDIS_HOST:$CGW_REDIS_PORT"
echo "CGW REDIS TLS                     : $CGW_REDIS_TLS"
echo "CGW METRICS PORT                  : $CGW_METRICS_PORT"
echo "CGW CERTS PATH                    : $CGW_CERTS_PATH"
echo "CGW ALLOW CERT MISMATCH           : $CGW_ALLOW_CERT_MISMATCH"
echo "CGW NB INFRA CERTS PATH           : $CGW_NB_INFRA_CERTS_PATH"
echo "CGW NB INFRA TLS                  : $CGW_NB_INFRA_TLS"
echo "CGW UCENTRAL AP DATAMODEL URI     : $CGW_UCENTRAL_AP_DATAMODEL_URI"
echo "CGW UCENTRAL SWITCH DATAMODEL URI : $CGW_UCENTRAL_SWITCH_DATAMODEL_URI"
echo "CGW PROXY MODE					: $CGW_PROXY_MODE"

docker run \
	-p $CGW_WSS_PORT:$CGW_WSS_PORT \
	-p $CGW_GRPC_PUBLIC_PORT:$CGW_GRPC_PUBLIC_PORT \
	-p $CGW_METRICS_PORT:$CGW_METRICS_PORT \
	--cap-add=SYS_PTRACE --security-opt seccomp=unconfined        \
	-v $CGW_CERTS_PATH:$CONTAINER_CERTS_VOLUME                   \
	-v $CGW_NB_INFRA_CERTS_PATH:$CONTAINER_NB_INFRA_CERTS_VOLUME \
	-e CGW_LOG_LEVEL                     \
	-e CGW_ID                            \
	-e CGW_GROUPS_CAPACITY               \
	-e CGW_GROUPS_THRESHOLD              \
	-e CGW_GROUP_INFRAS_CAPACITY         \
	-e CGW_WSS_IP                        \
	-e CGW_WSS_PORT                      \
	-e DEFAULT_WSS_THREAD_NUM            \
	-e CGW_WSS_CAS                       \
	-e CGW_WSS_CERT                      \
	-e CGW_WSS_KEY                       \
	-e CGW_GRPC_LISTENING_IP             \
	-e CGW_GRPC_LISTENING_PORT           \
	-e CGW_GRPC_PUBLIC_HOST              \
	-e CGW_GRPC_PUBLIC_PORT              \
	-e CGW_KAFKA_HOST                    \
	-e CGW_KAFKA_PORT                    \
	-e CGW_KAFKA_CONSUME_TOPIC           \
	-e CGW_KAFKA_PRODUCE_TOPIC           \
	-e CGW_KAFKA_TLS                     \
	-e CGW_KAFKA_CERT                    \
	-e CGW_DB_NAME                       \
	-e CGW_DB_HOST                       \
	-e CGW_DB_PORT                       \
	-e CGW_DB_USERNAME                   \
	-e CGW_DB_PASSWORD                   \
	-e CGW_DB_TLS                        \
	-e CGW_REDIS_HOST                    \
	-e CGW_REDIS_PORT                    \
	-e CGW_REDIS_USERNAME                \
	-e CGW_REDIS_PASSWORD                \
	-e CGW_REDIS_TLS                     \
	-e CGW_FEATURE_TOPOMAP_ENABLE='1'    \
	-e CGW_METRICS_PORT                  \
	-e CGW_ALLOW_CERT_MISMATCH           \
	-e CGW_NB_INFRA_TLS                  \
	-e CGW_UCENTRAL_AP_DATAMODEL_URI     \
	-e CGW_UCENTRAL_SWITCH_DATAMODEL_URI \
	-e CGW_PROXY_MODE					 \
	-d -t --network=docker_cgw_network --name $2 $1 ucentral-cgw
