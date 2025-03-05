#!/bin/bash

DEFAULT_ID=0
DEFAULT_LOG_LEVEL="debug"
DEFAULT_GROUPS_CAPACITY=1000
DEFAULT_GROUPS_THRESHOLD=50
DEFAULT_GROUP_INFRAS_CAPACITY=2000

# By default - listen to all interfaces
DEFAULT_WSS_IP="0.0.0.0"
DEFAULT_WSS_PORT=443
DEFAULT_WSS_T_NUM=4

DEFAULT_CERTS_PATH="`realpath ./utils/cert_generator/certs/server/`"
DEFAULT_CLIENT_CERTS_PATH="`realpath ./utils/cert_generator/certs/client/`"
DEFAULT_WSS_CAS="cas.pem"
DEFAULT_WSS_CERT="cert.pem"
DEFAULT_WSS_KEY="key.pem"
DEFAULT_CLIENT_CERT="base.crt"
DEFAULT_CLIENT_KEY="base.key"

CONTAINER_CERTS_VOLUME="/etc/cgw/certs"

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
export CGW_CERTS_PATH="${CGW_CERTS_PATH:-$DEFAULT_CERTS_PATH}"
export RUST_BACKTRACE=1

if	[ ! -f $CGW_CERTS_PATH/$CGW_WSS_CERT ] ||
	[ ! -f $CGW_CERTS_PATH/$CGW_WSS_KEY ] ||
	[ ! -f $CGW_CERTS_PATH/$CGW_WSS_CAS ] ||
	[ ! -f $DEFAULT_CLIENT_CERTS_PATH/$DEFAULT_CLIENT_CERT ] ||
	[ ! -f $DEFAULT_CLIENT_CERTS_PATH/$DEFAULT_CLIENT_KEY ]; then
	echo "WARNING: at specified path $CGW_CERTS_PATH either CAS, CERT or KEY is missing!"
	echo "WARNING: changing source folder for certificates to default: $DEFAULT_CERTS_PATH and generating self-signed..."
	export CGW_CERTS_PATH="$DEFAULT_CERTS_PATH";
	export CGW_WSS_CAS="$DEFAULT_WSS_CAS"
	export CGW_WSS_CERT="$DEFAULT_WSS_CERT"
	export CGW_WSS_KEY="$DEFAULT_WSS_KEY"

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
		cp ./certs/server/gw.crt $DEFAULT_CERTS_PATH/$DEFAULT_WSS_CERT && \
		cp ./certs/server/gw.key $DEFAULT_CERTS_PATH/$DEFAULT_WSS_KEY && \
		cp ./certs/client/*crt $DEFAULT_CLIENT_CERTS_PATH/$DEFAULT_CLIENT_CERT && \
		cp ./certs/client/*key $DEFAULT_CLIENT_CERTS_PATH/$DEFAULT_CLIENT_KEY && \
		echo "Generating self-signed certificates done!"
fi

echo "Starting Proxy CGW..."
echo "CGW LOG LEVEL                 : $CGW_LOG_LEVEL"
echo "CGW ID                        : $CGW_ID"
echo "CGW GROUPS CAPACITY/THRESHOLD : $CGW_GROUPS_CAPACITY:$CGW_GROUPS_THRESHOLD"
echo "CGW GROUP INFRAS CAPACITY     : $CGW_GROUP_INFRAS_CAPACITY"
echo "CGW WSS THREAD NUM            : $DEFAULT_WSS_THREAD_NUM"
echo "CGW WSS IP/PORT               : $CGW_WSS_IP:$CGW_WSS_PORT"
echo "CGW WSS CAS                   : $CGW_WSS_CAS"
echo "CGW WSS CERT                  : $CGW_WSS_CERT"
echo "CGW WSS KEY                   : $CGW_WSS_KEY"
echo "CGW CERTS PATH                : $CGW_CERTS_PATH"

docker run \
	-p $CGW_WSS_PORT:$CGW_WSS_PORT \
	--cap-add=SYS_PTRACE --security-opt seccomp=unconfined        \
	-v $CGW_CERTS_PATH:$CONTAINER_CERTS_VOLUME                   \
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
	-d -t --network=docker_cgw_network --name $2 $1
