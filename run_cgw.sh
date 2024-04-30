#!/bin/bash

# By default - use default subnet's SRC ip to listen to gRPC requests
DEFAULT_SRC_IP=`ip route get 1 | grep -o "src\ .*\ " |  grep -oE "\b([0-9]{1,3}\.){3}[0-9]{1,3}\b"`
DEFAULT_GRPC_PORT=50051

# By default - listen to all interfaces
DEFAULT_WSS_IP="0.0.0.0"
DEFAULT_WSS_PORT=15002

DEFAULT_KAFKA_IP="127.0.0.1"
DEFAULT_KAFKA_PORT=9092

DEFAULT_DB_IP="127.0.0.1"
DEFAULT_DB_PORT=5432

DEFAULT_REDIS_DB_IP="127.0.0.1"
DEFAULT_REDIS_DB_PORT=6379

ID="${CGW_ID:-1}"

GRPC_IP="${CGW_GRPC_IP:-$DEFAULT_SRC_IP}"
GRPC_PORT="${CGW_GRPC_PORT:-$DEFAULT_GRPC_PORT}"
WSS_LOCAL_IP="${CGW_WSS_IP:-$DEFAULT_WSS_IP}"
WSS_LOCAL_PORT="${CGW_WSS_PORT:-$DEFAULT_WSS_PORT}"
KAFKA_IP="${CGW_KAFKA_IP:-$DEFAULT_KAFKA_IP}"
KAFKA_PORT="${CGW_KAFKA_PORT:-$DEFAULT_KAFKA_PORT}"
DB_IP="${CGW_DB_IP:-$DEFAULT_DB_IP}"
DB_PORT="${CGW_DB_PORT:-$DEFAULT_DB_PORT}"
DB_USR="${CGW_DB_USER:-cgw}"
DB_PASS="${CGW_DB_PASS:-123}"
REDIS_DB_IP="${CGW_REDIS_DB_IP:-$DEFAULT_REDIS_DB_IP}"
REDIS_DB_PORT="${CGW_REDIS_DB_PORT:-$DEFAULT_REDIS_DB_PORT}"
LOG_LEVEL="${CGW_LOG_LEVEL:-info}"

echo "Starting CGW..."
echo "CGW ID: $ID"
echo "LOG_LEVEL: $LOG_LEVEL"
echo "GRPC: $GRPC_IP:$GRPC_PORT"
echo "WSS: $WSS_LOCAL_IP:$WSS_LOCAL_PORT"
echo "KAFKA: $KAFKA_IP:$KAFKA_PORT"
echo "DB: $DB_IP:$DB_PORT"
echo "REDIS_DB: $REDIS_DB_IP:$REDIS_DB_PORT"

docker run -d -t --network=host --name $2 $1 ucentral-cgw \
	--cgw-id $ID \
	--grpc-ip $GRPC_IP --grpc-port $GRPC_PORT \
	--wss-ip $WSS_LOCAL_IP --wss-port $WSS_LOCAL_PORT \
	--kafka-ip $KAFKA_IP --kafka-port $KAFKA_PORT \
	--db-ip $DB_IP --db-port $DB_PORT \
	--db-username $DB_USR --db-password $DB_PASS \
	--redis-db-ip $REDIS_DB_IP --redis-db-port $REDIS_DB_PORT \
	$LOG_LEVEL
