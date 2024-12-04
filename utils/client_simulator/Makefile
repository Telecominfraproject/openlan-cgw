IMG_NAME=cgw-client-sim
CONTAINER_NAME=cgw_client_sim
MAC?=XX:XX:XX:XX:XX:XX
COUNT?=1000
URL=wss://localhost:15002
CA_CERT_PATH?=$(PWD)/../cert_generator/certs/ca
CLIENT_CERT_PATH?=$(PWD)/../cert_generator/certs/client
MSG_INTERVAL?=10
MSG_SIZE?=1000

.PHONY: build spawn stop start

build:
	docker build -t ${IMG_NAME} .

spawn:
	docker run --name "${CONTAINER_NAME}_${COUNT}_$(subst :,-,$(MAC))" \
		-d --rm --network host \
		-v $(PWD):/opt/client_simulator \
		-v ${CA_CERT_PATH}:/etc/ca \
		-v ${CLIENT_CERT_PATH}:/etc/certs \
		${IMG_NAME} \
		python3 main.py -M ${MAC} -N ${COUNT} -s ${URL} \
			--ca-cert /etc/ca/ca.crt \
			--client-certs-path /etc/certs \
			--msg-interval ${MSG_INTERVAL} \
			--payload-size ${MSG_SIZE} \
			--wait-for-signal

stop:
	docker stop $$(docker ps -q -f name=$(CONTAINER_NAME))

start:
	docker kill -s SIGUSR1 $$(docker ps -q -f name=$(CONTAINER_NAME))