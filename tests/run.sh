#!/bin/bash

# Separate exports for clearer visibility of _what exactly_
# we're putting in python path

rm -rf /tmp/cgw_tests_runner;
mkdir /tmp/cgw_tests_runner && \
	cp -rf ../tests /tmp/cgw_tests_runner/ && \
	cp -rf ../utils /tmp/cgw_tests_runner/;

cd /tmp/cgw_tests_runner/tests

export PYTHONPATH="$PYTHONPATH:$PWD"
export PYTHONPATH="$PYTHONPATH:$PWD/../utils"

ln -sf ../utils/client_simulator/sim_data sim_data
ln -sf ../utils/kafka_producer/kafka_data kafka_data
ln -sf ../utils/cert_generator/certs/client/ certs
ln -sf ../utils/cert_generator/certs/ca/ ca-certs
ln -sf ../utils/client_simulator/ client_simulator
ln -sf ../utils/kafka_producer/ kafka_producer
ln -sf ../utils/psql_client/ psql_client
ln -sf ../utils/redis_client/ redis_client

pip install -r requirements.txt

pytest -v
#pytest -v -s test_cgw_infras.py
