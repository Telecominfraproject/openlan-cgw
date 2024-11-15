#!/bin/bash

# Separate exports for clearer visibility of _what exactly_
# we're putting in python path
export PYTHONPATH="$PYTHONPATH:$PWD"
export PYTHONPATH="$PYTHONPATH:$PWD/../utils"

ln -sf ../utils/client_simulator/sim_data sim_data
ln -sf ../utils/kafka_producer/kafka_data kafka_data
ln -sf ../utils/cert_generator/certs/client/ certs
ln -sf ../utils/cert_generator/certs/ca/ ca-certs

pip install -r requirements.txt

pytest -v
#pytest -v -s .
