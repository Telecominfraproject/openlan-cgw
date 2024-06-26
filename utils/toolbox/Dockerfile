FROM python:3.12

RUN apt-get update -y && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    zip unzip kcat vim postgresql-client redis-tools perf-tools-unstable

# client simulator
RUN mkdir -p client_simulator/src \
    client_simulator/data \
    client_simulator/certs \
    client_simulator/tipcerts
COPY client_simulator/main.py \
    client_simulator/single.py \
    client_simulator/runsingle \
    client_simulator/README.md \
    client_simulator/requirements.txt \
    client_simulator
COPY client_simulator/src/log.py \
    client_simulator/src/utils.py \
    client_simulator/src/simulation_runner.py \
    client_simulator/src/__init__.py \
    client_simulator/src
COPY client_simulator/data/message_templates.json \
    client_simulator/data
COPY client_simulator/certs/ca.crt \
    client_simulator/certs/ca.key \
    client_simulator/certs

# kafka producer
RUN mkdir -p kafka_producer/src \
    kafka_producer/data
COPY kafka_producer/main.py \
    kafka_producer/requirements.txt \
    kafka_producer
COPY kafka_producer/src/log.py \
    kafka_producer/src/utils.py \
    kafka_producer/src/producer.py \
    kafka_producer/src/cli_parser.py \
    kafka_producer/src/__init__.py \
    kafka_producer/src
COPY kafka_producer/data/message_template.json \
    kafka_producer/data

# cert generator
RUN mkdir -p cert_generator/certs/ca \
    cert_generator/certs/clients
COPY cert_generator/generate_certs.sh \
    cert_generator/ca.conf \
    cert_generator/README.md \
    cert_generator

RUN pip install -r client_simulator/requirements.txt
RUN pip install -r kafka_producer/requirements.txt
