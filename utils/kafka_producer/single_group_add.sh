#!/bin/bash

# Although we're looking for broker at localhost,
# broker can still direct us to some <docker-broker-1>
# so it's up to the caller to either run this script inside
# the same network instance as broker, or create a static
# hostname entry to point <docker-broker-1>, for example,
# to whenever it resides.
#
# ARGS:
# $1 - group id
./run.sh -s localhost:9092 -c 1 --new-group $1 0 some_name_0