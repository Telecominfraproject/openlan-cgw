#!/bin/sh

# Although we're looking for broker at localhost,
# broker can still direct us to some <docker-broker-1>
# so it's up to the caller to either run this script inside
# the same network instance as broker, or create a static
# hostname entry to point <docker-broker-1>, for example,
# to whenever it resides.
#
# ARGS:
# $1 - group id
# $2 - mac address
# $3 - file name containing complete uCentral request
./run.sh --send-to-group $1 --send-to-mac $2^1 -c 1  -m "`cat $3`" 2>/dev/null
