#!/bin/bash
TEMPLATE="02:XX:XX:XX:XX:XX"
HEXCHARS="0123456789ABCDEF"
CONF_FILE=ca.conf
CERT_DIR=certs
CA_DIR=$CERT_DIR/ca
CA_CERT=$CA_DIR/ca.crt
CA_KEY=$CA_DIR/ca.key
SERVER_DIR=$CERT_DIR/server
CLIENT_DIR=$CERT_DIR/client
METHOD_FAST=y

usage()
{
	echo "Usage: $0 [options]"
	echo
	echo "options:"
	echo "-h"
	echo -e "\tprint this help"
	echo "-a"
	echo -e "\tgenerate CA key and certificate"
	echo "-s"
	echo -e "\tgenerate server key and certificate; sign the certificate"
	echo -e "\tusing the CA certificate"
	echo "-c NUMBER"
	echo -e "\tgenerate *NUMBER* of client keys and certificates; sign"
	echo -e "\tall of the certificates using the CA certificate"
	echo "-m MASK"
	echo -e "\tspecify custom MAC addr mask"
	echo "-o"
	echo -e "\tslow mode"
}

rand_mac()
{
	local MAC=$TEMPLATE
	while [[ $MAC =~ "X" || $MAC =~ "x" ]]
	do
		MAC=${MAC/[xX]/${HEXCHARS:$(( $RANDOM % 16 )):1}}
	done
	echo $MAC
}

gen_cert()
{
	local req_file=$(mktemp)
	local pem=$(mktemp)
	local type=$1
	local cn=$2
	local key=$3
	local cert=$4
	# generate key and request to sign
	openssl req -config $CONF_FILE -x509 -nodes -newkey rsa:4096 -sha512 -days 365 \
		-extensions $type -subj "/CN=$cn" -out $req_file -keyout $key &> /dev/null
	# sign certificate
	openssl x509 -extfile $CONF_FILE -CA $CA_CERT -CAkey $CA_KEY -CAcreateserial -sha512 -days 365 \
		-in $req_file -out $pem
	if [ $? == "0" ]
	then
		cat $pem $CA_CERT > $cert
	else
		>&2 echo Failed to generate certificate
		rm $key
	fi
	rm $req_file
	rm $pem
}

gen_client_batch()
{
	batch=$(($1 / 100))
	sync_count=$(($1 / 10))
	baseline=$(ps aux | grep openssl | wc -l)
	for (( c=1; c<=$1; c++ ))
	do
		mac=$(rand_mac)
		gen_cert client $mac $CLIENT_DIR/$mac.key $CLIENT_DIR/$mac.crt &
		if [ "$(( $c % $batch ))" -eq "0" ]
		then
			echo $(($c/$batch))%
		fi
		if [ "$(( $c % $sync_count ))" -eq "0" ]
		then
			until [ $(ps aux | grep openssl | wc -l) -eq "$baseline" ]; do sleep 1; done
		fi
	done
}

gen_client()
{
	for x in $(seq $1)
	do
		echo $x
		mac=$(rand_mac)
		gen_cert client $mac $CLIENT_DIR/$mac.key $CLIENT_DIR/$mac.crt
	done
}

while getopts "ac:shm:o" arg; do
case $arg in
a)
	GEN_CA=y
	;;
s)
	GEN_SER=y
	;;
c)
	GEN_CLI=y
	NUM_CERTS=$OPTARG
	if [ $NUM_CERTS -lt 100 ]
	then
		METHOD_FAST=n
	fi
	;;
m)
	TEMPLATE=$OPTARG
	;;
o)
	METHOD_FAST=n
	;;
h)
	usage
	exit 0
	;;
*)
	usage
	exit 1
	;;
esac
done

if [ "$GEN_CA" == "y" ]
then
	echo Generating root CA certificate
	mkdir -p $CA_DIR
	openssl req -config $CONF_FILE -x509 -nodes -newkey rsa:4096 -sha512 -days 365 \
		-extensions ca -subj "/CN=CA" -out $CA_CERT -keyout $CA_KEY &> /dev/null
fi

if [ "$GEN_SER" == "y" ]
then
	mkdir -p $SERVER_DIR
	echo Generating server certificate
	gen_cert server localhost $SERVER_DIR/gw.key $SERVER_DIR/gw.crt
fi

if [ "$GEN_CLI" == "y" ]
then
	echo Generating $NUM_CERTS client certificates
	mkdir -p $CLIENT_DIR
	if [ $METHOD_FAST == "y" ]
	then
		# because of race condition some of the certificates might fail to generate
		# but this is ~15 times faster than generating certificates one by one
		gen_client_batch $NUM_CERTS
	else
		gen_client $NUM_CERTS
	fi
fi

echo done
