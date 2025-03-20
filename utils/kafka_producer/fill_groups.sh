#!/bin/bash
################################################################################

kafka_host="NULL"
starting_mac_offset="NULL"
mac_max_num=0
total_macs_generated=0
num_of_groups_filled=0
out_fname="/tmp/gid_gen.sh"
gid_offset=0
shard_id=-1

################################################################################

function incr_mac() {
	in_mac=$1
	in_incr_num=$2
	in_incr_num=$(($in_incr_num + 1))
	mac=$(echo $in_mac | tr -d ':')
	macadd=$(( 0x$mac + $in_incr_num ))
	macnew=$(printf "%012X" $macadd | sed 's/../&:/g;s/:$//')
	echo $macnew
}

function usage() {
	usage="$(basename "$0") [-o <starting_mac_offset>] [-s ip:port] [-c n] [-g n] -- randomly generate and fill KAFKA BUS with groups and infras list create / add messages

	where:
	-o  Starting mac offset (e.g. 00:00:00:00:00:01)
	-s  KAFKA server (in a IP:PORT fashion)
	-c  infras (mac addresses) number to create
	-g  group ID offset to start from
	-i  shard id to assign newly created groups to"
	echo "$usage"
}

function check_args() {
	if [ $starting_mac_offset = "NULL" ] ; then
		echo "ERR: Starting mac offset ('-o' option) is NULL (unset)"
		echo
		usage
		exit 4;
	fi

	if [ $mac_max_num -eq 0 ] ; then
		echo "ERR: Num of macs to create has to be greater than zero ('-c' option is NULL - unset)"
		echo
		usage
		exit 5;
	fi

	if [ $kafka_host = "NULL" ] ; then
		echo "ERR: Kafka host has to be set ('-s' option is NULL - unset)"
		echo
		usage
		exit 6;
	fi

	if ! [ -e ./main.py ] ; then
		echo "Failed to find main.py file of kafka_producer!"
		exit 2;
	fi

	if ! [ `realpath main.py | grep "kafka_producer"` ] ; then
		echo -e \
			"Found main.py but it seems invalid:\n"\
			"expected it to be inside 'kafka_producer' dir.\n" \
			"$0 should be executed from that folder as well.";
					exit 2
	fi
}

################################################################################

while getopts ':o:hs:c:g:i:' option; do
	case "$option" in
		h) usage
			exit
			;;
		o) starting_mac_offset=$OPTARG
			echo -e "OPT_IN: starting mac offset will be used: '$starting_mac_offset'"
			;;
		s) kafka_host=$OPTARG
			echo -e "OPT_IN: Kafka host:port will be used: '$kafka_host'"
			;;
		c) mac_max_num=$OPTARG
			echo -e "OPT_IN: Will create the following random macs num: '$mac_max_num'"
			;;
		g) gid_offset=$OPTARG
			echo -e "OPT_IN: Will start creating groups starting from the following GID offset: '$gid_offset'"
			;;
		i) shard_id=$OPTARG
			echo -e "OPT_IN: Will assign groups to the specified shard: '$shard_id'"
			;;
		:) printf "missing argument for -%s\n" "$OPTARG" >&2
			echo "$usage" >&2
			exit 1
			;;
		\?) printf "illegal option: -%s\n" "$OPTARG" >&2
			echo "$usage" >&2
			exit 1
			;;
	esac
done
check_args
################################################################################

echo "#!/bin/bash" > $out_fname
echo >> $out_fname

mac_offset=$starting_mac_offset
num_of_groups_filled=$gid_offset
while true ; do
	echo "Processing mac offset $mac_offset..."
	random_mac_num=`seq 40 200 | sort -R | head -1` && echo "Generated mac num for gid $num_of_groups_filled - $random_mac_num"
	if [ $shard_id -eq -1 ]; then
		echo "python3 ./main.py -s $kafka_host -c 1 --new-group $num_of_groups_filled" >> $out_fname
	else
		echo "python3 ./main.py -s $kafka_host -c 1 --new-group-to-shard $num_of_groups_filled $shard_id" >> $out_fname
	fi
	echo "python3 ./main.py -s $kafka_host -d $num_of_groups_filled '$mac_offset^$random_mac_num'" >> $out_fname
	total_macs_generated=$((total_macs_generated + random_mac_num + 1))
	num_of_groups_filled=$((num_of_groups_filled + 1))
	if [ $total_macs_generated -ge $mac_max_num ]; then
		break;
	fi
	mac_offset=`incr_mac $mac_offset $random_mac_num`
done
echo "Created '$total_macs_generated' infra entries dispersed among '$((num_of_groups_filled - gid_offset))' number of groups"

chmod +x $out_fname

echo
echo "Output file generated, and can be launched: $out_fname"
