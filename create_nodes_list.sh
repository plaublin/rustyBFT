#!/bin/bash

NODE_FILE=$1
NUM_CLIENTS=$2
HMAC=${3:-0}
SIGN=${4:-0}

NUM_FAULTS=1
NUM_REPLICAS=$((3*$NUM_FAULTS +1))
REPLICAS_IP=(10.10.1.1 10.10.1.2 10.10.1.3 10.10.1.4)
CLIENTS_IP=(10.10.1.5)
REPLICA_PORT=6000
REPLICA_PORT2=7000
CLIENT_PORT=8000
SIGNATURE=42177206f5e9a64b12f44826bf917a65e958aaf2cd97464be33e8f7d86a65d722b794e21f3fd0ac8bdef1172f4f6cb1405043e469d33b812342a8a8f41b882c5

if [ $# -lt 2 ]; then
   echo "Usage: ./$(basename $0) <node_file> <num_clients> <hmac?0:1> <sig?0:1>"
   exit 1
fi



cat << EOF > $1
# IP	PORT_FOR_REPLICAS	PORT_FOR_CLIENTS	HMAC	KEY==concat(pub+priv)
EOF

r=0
while [ $r -lt $NUM_REPLICAS ]; do
	IP=${REPLICAS_IP[$r % ${#REPLICAS_IP[@]}]}
	if [ $HMAC -eq 1 ]; then
		MAC=replica$r
	else
		MAC=NONE
	fi
	echo $IP $(($REPLICA_PORT+$r)) $(($REPLICA_PORT2+r)) $MAC NONE >> $1

	r=$(($r+1))
done

c=0
while [ $c -lt $NUM_CLIENTS ]; do
	IP=${CLIENTS_IP[$r % ${#CLIENTS_IP[@]}]}
	if [ $HMAC -eq 1 ]; then
		MAC=client$c
	else
		MAC=NONE
	fi
	if [ $SIGN -eq 1 ]; then
		SIG=$SIGNATURE
	else
		SIG=NONE
	fi
	echo $IP 0 $(($CLIENT_PORT+$c)) $MAC $SIG >> $1
	c=$(($c+1))
done

