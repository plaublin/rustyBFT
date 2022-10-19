#!/bin/bash

if [ $# -ne 6 ]; then
   echo "Args: <config_file> <f> <#clients> <reqlen> <crypto_threads> <malicious_ratio>"
	exit 0
fi

EXPDIR=$(pwd)
NODES=$1
F=$2
CLIENTS=$3
REQLEN=$4
CRYPTO_THREADS=$5
MALICIOUS=$6
DURATION=30
REPLEN=0
REPLICAS=(node0 node1 node2 node3 node4 node5 node6)
FIRST_CLIENT="${#REPLICAS[@]}"

function stop_all {
	echo "Stopping all nodes"
	for n in ${REPLICAS[@]}; do
		ssh $n $EXPDIR/stop_node.sh
	done
	./stop_node.sh
}

RESDIR="exp_${F}_${CLIENTS}_${REQLEN}_${REPLEN}_${CRYPTO_THREADS}_${MALICIOUS}"
if [ -e $RESDIR ]; then
	echo Skipping existing experiment $RESDIR
	exit 0
fi

# stop all
stop_all

# start replicas
echo "Starting all replicas"
N=$((3*$F + 1))
r=0
while [ $r -lt $N ]; do
	echo start replica $r
	ssh -n node$r ". $HOME/.cargo/env; cd $EXPDIR; nohup ./start_replica.sh $NODES $F $r $REPLEN $CRYPTO_THREADS &> replica${r}.log &"
	r=$(($r+1))
done

# start clients
echo "Starting clients"
./start_clients.sh $NODES $F $FIRST_CLIENT $CLIENTS $DURATION $REQLEN $MALICIOUS | tee clients.log &
sleep $(($DURATION + 5))

# stop all
stop_all

# collect results (primary log + client log)
echo "Collect results"
mkdir $RESDIR
scp node0:$EXPDIR/replica0.log $RESDIR/
mv clients.log $RESDIR/
