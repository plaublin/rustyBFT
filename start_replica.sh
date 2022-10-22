#!/bin/bash

if [ $# -ne 5 ]; then
   echo "Args: <config_file> <f> <id> <replen> <#crypto_threads>"
	exit 0
fi

NODES=$1
F=$2
ID=$3
REPLEN=$4
CRYPTO_THREADS=$5

cargo build --release --bin replica
sudo target/release/replica $NODES $F $ID $REPLEN $CRYPTO_THREADS
