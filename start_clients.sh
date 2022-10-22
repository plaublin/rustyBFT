#!/bin/bash

if [ $# -ne 8 ]; then
   echo "Args: <config_file> <f> <id> <#clients> <duration_sec> <reqlen> <malicious_ratio> <save_latencies>"
	exit 0
fi

NODES=$1
F=$2
ID=$3
CLIENTS=$4
DURATION=$5
REQLEN=$6
MALICIOUS=$7
SAVE_LATENCIES=$8

cargo run --release --bin client $NODES $F $ID $CLIENTS $DURATION $REQLEN $MALICIOUS $SAVE_LATENCIES
