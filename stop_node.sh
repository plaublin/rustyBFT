#!/bin/bash

sudo pkill replica
pkill client
sudo iptables -F
