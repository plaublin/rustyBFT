#!/bin/bash

pkill replica
pkill client
sudo iptables -F
