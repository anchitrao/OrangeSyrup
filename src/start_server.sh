#!/bin/bash
make

# Lookup IP address of ds_node_1
master_ip=$(host $NODE_NAME_PREFIX_node_1 | awk '/has.*address/{print $NF; exit}')

# Get our IP
my_ip=$(hostname -I)

if [ $my_ip == $master_ip ]; then
    # We are the master
    ./mp3
else
    # Not the master, sleep and then try to connect
    #sleep 3
    ./mp3 -i $NODE_NAME_PREFIX_node_1
fi

cat
