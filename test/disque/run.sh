#!/bin/bash
trap 'kill $(jobs -p)' EXIT
disque-server node1.conf &
disque-server node2.conf &
disque-server node3.conf
