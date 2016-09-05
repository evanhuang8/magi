#!/bin/bash
trap 'kill $(jobs -p)' EXIT
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $BASEDIR
disque-server $BASEDIR/node1.conf &
disque-server $BASEDIR/node2.conf &
disque-server $BASEDIR/node3.conf
