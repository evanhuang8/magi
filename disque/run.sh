#!/bin/sh

if [ -z $1 ] ; then
  echo "Usage: ./run.sh [1|2|3]"
  exit 1
fi

disque-server node$1.conf
