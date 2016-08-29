#!/bin/bash
trap 'kill $(jobs -p)' EXIT
redis-server --port 7777 --loglevel verbose &
redis-server --port 7778 --loglevel verbose &
redis-server --port 7779 --loglevel verbose
