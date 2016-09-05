#!/bin/bash
trap 'kill $(jobs -p)' EXIT
redis-server --port 7777 &
redis-server --port 7778 &
redis-server --port 7779
