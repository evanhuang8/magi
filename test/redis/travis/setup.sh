#!/bin/sh

set -e

echo "****************************"
echo "Creating Redis Installations"
echo "****************************"
echo ""
echo ""

echo "Copying Configuration..."

RPATH=${TRAVIS_BUILD_DIR}/test/redis

sudo cp ${RPATH}/travis/redis /etc/init.d/redis1
sudo cp ${RPATH}/travis/redis /etc/init.d/redis2
sudo cp ${RPATH}/travis/redis /etc/init.d/redis3

sudo cp ${RPATH}/travis/redis1.conf /etc/redis/redis1.conf
sudo cp ${RPATH}/travis/redis2.conf /etc/redis/redis2.conf
sudo cp ${RPATH}/travis/redis3.conf /etc/redis/redis3.conf

echo "Creating Data Directory..."

sudo mkdir /var/lib/redis1
sudo mkdir /var/lib/redis2
sudo mkdir /var/lib/redis3

sudo chown redis:redis /var/lib/redis1
sudo chown redis:redis /var/lib/redis2
sudo chown redis:redis /var/lib/redis3

echo "Starting Services..."

sudo service redis1 start
sudo service redis2 start
sudo service redis3 start

sleep 3

echo "Finished."