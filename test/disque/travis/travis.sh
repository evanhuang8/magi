#!/bin/bash

set -e
DQPATH=${TRAVIS_BUILD_DIR}/tests/disque/travis

# Install disque

mkdir -p ${DQPATH}/src
/usr/bin/env git clone https://github.com/antirez/disque.git ${DQPATH}/src/disque
cd ${DQPATH}/src/disque/src
make

# Remove existing files

rm ${DQPATH}/nodes-*.conf

# Run 3 instances

${DQPATH}/src/disque/src/disque-server ${DQPATH}/node1.conf
${DQPATH}/src/disque/src/disque-server ${DQPATH}/node2.conf
${DQPATH}/src/disque/src/disque-server ${DQPATH}/node3.conf

# Wait & join

sleep 5
${DQPATH}/src/disque/src/disque -p 7711 cluster meet 127.0.0.1 7712
${DQPATH}/src/disque/src/disque -p 7711 cluster meet 127.0.0.1 7713