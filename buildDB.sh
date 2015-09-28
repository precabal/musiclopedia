#!/bin/bash
HOST_DNS='hdfs://ec2-54-210-182-168.compute-1.amazonaws.com:9000'
HDFS_BASE_PATH='/user/data/'

java -cp \
./bin:\
./releases/orientdb-community-2.1.2/lib/blueprints-core-2.6.0.jar:\
./releases/orientdb-community-2.1.2/lib/orientdb-graphdb-2.1.2.jar:\
./releases/orientdb-community-2.1.2/lib/jna-platform-4.0.0.jar:\
./releases/orientdb-community-2.1.2/lib/jna-4.0.0.jar:\
./releases/orientdb-community-2.1.2/lib/concurrentlinkedhashmap-lru-1.4.jar \
DB_Manager \
data/jazzMusicians.txt \
$HOST_DNS$HDFSBASE_PATH \
plocal:/home/ubuntu/project/data/graphdb2

