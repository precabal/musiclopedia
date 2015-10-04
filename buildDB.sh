#!/bin/bash
HOST_DNS='hdfs://ec2-52-1-220-20.compute-1.amazonaws.com:9000'
HDFS_BASE_PATH='/user/data/'
HADOOP_CLASSPATH=$(hadoop classpath)
java -cp \
./bin:\
./releases/orientdb-community-2.1.2/lib/blueprints-core-2.6.0.jar:\
./releases/orientdb-community-2.1.2/lib/orientdb-graphdb-2.1.2.jar:\
./releases/orientdb-community-2.1.2/lib/jna-platform-4.0.0.jar:\
./releases/orientdb-community-2.1.2/lib/jna-4.0.0.jar:\
./releases/orientdb-community-2.1.2/lib/concurrentlinkedhashmap-lru-1.4.jar:\
/usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.1.jar:\
/usr/local/hadoop/share/hadoop/common/lib/commons-lang-2.6.jar:\
/usr/local/hadoop/share/hadoop/common/lib/guava-11.0.2.jar:\
/usr/local/hadoop/share/hadoop/hdfs/lib/commons-logging-1.1.3.jar:\
/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-2.7.1.jar:\
/usr/local/hadoop/share/hadoop/common/lib/commons-collections-3.2.1.jar:\
$HADOOP_CLASSPATH \
-Xmx4G \
DB_Manager \
data/jazzMusicians.txt \
$HOST_DNS$HDFS_BASE_PATH \
plocal:/home/ubuntu/project/data/graphdb2

