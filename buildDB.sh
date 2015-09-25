#!/bin/bash
ITERATIONS=1
COUNTER=0
FILE_NUMBER=00000
while [  $COUNTER -lt $ITERATIONS ]; do
FILE_NUMBER=$((00000+COUNTER))
APPENDIX=$(printf "%0*d\n" 5 $FILE_NUMBER)
echo $APPENDIX
java -cp \
./bin:\
./releases/orientdb-community-2.1.2/lib/blueprints-core-2.6.0.jar:\
./releases/orientdb-community-2.1.2/lib/orientdb-graphdb-2.1.2.jar:\
./releases/orientdb-community-2.1.2/lib/jna-platform-4.0.0.jar:\
./releases/orientdb-community-2.1.2/lib/jna-4.0.0.jar:\
./releases/orientdb-community-2.1.2/lib/concurrentlinkedhashmap-lru-1.4.jar \
DB_Manager \
data/jazzMusicians.txt \
data/output_1 \
plocal:/home/ubuntu/project/data/graphdb2

let COUNTER=COUNTER+1
done
