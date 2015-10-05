#!/bin/bash
COUNTER=1784
ITERATIONS=$(($1+$COUNTER))

#set up paths
HOST_DNS='hdfs://ec2-52-1-220-20.compute-1.amazonaws.com:9000/'
DATA_FLDR='data'
FILE_PATHS=$DATA_FLDR'/wet.paths'
PREFIX='https://aws-publicdatasets.s3.amazonaws.com/'
OUTPUT_FOLDER_PREFIX=$HOST_DNS'user/data/output_'
HDFS_FOLDER='/user/'$DATA_FLDR'/inputText/'


#hdfs dfs -rm -r /user/data/output_*
while [  $COUNTER -lt $ITERATIONS ]; do
     
     let COUNTER=COUNTER+1 

     #read n-th line of wet files paths
     CURRENT_FILE=$(sed ''$COUNTER'q;d' $FILE_PATHS | cut --characters=70-)
     CURRENT_FILE_PATH=$(sed ''$COUNTER'q;d' $FILE_PATHS)
     echo '##### current file  = '$PREFIX$CURRENT_FILE
     echo '##### output folder = '$OUTPUT_FOLDER_PREFIX$COUNTER

	 #downloads the current file to a temp directory
	 wget $PREFIX$CURRENT_FILE_PATH -O $DATA_FLDR'/tmp/'$CURRENT_FILE
	 gunzip $DATA_FLDR'/tmp/'$CURRENT_FILE 
	 CURRENT_FILE_UNZIPPED=${CURRENT_FILE::-3}

	 #moves to HDFS
	 hdfs dfs -moveFromLocal $DATA_FLDR'/tmp/'$CURRENT_FILE_UNZIPPED $HDFS_FOLDER

	 #calls the spark-submit with the recently downloaded file and the output folder name
	 time spark-submit \
	 --class sparkUtils.DistributedParse \
	 --master spark://ip-172-31-27-55:7077 \
	 --executor-memory 4G \
	 bin/DistributedParse.jar \
	 $HOST_DNS$HDFS_FOLDER$CURRENT_FILE_UNZIPPED \
	 data/jazzMusicians.txt \
	 $OUTPUT_FOLDER_PREFIX$COUNTER
	 
	 #3. delete the downloaded file form local and remote
	 rm -rf $DATA_FLDR'/tmp/'$CURRENT_FILE_UNZIPPED
	 hdfs dfs -rm /user/data/inputText/$CURRENT_FILE_UNZIPPED

done
