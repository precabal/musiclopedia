mvn clean
rm -rf outputText
git pull
mvn package

spark-submit \
--class com.precabal.app.DistributedParse \
--master spark://ip-172-31-27-55:7077 \
target/sparkUtils-1.0-SNAPSHOT.jar \
hdfs://ec2-54-210-182-168.compute-1.amazonaws.com:9000/user/test2.txt

