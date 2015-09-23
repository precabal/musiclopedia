
package sparkUtils;

import java.util.ArrayList;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import scala.Tuple2;

public final class DistributedParse {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
		final int addressIndex = 47;
		
		String artist1 = "Madonna";
		String artist2 = "Miley Cyrus";
		String artist3 = "Britney Spears";
		
		ArrayList<String> artistsNP = new ArrayList<String>();
		artistsNP.add(artist1);
		artistsNP.add(artist2);
		artistsNP.add(artist3);
		
		


		/* error checking */
		
    	if (args.length < 1) {
      		System.err.println("Usage: JavaWordCount <file>");
      		System.exit(1);
    	}
		
		/* setup */    	
    	
    	Configuration conf = new Configuration();
    	conf.set("textinputformat.record.delimiter", "WARC/1.0");

    	SparkConf sparkConf = new SparkConf().setAppName("JavaSearchForExpression");
		JavaSparkContext context = new JavaSparkContext(sparkConf);		
		
		
		
		/* read text file */
		
		JavaRDD<Text> multiLineRecords = context.newAPIHadoopFile(args[0], TextInputFormat.class, LongWritable.class, Text.class, conf).values();
		JavaRDD<String> artists = context.parallelize(artistsNP);
		
		/* process each line to remove the line breaks */
		
		JavaRDD<String> oneLineRecords = multiLineRecords.map(new Function<Text, String>() {
		
			@Override
			public String call(Text input) {
				return input.toString().replaceAll("\\r?\\n", " ");
			}
		});
	
		
		/* returns a dataset of all pairs */
		
		JavaPairRDD<String,String> recordsCrossArtists = oneLineRecords.cartesian(artists);
		
		JavaPairRDD<String,String> websiteArtistEdges = recordsCrossArtists.filter(new Function<Tuple2<String,String>,Boolean> (){
			
			@Override
			public Boolean call(Tuple2<String,String> input){
				
				return (input._1.startsWith(" WARC-Type: conversion WARC-Target-URI", 0)) && (input._1.toLowerCase().contains(input._2.toLowerCase()));
			}
			
		}).mapToPair(new PairFunction<Tuple2<String,String>,String,String> (){
			
			@Override
			public Tuple2<String,String> call(Tuple2<String,String> input){
				
				return new scala.Tuple2<String,String>(input._1.substring(addressIndex, input._1.indexOf(" ", addressIndex+1)), input._2) ;
				
			}
			
		});
		
		/*
		JavaPairRDD<String,String> aristsPairsCount = toGraph.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String,ArrayList<String>>,String,String>(){

			@Override
			public Iterable<Tuple2<String, String>> call(Tuple2<String, ArrayList<String>> inputList) {
				
				ArrayList<Tuple2<String,String>> output = new ArrayList<Tuple2<String,String>>();
				
				for(int i = 1; i<inputList._2.)
				
				
				return output;
			}
			
		});
		*/
		
		websiteArtistEdges.saveAsTextFile("hdfs://ec2-54-210-182-168.compute-1.amazonaws.com:9000/user/outputText");
		context.stop();
	}
}
