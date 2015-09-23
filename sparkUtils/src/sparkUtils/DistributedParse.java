
package sparkUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import scala.Tuple2;

public final class DistributedParse {


	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		final int urlPosition = 47;

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
		
		
		/* #### read & parse web text file #### */
		
		JavaRDD<Text> multiLineRecords = context.newAPIHadoopFile(args[0], TextInputFormat.class, LongWritable.class, Text.class, conf).values();
		
		JavaRDD<String> singleLineRecords = multiLineRecords.map(new Function<Text, String>() {
			@Override
			public String call(Text input) { return input.toString().replaceAll("\\r?\\n", " "); }
		});
		
		/* read artists text file */
		
		ArrayList<String> artistsImported = new ArrayList<String>();
		BufferedReader inputReader = new BufferedReader(new FileReader("./strippedArtists.txt"));
		
		String line = inputReader.readLine();
		while(line!=null){
			artistsImported.add(line);
			line = inputReader.readLine();
		}
		inputReader.close();
		
		final Broadcast<ArrayList<String>> artists = context.broadcast(artistsImported);
		
		//JavaPairRDD<String,String> artistsWebsites = 
		singleLineRecords.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
			
			@Override
			public Iterable<scala.Tuple2<String,String>> call(String record) {
				
				ArrayList<Tuple2<String,String>> url_artistEdge = new ArrayList<Tuple2<String,String>>();
				
				if(record.startsWith(" WARC-Type: conversion WARC-Target-URI", 0)){
					for(String artist: artists.value()){
						if( record.toLowerCase().contains(artist.toLowerCase()) )
							url_artistEdge.add(  new Tuple2<String,String>( record.substring(urlPosition, record.indexOf(" ", urlPosition+1)) , artist )  );
					}
				}
				System.out.print(".");
				return url_artistEdge;
			}
		}).saveAsTextFile("hdfs://ec2-54-210-182-168.compute-1.amazonaws.com:9000/user/outputText");
		
		

		context.stop();
	}
}
