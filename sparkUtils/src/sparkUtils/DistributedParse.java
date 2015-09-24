
package sparkUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
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
    	conf.set("mapreduce.input.fileinputformat.split.maxsize", "1000000");

    	
    	SparkConf sparkConf = new SparkConf().setAppName("JavaSearchForExpression");
		JavaSparkContext context = new JavaSparkContext(sparkConf);		
		
		/* read artists text file */
		
		ArrayList<String> artistsImported = new ArrayList<String>();
		
		BufferedReader inputReader = new BufferedReader(new FileReader(args[1]));
		
		String line = inputReader.readLine();
		while(line!=null){
			artistsImported.add(line);
			line = inputReader.readLine();
		}
		inputReader.close();
		
		final Broadcast<ArrayList<String>> artists = context.broadcast(artistsImported);
		
		
		/* #### read & parse web text file #### */
		
		JavaRDD<Text> multiLineRecords = context.newAPIHadoopFile(args[0], TextInputFormat.class, LongWritable.class, Text.class, conf).values();
		
		JavaRDD<String> singleLineRecords = multiLineRecords.map(new Function<Text, String>() {
			@Override
			public String call(Text input) { return input.toString().replaceAll("\\r?\\n", " "); }
		}).filter(new Function<String,Boolean> (){
			@Override
			public Boolean call(String record) { return record.startsWith(" WARC-Type: conversion WARC-Target-URI", 0); }
		});
		
		
		
		JavaPairRDD<String,String> url_sublines = singleLineRecords.flatMapToPair(new PairFlatMapFunction<String,String,String>(){

			@Override
			public Iterable<Tuple2<String, String>> call(String record) {	
				int urlEndPosition = record.indexOf(" ", urlPosition+1);
				String url = record.substring(urlPosition, urlEndPosition);
				
				StringTokenizer tokenizer = new StringTokenizer( record.substring(urlEndPosition), ".,#()[]$%*+,-.:;<=>?@[]^_`{|}~" ) ;
				
				ArrayList<Tuple2<String,String>> url_sentences = new ArrayList<Tuple2<String,String>>();
			
				while(tokenizer.hasMoreTokens()){
					url_sentences.add(new Tuple2<String,String>(url,tokenizer.nextToken()) );
				}

				return url_sentences;
			}
		
		});
		
		url_sublines.flatMapValues(new Function<String,Iterable<String>> (){

			@Override
			public Iterable<String> call(String arg0) {
				ArrayList<String> artistsFound = new ArrayList<String>();
				
				for(String artist: artists.value()){
					if( arg0.contains(artist) )
						artistsFound.add(artist);
				}
				return artistsFound;
			}	
		}).distinct().saveAsTextFile("hdfs://ec2-54-210-182-168.compute-1.amazonaws.com:9000/user/outputText");
		

		context.stop();
	}
}
