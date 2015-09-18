
package com.precabal.app;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import scala.Tuple2;

import java.util.regex.Pattern;

public final class DistributedParse {
	
	private static final Pattern LINE_BREAK = Pattern.compile(System.lineSeparator());

	public static void main(String[] args) throws Exception {

		class LookForString implements Function<String, Boolean> {
			
			private String target;
			public LookForString(String target) { this.target = target; }
			@Override
			public Boolean call(String s) {
				if(s.startsWith(" WARC-Type: conversion WARC-Target-URI", 0)){
					if( s.toLowerCase().contains(target.toLowerCase()) )
						return true;
				}

				return false;
			}
		}
		
		class CreateTuple implements PairFunction<String,String,String> {
			private String target;
			public CreateTuple(String target) { this.target = target; }
			@Override
			public scala.Tuple2<String,String> call(String s) {
				return new scala.Tuple2<String,String>(s.substring(47, s.indexOf(" ", 48)),target) ;
			}
				
		}



		/* error checking */
    	if (args.length < 1) {
      		System.err.println("Usage: JavaWordCount <file>");
      		System.exit(1);
    	}
		
		/* setup TODO: mod this to include subsequent registers */    	
    	Configuration conf = new Configuration();
    	conf.set("textinputformat.record.delimiter", "WARC/1.0");

    	SparkConf sparkConf = new SparkConf().setAppName("JavaSearchForExpression");
		JavaSparkContext context = new JavaSparkContext(sparkConf);		
		
		
		/* read text file */
		
		JavaRDD<Text> records = context.newAPIHadoopFile(args[0], TextInputFormat.class, LongWritable.class, Text.class, conf).values();
		
		
		/* process each line to remove the linebreak */
		
		JavaRDD<String> linesNoBreaks = records.map(new Function<Text, String>() {
		
			@Override
			public String call(Text input) {
				return input.toString().replaceAll("\\r?\\n", " ");
			}
		});
		
		String artist1 = "Madonna";
		String artist2 = "Miley Cyrus";
		String artist3 = "britney spears";
		
		//linesNoBreaks.persist(StorageLevel.MEMORY_ONLY());
		JavaRDD<String> linesWithArtist1 = linesNoBreaks.filter(new LookForString(artist1));
		JavaRDD<String> linesWithArtist2 = linesNoBreaks.filter(new LookForString(artist2));
		JavaRDD<String> linesWithArtist3 = linesNoBreaks.filter(new LookForString(artist3));
		
		JavaPairRDD<String,String> pairs1 = linesWithArtist1.mapToPair(new CreateTuple(artist1));
		JavaPairRDD<String,String> pairs2 = linesWithArtist2.mapToPair(new CreateTuple(artist2));
		JavaPairRDD<String,String> pairs3 = linesWithArtist3.mapToPair(new CreateTuple(artist3));
		
		/* save output */		
		
		JavaRDD<Tuple2<String,String>> pairs = 	pairs1.join(pairs2).values().union( 
													pairs2.join(pairs3).values().union(
															pairs1.join(pairs3).values()
													)
												);			
		
		pairs.mapToPair(new PairFunction<Tuple2<String, String>,Tuple2<String, String>,Integer>(){
			@Override
			public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, String> input) {
				return new Tuple2<Tuple2<String, String>, Integer>(input, 1);
			}
		});

		pairs.saveAsTextFile("hdfs://ec2-54-210-182-168.compute-1.amazonaws.com:9000/user/outputText");

		context.stop();
	}
}
