
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

import java.util.regex.Pattern;

public final class DistributedParse {
	
	private static final Pattern LINE_BREAK = Pattern.compile(System.lineSeparator());

	public static void main(String[] args) throws Exception {

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
		
		final String test = "Madonna";
		
		JavaRDD<String> filteredLines = linesNoBreaks.filter(new Function<String, Boolean>() {
			
			@Override
			public Boolean call(String s) {
				if(s.startsWith(" WARC-Type: conversion WARC-Target-URI", 0)){
					if( s.toLowerCase().contains(test.toLowerCase()) )
						return true;
				}

				return false;
			}
				
		});
		
		JavaPairRDD<String,String> pairs = filteredLines.mapToPair(new PairFunction<String,String,String>() {
			
			@Override
			public scala.Tuple2<String,String> call(String s) {
				return new scala.Tuple2<String,String>(test,s.substring(47, s.indexOf(" ", 48))) ;
			}
				
		});
		

		
		/* save output */		
		
		pairs.saveAsTextFile("hdfs://ec2-54-210-182-168.compute-1.amazonaws.com:9000/user/outputText");
	
		context.stop();
	}
}
