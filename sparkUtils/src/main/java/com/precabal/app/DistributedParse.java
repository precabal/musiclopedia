
package com.precabal.app;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

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
    	String newLine = System.lineSeparator();
    	Configuration conf = new Configuration();
    	conf.set("textinputformat.record.delimiter", "WARC/1.0"+newLine+"WARC-Type: conversion");

    	SparkConf sparkConf = new SparkConf().setAppName("JavaSearchForExpression");
		JavaSparkContext context = new JavaSparkContext(sparkConf);		
		
		
		/* read text file */
		
		JavaRDD<Text> lines = context.newAPIHadoopFile(args[0], TextInputFormat.class, LongWritable.class, Text.class, conf).values();
		
		
		/* process each line to remove the linebreak */
		
		
		JavaRDD<String> linesNoBreaks = lines.map(new Function<Text, String>() {
		
			@Override
			public String call(Text input) {
				String output = input.toString().replace(System.lineSeparator(), " ");
				return output;
			}
		});
		
		
		//System.out.print(linesNoBreaks.first());
		
		
		/* save output */		
		
		linesNoBreaks.saveAsTextFile("hdfs://ec2-54-210-182-168.compute-1.amazonaws.com:9000/user/outputText");
	
		context.stop();
	}
}
