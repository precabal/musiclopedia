
package com.precabal.app;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


import java.util.List;
import java.util.regex.Pattern;

public final class DistributedParse {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {

		/* error checking */

    	if (args.length < 1) {
      		System.err.println("Usage: JavaWordCount <file>");
      		System.exit(1);
    	}
		
		
		/* setup and read text file */
    	
		SparkConf sparkConf = new SparkConf().setAppName("sparkUtils-1.0-SNAPSHOT.jar");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = context.textFile(args[0], 1);


		/* process each line to remove the linebreak except for the headers. */
		
		JavaRDD<String> strippedLines = lines.map(new Function<String, String>() {
			@Override
			public String call(String s) {
				/*
				if ( s.equals("WARC/1.0") )
					s = System.lineSeparator().concat(s);
				
				s.substring(0, s.length()-2);
				*/
				s.concat("poto");
				return s;
			}
		});
		
		List<String> linesNonRDD = lines.collect();
		/* save output */
		for(int i = 0; i<linesNonRDD.size(); i++)
		{
			System.out.print(linesNonRDD.get(i));
		}

		lines.saveAsTextFile("outputText");
	
		context.stop();
	}
}
