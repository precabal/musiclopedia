
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
		
		final int addressIndex = 47;
		class LookForStrings implements PairFlatMapFunction<String, String, String> {
			
			private ArrayList<String> artists;
			public LookForStrings(ArrayList<String> artistsIn) { this.artists = artistsIn; }
			@Override
			public Iterable<scala.Tuple2<String,String>> call(final String record) {
				
				ArrayList<Tuple2<String,String>> output = new ArrayList<Tuple2<String,String>>();
			
				for(String artist: artists){
					if(record.startsWith(" WARC-Type: conversion WARC-Target-URI", 0)){
						if( record.toLowerCase().contains(artist.toLowerCase()) )
							output.add(  new Tuple2<String,String>( record.substring(addressIndex, record.indexOf(" ", addressIndex+1)) , artist )  );
					}else{

					}
				}

				return output;
			}
		}
		
		class CreateTuple implements PairFunction<String,String,String> {
			private String target;
			public CreateTuple(String target) { this.target = target; }
			@Override
			public scala.Tuple2<String,String> call(String s) {
				return new scala.Tuple2<String,String>(s.substring(addressIndex, s.indexOf(" ", addressIndex+1)), target) ;
			}
				
		}


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
		String artist3 = "Britney Spears";
		
		ArrayList<String> artistsNP = new ArrayList<String>();
		artistsNP.add(artist1);
		artistsNP.add(artist2);
		artistsNP.add(artist3);
		JavaRDD<String> artists = context.parallelize(artistsNP);
		
		final Broadcast<JavaRDD<String>> artistas = context.broadcast(artists);
		
		
		//cartesian(otherDataset)	When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements).
		//JavaPairRDD<String,String> mixedRDD = linesNoBreaks.cartesian(artists);
		
		JavaPairRDD<String,String> artistsWebsites = linesNoBreaks.flatMapToPair(new LookForStrings(artistas.value));
		
		JavaRDD<String> linesWithArtist1 = linesNoBreaks.filter(new LookForString(artist1));
		JavaRDD<String> linesWithArtist2 = linesNoBreaks.filter(new LookForString(artist2));
		JavaRDD<String> linesWithArtist3 = linesNoBreaks.filter(new LookForString(artist3));
		/* output = <websites where Miley Cyrus is mentioned> */
		
		JavaPairRDD<String,String> pairs1 = linesWithArtist1.mapToPair(new CreateTuple(artist1));
		JavaPairRDD<String,String> pairs2 = linesWithArtist2.mapToPair(new CreateTuple(artist2));
		JavaPairRDD<String,String> pairs3 = linesWithArtist3.mapToPair(new CreateTuple(artist3));
		/* output = <Miley Cyrus, website> */
		
		JavaRDD<Tuple2<String,String>> pairs = 	pairs1.join(pairs2).values().union( 
													pairs2.join(pairs3).values().union(
															pairs1.join(pairs3).values()
													)
												);			
		/* output = <Miley Cyrus, Madonna> */
		
		Map<Tuple2<String,String>,Long> output = artistsWebsites.countByValue();
		System.out.println(output.toString());
		
		//Thread.sleep(4l * 60l * 1000l);
		context.stop();
	}
}
