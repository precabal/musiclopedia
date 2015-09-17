
package sparkUtils;

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
	
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {

		/* error checking */

    	if (args.length < 1) {
      		System.err.println("Usage: JavaWordCount <file>");
      		System.exit(1);
    	}
		
    	Configuration conf = new Configuration();
    	//TODO: mod this to include following registers
    	conf.set("textinputformat.record.delimiter", "WARC/1.0");

 
		/* setup and read text file */
    	SparkConf sparkConf = new SparkConf().setAppName("JavaSearchForExpression");
		JavaSparkContext context = new JavaSparkContext(sparkConf);		
		JavaPairRDD<LongWritable,Text> lines = context.newAPIHadoopFile(args[0], TextInputFormat.class, LongWritable.class, Text.class, conf);
		
		/* process each line to remove the linebreak except for the headers. */
		/*
		JavaRDD<String> strippedLines = lines.map(new Function<String, String>() {
			@Override
			public String call(String s) {
				
				if ( s.equals("WARC/1.0") )
					s = System.lineSeparator().concat(s);
				
				//see why this in not working
				s.concat("poto");
				return s;
			}
		});
		*/
		
		/* save output */
		/*
		List<String> linesNonRDD = strippedLines.collect();
		
		for(int i = 0; i<linesNonRDD.size(); i++)
		{
			//ADD A SPACE FOR EACH LINE BREAK
			System.out.print(linesNonRDD.get(i));
		}
		*/

		lines.saveAsTextFile("hdfs://ec2-54-210-182-168.compute-1.amazonaws.com:9000/user/outputText");
	
		context.stop();
	}
}
