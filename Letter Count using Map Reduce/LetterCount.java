import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LetterCount {

  public static class MapperTokenizer
       extends Mapper<LongWritable, Text, Text, IntWritable>{

    // For adding 1 to the each value of the charecter
	private final static IntWritable one = new IntWritable(1);
    String word;
    @Override
	public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase());
      while (itr.hasMoreTokens()) {
    	  word = itr.nextToken();
    	char tempChar = word.charAt(0);
   	  	// To check if the the values between 'a' and 'z'
    	if ((tempChar>='a' && tempChar<='z') || tempChar == ' ' ) {   		 
   		context.write(new Text(String.valueOf(tempChar)), one );
   	  	}	
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
	public void reduce(Text key, Iterable<IntWritable> values,Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
       	  
      for ( IntWritable val : values) {
    	sum += val.get(); 
    	 }
      result.set(sum);
      context.write(key, result);
    }
    
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "LetterCount");
    job.setJarByClass(LetterCount.class);
    job.setMapperClass(MapperTokenizer.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}