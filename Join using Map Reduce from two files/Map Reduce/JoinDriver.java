package tableJoin;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 3)
			return -1;
		JobConf config = new JobConf(JoinDriver.class);
		FileInputFormat.setInputPaths(config, new Path(args[0]), new Path(args[1]));
		FileOutputFormat.setOutputPath(config, new Path(args[2]));
		config.setMapperClass(JoinMapper.class);
		//conf.setCombinerClass(JoinReducer.class);
		config.setReducerClass(JoinReducer.class);
		config.setMapOutputKeyClass(IntWritable.class);
		config.setMapOutputValueClass(Text.class);
		config.setOutputKeyClass(IntWritable.class);
		config.setOutputValueClass(IntWritable.class);
		JobClient.runJob(config);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JoinDriver(),args);
		if (exitCode == 0) {
		System.out.println("SUCCESS");
		}else if (exitCode !=0) {
			System.out.println("FAILED");
		}
			
	}

}
