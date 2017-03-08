package tableJoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class JoinMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> outputCollector,
			Reporter reporter) throws IOException {

		FileSplit splitFile = (FileSplit) reporter.getInputSplit();
		String fileName = splitFile.getPath().getName();
		String splitField = value.toString();
		if (!isHeader(splitField)) {
			if (fileName.equals("page_view.txt")) {
				splitField = "F1\t" + splitField;
				
				outputCollector
						.collect(
								new IntWritable(Integer.parseInt(splitField
										.split("\t")[2])), new Text(splitField));
			} else if (fileName.equals("user.txt")) {
				splitField = "F2\t" + splitField;
				outputCollector
						.collect(
								new IntWritable(Integer.parseInt(splitField
										.split("\t")[1])), new Text(splitField));
			}
		} }

	private boolean isHeader(String splitField) {
		if (splitField.split("\t")[0].equals("page_id")
				|| splitField.split("\t")[0].equals("user_id")) {
			return true;
		}
		return false;
	}

}
