package tableJoin;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class JoinReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, IntWritable, IntWritable> {

	@Override
	public void reduce(IntWritable key, Iterator<Text> value,
			OutputCollector<IntWritable, IntWritable> outputCollector,
			Reporter reporter) throws IOException {

		ArrayList<String> pageViewTableFields = new ArrayList<String>();
		ArrayList<String> usersTableFields = new ArrayList<String>();
		while (value.hasNext()) {
			String splitField = value.next().toString();
			if (splitField.startsWith("F1")) {
				pageViewTableFields.add(splitField);
			} else if (splitField.startsWith("F2")) {
				usersTableFields.add(splitField);
			}
		}
		
		for (String pageSplit : pageViewTableFields) {
			for (String userSplit : usersTableFields) {
				outputCollector.collect(
						new IntWritable(
								Integer.parseInt(pageSplit.split("\t")[1])),
						new IntWritable(
								Integer.parseInt(userSplit.split("\t")[2])));
			}
		}

	}

}
