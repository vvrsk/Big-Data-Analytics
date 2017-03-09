package tableJoinHeader;
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
		Reducer<IntWritable, Text, IntWritable, String> {

	@Override
	public void reduce(IntWritable key, Iterator<Text> value,
			OutputCollector<IntWritable, String> outputCollector,
			Reporter reporter) throws IOException {

		ArrayList<String> pageViewTableFields = new ArrayList<String>();
		ArrayList<String> usersageFields = new ArrayList<String>();
		ArrayList<String> usersgenderFields = new ArrayList<String>();
		while (value.hasNext()) {
			String splitField = value.next().toString();
			if (splitField.startsWith("F1")) {
				pageViewTableFields.add(splitField);
			} else if (splitField.startsWith("F2")) {
				usersageFields.add(splitField);
			} else if (splitField.startsWith("F3")) {
				usersgenderFields.add(splitField);
			}
		}

		for (String pageSplit : pageViewTableFields) {
			for (String userageSplit : usersageFields) {
				for (String usergenderSplit : usersgenderFields) {
				
					String combinedTemp = userageSplit.split("\t")[2]+"\t"+pageSplit.split("\t")[3]+"\t"+usergenderSplit.split("\t")[2];
				
				outputCollector.collect(
						new IntWritable(
								Integer.parseInt(pageSplit.split("\t")[1])),
																(combinedTemp));
				}
			}
		}
	}

}
