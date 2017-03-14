import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SameRankingMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text>{
	@Override
	public void map(Object key, Text value, OutputCollector<Text ,Text> output, Reporter r)
			throws IOException {
		output.collect(new Text(key.toString()), value);
	}
}