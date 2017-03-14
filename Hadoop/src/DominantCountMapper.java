import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DominantCountMapper extends MapReduceBase implements Mapper<Object, Object, Text, IntWritable>{
	@Override
	public void map(Object key, Object value, OutputCollector<Text ,IntWritable> output, Reporter r)
			throws IOException {
		output.collect(new Text(key.toString()), new IntWritable(1));
	}
}