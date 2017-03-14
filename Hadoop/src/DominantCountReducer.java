import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DominantCountReducer extends MapReduceBase implements Reducer<Text, IntWritable,Text,IntWritable>{

	@Override
	public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, IntWritable> output, Reporter r)
			throws IOException {
		
		int total = 0;
		while(value.hasNext()) {
			total++;
			value.next();
		}
		output.collect(key, new IntWritable(total));
	}

}
