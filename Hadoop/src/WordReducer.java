import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WordReducer extends MapReduceBase implements Reducer<StateAndWordWritable, IntWritable,StateAndWordWritable,IntWritable>{

	@Override
	public void reduce(StateAndWordWritable key, Iterator<IntWritable> value, OutputCollector<StateAndWordWritable, IntWritable> output, Reporter r)
			throws IOException {
		int total = 0;
		while(value.hasNext()) {
			total += value.next().get();
		}
		output.collect(key, new IntWritable(total));
	}

}
