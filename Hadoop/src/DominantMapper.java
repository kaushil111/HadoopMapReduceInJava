import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DominantMapper extends MapReduceBase implements Mapper<Object, Object, Text, WordAndCountWritable>{
    
	@Override
	public void map(Object key, Object value, OutputCollector<Text ,WordAndCountWritable> output, Reporter r)
			throws IOException {
		
		String content = value.toString();
		String[] words = content.split("\t");
		if(words.length == 2) {
			int count = Integer.parseInt(words[1]);
			output.collect(new Text(key.toString()), new WordAndCountWritable(new IntWritable(count), new Text(words[0])));
		}
		
		
	}
}