import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordMapper extends MapReduceBase implements Mapper<Object, Text, StateAndWordWritable, IntWritable>{
    private Text filename = new Text(); 
    private final static IntWritable one = new IntWritable(1);
    
	@Override
	public void map(Object key, Text value, OutputCollector<StateAndWordWritable,IntWritable> output, Reporter r)
			throws IOException {
		
		filename = new Text(((FileSplit) r.getInputSplit()).getPath().getName());
		String content = value.toString();
		content = content.replaceAll( "[^A-Za-z ]", " " ).toLowerCase();
		
		for (String word: content.split(" ")) {
			if (word.length() > 0) {
				
				if(word.equalsIgnoreCase("politics") || word.equalsIgnoreCase("education") || word.equalsIgnoreCase("agriculture")||word.equalsIgnoreCase("sports")) {
					output.collect(new StateAndWordWritable(filename, new Text(word)), one);
				}
			}
		}
	}
}
