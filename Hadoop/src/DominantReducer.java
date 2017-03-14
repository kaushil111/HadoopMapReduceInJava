import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DominantReducer extends MapReduceBase implements Reducer<Text, WordAndCountWritable,Text,Text>{

	@Override
	public void reduce(Text key, Iterator<WordAndCountWritable> value, OutputCollector<Text, Text> output, Reporter r)
			throws IOException {
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		while(value.hasNext()) {
			WordAndCountWritable wc = value.next();
			String word = wc.word.toString();
			if(word != null) {
					map.put(word, wc.count.get());
			}
		}
		
		int max = 0;
		String maxWord = "";
		for (Iterator<String> iterator = map.keySet().iterator(); iterator.hasNext();) {
			String keyIterator = (String) iterator.next();
			if(map.get(keyIterator) > max) {
				max = map.get(keyIterator);
				maxWord = keyIterator;
			}
		}
		output.collect(new Text(maxWord),key);
	}

}