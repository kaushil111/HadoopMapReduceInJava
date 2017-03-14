import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Comparator;
import java.util.Map;
	
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RankingReducer extends MapReduceBase implements Reducer<Text, WordAndCountWritable,Text,Text>{

	@Override
	public void reduce(Text key, Iterator<WordAndCountWritable> value, OutputCollector<Text, Text> output, Reporter r)
			throws IOException {
		
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		while(value.hasNext()) {
			WordAndCountWritable wc = value.next();
			String word = wc.word.toString();
			if(word != null) {
					map.put(word, wc.count.get());
			}
		}
		
		Set<Entry<String, Integer>> set = map.entrySet();
        ArrayList<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(set);
        Collections.sort( list, new Comparator<Map.Entry<String, Integer>>()
        {
            public int compare( Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2 )
            {
                return (o2.getValue()).compareTo( o1.getValue() );
            }
        } );
		
		String ranking = "";
		for(Map.Entry<String, Integer> entry:list){
            ranking+= entry.getKey()+" ";
        }
		output.collect(new Text(ranking),key);
		
	}

}