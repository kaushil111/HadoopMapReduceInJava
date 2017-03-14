import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordAndCountWritable implements WritableComparable<WordAndCountWritable>{
	IntWritable count;
	Text word;

	public WordAndCountWritable(IntWritable count,Text word) {
		this.count = count;
		this.word = word;
	}
	public WordAndCountWritable() {
		this.count = new IntWritable(0);
		this.word = new Text();
	}
	
	@Override
	public boolean equals(Object obj) {		
		if (obj == null) {
	        return false;
	    }
	    if (!StateAndWordWritable.class.isAssignableFrom(obj.getClass())) {
	        return false;
	    }
	    final WordAndCountWritable newObject = (WordAndCountWritable) obj;
	    if ((this.word == null) || (newObject.word != null)) {
	        return false;
	    }
		if (this.word.equals(newObject.word) && this.count == newObject.count) {
			return true;
		}
		
		
		return false;
	}
	
	@Override
	public String toString() {
		return word + "\t" + count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.word.readFields(in);
        this.count.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
        this.word.write(out);
        this.count.write(out);
	}
	@Override
	public int compareTo(WordAndCountWritable o) {
		if (o == null)
			return 0;
		int difference = this.word.compareTo(o.word);
		return difference == 0 ? this.count.compareTo(o.count) : difference;
		
	}

	
	
}
