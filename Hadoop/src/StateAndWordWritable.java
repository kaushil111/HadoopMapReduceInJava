import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StateAndWordWritable implements WritableComparable<StateAndWordWritable>{
	Text state;
	Text word;

	public StateAndWordWritable(Text state,Text word) {
		this.state = state;
		this.word = word;
	}
	public StateAndWordWritable() {
		this.state = new Text();
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
	    final StateAndWordWritable newObject = (StateAndWordWritable) obj;
	    if ((this.word == null) || (newObject.word != null) || (this.state == null) || (newObject.state != null)) {
	        return false;
	    }
		if (this.state.equals(newObject.state) && this.word.equals(newObject.word)) {
			return true;
		}
		
		
		return false;
	}
	
	@Override
	public String toString() {
		return state + "\t" + word;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.state.readFields(in);
        this.word.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.state.write(out);
        this.word.write(out);
	}

	@Override
	public int compareTo(StateAndWordWritable o) {
		
		if (o == null)
			return 0;
		int difference = this.state.compareTo(o.state);
		return difference == 0 ? this.word.compareTo(o.word) : difference;
		
	}
	
	
}
