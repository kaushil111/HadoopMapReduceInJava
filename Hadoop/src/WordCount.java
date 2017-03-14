
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		if(args.length<2) {
			System.out.println("Please provide the input and output directories!");
			return -1;
		}
		JobConf conf = new JobConf(WordCount.class);
		FileSystem fs= FileSystem.get(conf); 
		//get the FileStatus list from given dir
		FileStatus[] status_list = fs.listStatus(new Path(args[0]));
		if(status_list != null){
		    for(FileStatus status : status_list){
		        //add each file to the list of inputs for the map-reduce job
		        FileInputFormat.addInputPath(conf, status.getPath());
		    }
		}
		FileOutputFormat.setOutputPath(conf, new Path(args[1]+"/Output1"));
	      
		conf.setMapperClass(WordMapper.class);
		conf.setReducerClass(WordReducer.class);
		conf.setMapOutputKeyClass(StateAndWordWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(StateAndWordWritable.class);
		conf.setOutputValueClass(IntWritable.class);
		
		RunningJob job = JobClient.runJob(conf);
		job.waitForCompletion();
		
		if (job.isSuccessful()) {
			JobConf conf2 = new JobConf(WordCount.class);
			fs= FileSystem.get(conf2); 
			//get the FileStatus list from given dir
			status_list = fs.listStatus(new Path(args[1]+"/Output1"));
			if(status_list != null){
			    for(FileStatus status : status_list){
			        //add each file to the list of inputs for the map-reduce job
			    	if (status.getLen() > 0) {
			    		FileInputFormat.addInputPath(conf2, status.getPath());
					}
			        
			    }
			}
			FileOutputFormat.setOutputPath(conf2, new Path(args[1]+"/Output2"));
		      
			conf2.setMapperClass(DominantMapper.class);
			conf2.setReducerClass(DominantReducer.class);
			conf2.setInputFormat(KeyValueTextInputFormat.class);
			conf2.setMapOutputKeyClass(Text.class);
			conf2.setMapOutputValueClass(WordAndCountWritable.class);
			conf2.setOutputKeyClass(Text.class);
			conf2.setOutputValueClass(Text.class);
			job = JobClient.runJob(conf2);
			job.waitForCompletion();
		}
		
		if (job.isSuccessful()) {
			JobConf conf3 = new JobConf(WordCount.class);
			fs= FileSystem.get(conf3); 
			//get the FileStatus list from given dir
			status_list = fs.listStatus(new Path(args[1]+"/Output2"));
			if(status_list != null){
			    for(FileStatus status : status_list){
			        //add each file to the list of inputs for the map-reduce job
			    	if (status.getLen() > 0) {
			    		FileInputFormat.addInputPath(conf3, status.getPath());
					}
			        
			    }
			}
			FileOutputFormat.setOutputPath(conf3, new Path(args[1]+"/Output3"));
		      
			conf3.setMapperClass(DominantCountMapper.class);
			conf3.setReducerClass(DominantCountReducer.class);
			conf3.setInputFormat(KeyValueTextInputFormat.class);
			conf3.setMapOutputKeyClass(Text.class);
			conf3.setMapOutputValueClass(IntWritable.class);
			conf3.setOutputKeyClass(Text.class);
			conf3.setOutputValueClass(IntWritable.class);
			job = JobClient.runJob(conf3);
			job.waitForCompletion();
		}
		
		if (job.isSuccessful()) {
			JobConf conf4 = new JobConf(WordCount.class);
			fs= FileSystem.get(conf4); 
			//get the FileStatus list from given dir
			status_list = fs.listStatus(new Path(args[1]+"/Output1"));
			if(status_list != null){
			    for(FileStatus status : status_list){
			        //add each file to the list of inputs for the map-reduce job
			    	if (status.getLen() > 0) {
			    		FileInputFormat.addInputPath(conf4, status.getPath());
					}
			        
			    }
			}
			FileOutputFormat.setOutputPath(conf4, new Path(args[1]+"/Output4"));
		      
			conf4.setMapperClass(DominantMapper.class);
			conf4.setReducerClass(RankingReducer.class);
			conf4.setInputFormat(KeyValueTextInputFormat.class);
			conf4.setMapOutputKeyClass(Text.class);
			conf4.setMapOutputValueClass(WordAndCountWritable.class);
			conf4.setOutputKeyClass(Text.class);
			conf4.setOutputValueClass(Text.class);
			job = JobClient.runJob(conf4);
			job.waitForCompletion();
		}
		
		if (job.isSuccessful()) {
			JobConf conf5 = new JobConf(WordCount.class);
			fs= FileSystem.get(conf5); 
			//get the FileStatus list from given dir
			status_list = fs.listStatus(new Path(args[1]+"/Output4"));
			if(status_list != null){
			    for(FileStatus status : status_list){
			        //add each file to the list of inputs for the map-reduce job
			    	if (status.getLen() > 0) {
			    		FileInputFormat.addInputPath(conf5, status.getPath());
					}
			        
			    }
			}
			FileOutputFormat.setOutputPath(conf5, new Path(args[1]+"/Output5"));
		      
			conf5.setMapperClass(SameRankingMapper.class);
			conf5.setReducerClass(SameRankingReducer.class);
			conf5.setInputFormat(KeyValueTextInputFormat.class);
			conf5.setMapOutputKeyClass(Text.class);
			conf5.setMapOutputValueClass(Text.class);
			conf5.setOutputKeyClass(Text.class);
			conf5.setOutputValueClass(Text.class);
			job = JobClient.runJob(conf5);
			job.waitForCompletion();
		}
		
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WordCount(), args);
		System.exit(exitCode);		
	}

}
