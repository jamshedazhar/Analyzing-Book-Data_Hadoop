import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class NumFrequency {
	
	public static  class NumFrequencyMapper extends
    	Mapper<Object, Text, Text, Text> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context)
		    throws IOException, InterruptedException {
			String[] csv = value.toString().split("\t");
		    word.set(csv[0]);
		    context.write(word, new Text(csv[1]));
	    }
}
	
	public static class NumFrequencyReducer extends
    	Reducer<Text, Text, Text, Text> {

		@SuppressWarnings("unused")
		public void reduce(Text text, Iterable<Text> values, Context context)
		        throws IOException, InterruptedException {
		    int sum = 0;
		    for(Text value : values){
		    	sum +=1;
		    }	
		    context.write(text, new Text(""+sum));
		    sum = 0;
		}
}
 
    public static void numFrequency(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
    	
    	//Set input/ Output path
    	Path inputPath = new Path(args[0]+"/part-r-00000");
        Path outputDir = new Path(args[1]);
 
        // Create configuration
        Configuration conf = new Configuration(true);
 
        // Create job
		Job job = Job.getInstance(conf);
        job.setJobName("NumFrequency");
        job.setJarByClass(NumFrequency.class);
 
        // Setup MapReduce class
        job.setMapperClass(NumFrequencyMapper.class);
        job.setReducerClass(NumFrequencyReducer.class);
        
        // Set Output key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);
 
        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        // Execute job
        job.waitForCompletion(true);
        
        // Delete tmp folder created by count job
        FileSystem hdfs = FileSystem.get(conf);
        Path del = new Path(args[0]);
        if (hdfs.exists(del))
            hdfs.delete(del, true);
    }
}
