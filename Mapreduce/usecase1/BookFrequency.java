package case1;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class BookFrequency {
	
	public static  class BookFrequencyMapper extends
    	Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context)
		    throws IOException, InterruptedException {
			String[] csv = value.toString().split(";");
			String year = csv[3];
			year = year.replace("\"","");
			try{
				int yea = Integer.parseInt(year);
		    word.set(Integer.toString(yea));
		    context.write(word, new IntWritable(1));
			}catch(Exception e){}
	    }
}
	
	public static class BookFrequencyReducer extends
    	Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text text, Iterable<IntWritable> values, Context context)
		        throws IOException, InterruptedException {
		    int sum = 0;
		    for (IntWritable value : values) {
		        sum+=value.get();
		    }
		    
		    context.write(text, new IntWritable(sum));
		    sum=0;
		}
}
 
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

    	if(args.length<2){
    		System.out.println("USage: <Class name> <input file><Output folder>");
    		System.exit(0);
    	}
    	
    	//Set input/ Output path
    	Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
 
        // Create configuration
        Configuration conf = new Configuration(true);
 
        // Create job
		Job job = Job.getInstance(conf);
        job.setJobName("BookFrequency");
        job.setJarByClass(BookFrequency.class);
 
        // Setup MapReduce class
        job.setMapperClass(BookFrequencyMapper.class);
        job.setReducerClass(BookFrequencyReducer.class);
        
        // Set Output key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);
 
        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        // Execute job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

