import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class NumBook {
	
	public static  class BookMapper extends
    	Mapper<Object, Text, Text, Text> {
		private Text word = new Text();
		public void map(Object key, Text value, Context context)
		    throws IOException, InterruptedException {
			String[] csv = value.toString().split(";");
			String year = csv[3];
			year = year.replace("\"","").trim();
			try{
			     int yea = Integer.parseInt(year);
			     if(yea ==2002){
		    		word.set(csv[0].replace("\"","").trim());
		    		context.write(new Text(word), new Text("book"));
			     }
			}catch(Exception e){}
	    }
	}
 	public static  class RatingMapper extends
        Mapper<Object, Text, Text, Text> {
                private Text word = new Text();
                public void map(Object key, Text value, Context context)
                    throws IOException, InterruptedException {
                        String[] csv = value.toString().split(";");
                        word.set(csv[1].replace("\"","").trim());
                        context.write(new Text(word), new Text("rat:"+csv[2].replace("\"","").trim()));
                }
	}
    public static  class NumBookReducer extends
    	Reducer<Text, Text, Text, Text> {

			public void reduce(Text text, Iterable<Text> values, Context context)
			        throws IOException, InterruptedException {
				int flag = 0;
			    for (Text value : values) {
			        if(value.toString().contains("book"))
			        	flag =1;
			        else if(flag == 1 ){
			        	context.write(new Text(value.toString().split(":")[1]), new Text("1"));
			        }		        		
			    }
			    flag = 0;
			}
        }
    public static  void  main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

    	if(args.length<1){
    		System.out.println("USage: <Class name> <Output folder>");
    		System.exit(0);
    	}
    	
    	// Define All file path
    	Path bookDir = new Path("/input/BX-Books.csv");
    	Path ratingDir = new Path("/input/BX-Book-Ratings.csv");
        Path outputDir = new Path(args[0]);        
        Path tmp = new Path("/tmpmax");
        
 
        // Create configuration
        Configuration conf = new Configuration(true);
 
        // Create job
        Job job = Job.getInstance(conf);
        job.setJobName("NumBook");
        job.setJarByClass(NumBook.class);
        
        // Setup MapReduce class 	
        MultipleInputs.addInputPath(job, bookDir ,TextInputFormat.class, BookMapper.class);
        MultipleInputs.addInputPath(job, ratingDir ,TextInputFormat.class, RatingMapper.class);
        //job.setCombinerClass(NumBookCombiner.class);
        job.setReducerClass(NumBookReducer.class);

        // Set Output key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
	    // Output
        FileOutputFormat.setOutputPath(job, tmp);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Delete file if already exist
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
          hdfs.delete(outputDir, true);
        if(hdfs.exists(tmp))
        		hdfs.delete(tmp, true);
 
        // Execute  count job
        job.waitForCompletion(true);
        
        //Execute frequency job
        String []arg= {"/tmpmax",args[0]};
        NumFrequency.numFrequency(arg);
        
    }

	
}

