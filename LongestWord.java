import java.util.StringTokenizer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class LongestWord {

	public static class LWMapper extends Mapper<Object,Text,IntWritable, Text>  {
		private Text word = new Text();
		private IntWritable length = new IntWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()){
				word.set(itr.nextToken());
				length.set(word.getLength());
				context.write(length,word);
			
		}
		
	}
	}
	
	public static class LWReducer extends Reducer<IntWritable,Text,IntWritable,Text>{
		Text lw = new Text();
		String s = new String();
		public void reduce(IntWritable key, Iterable<Text> word, Context context)throws IOException, InterruptedException {
			Text lw = new Text();
			String s = new String();
			 for (Text val : word) {
				s = s+" "+val;
			 }
			
			lw.set(s); 
			context.write(key, lw);
			
		}
	}	
	
	public static void main(String[] args) throws  Exception {
		 Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "wordcount");
		    job.setJarByClass(LongestWord.class);
		    job.setMapperClass(LWMapper.class);
		    //job.setSortComparatorClass(DescendingKeyComparator.class);
		    job.setCombinerClass(LWReducer.class);
		    job.setReducerClass(LWReducer.class);
		    job.setOutputValueClass(Text.class);
		    job.setOutputKeyClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
