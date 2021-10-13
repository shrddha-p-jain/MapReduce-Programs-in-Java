import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Temperatuer {
  public static final class TMapper extends Mapper<Object, Text, Text, IntWritable>{
	int missing = 9999;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      final String line= value.toString();
      String year = line.substring(15,19);
      int temp;
      if(line.charAt(87)=='+') {
    	  temp = Integer.parseInt(line.substring(88,92));
      }
      else {
    	  temp = Integer.parseInt(line.substring(87,92));
      }
      String quality = line.substring(92,93);
      if(temp != missing && quality.matches("[01459]")) {
    	  context.write(new Text(year), new IntWritable(temp));
      }
    }
  }

  public static class TReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
   
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
      int max = Integer.MIN_VALUE;
      for (IntWritable val : values) {
        max = Math.max(max,val.get());
      }
      result.set(max);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "sales");
    job.setJarByClass(Temperatuer.class);
    job.setMapperClass(TMapper.class);
    job.setCombinerClass(TReducer.class);
 //   job.setReducerClass(TReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


