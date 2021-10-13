/*
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesTotalMam {
  public static final class SMapper extends Mapper<Object, Text, Text, DoubleWritable>{
    private Text word = new Text("Dummy");
    private DoubleWritable sale = new DoubleWritable();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      final String line= value.toString();
      final String[] data = line.split(",");
   //   String dummy = "Dummy";
    if(data.length == 6) {
    	  sale.set(Double.parseDouble(data[4].trim()));
    	  context.write(word,sale);
      }        
    }
  }

  public static class DoubleSumReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
   
    private DoubleWritable result = new DoubleWritable();
    Text counter = new Text();
    int count = 0;
    public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
      double sum = 0;
      
      for (DoubleWritable val : values) {
    	  count++;
        sum += val.get();
      }
      
      result.set(sum);
      Text temp = new Text(count+"");
      context.write(temp, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "sales");
    job.setJarByClass(SalesTotalMam.class);
    job.setMapperClass(SMapper.class);
    job.setCombinerClass(DoubleSumReducer.class);
    job.setReducerClass(DoubleSumReducer.class);
  //  job.setMapOutputKeyClass(Text.class);
  //  job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
*/



import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesTotalMam {
  public static final class SMapper extends Mapper<Object, Text, Text, DoubleWritable>{
    private Text word = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      final String line= value.toString();
      final String[] data = line.split(",");
   //   String dummy = "Dummy";
    if(data.length == 6) {
    	final String product = "Dummy";
    	final Double sales = Double.parseDouble(data[4]);
    	word.set(product);
    	  context.write(word,new DoubleWritable(sales));
      }        
    }
  }

  public static class DoubleSumReducer extends Reducer<Text,DoubleWritable,IntWritable,DoubleWritable> {
   
    private DoubleWritable result = new DoubleWritable();
    Text counter = new Text();
    int count = 0;
    public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
      double sum = 0;
      
      for (DoubleWritable val : values) {
    	  count++;
        sum += val.get();
      }
      
      result.set(sum);

      context.write(new IntWritable(count), result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "sales");
    job.setJarByClass(SalesTotalMam.class);
    job.setMapperClass(SMapper.class);
    job.setCombinerClass(DoubleSumReducer.class);
    job.setReducerClass(DoubleSumReducer.class);
  //  job.setMapOutputKeyClass(Text.class);
  //  job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}