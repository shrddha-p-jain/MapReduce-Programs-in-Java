
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

public class SalesTotal {
  public static final class SMapper extends Mapper<Object, Text, IntWritable, DoubleWritable>{
 // need not be final, just there in ma'am's code.  
    //private Text word = new Text("Dummy");
    private DoubleWritable sale = new DoubleWritable();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      final String line= value.toString();
      final String[] data = line.split(",");
   //   String dummy = "Dummy";
    if(data.length == 6) {
    	  //final String product = data[3];
    	 // product.set(data[3].trim());
    	  sale.set(Double.parseDouble(data[4].trim()));
    	  context.write(new IntWritable(1),sale);
      }        
    }
  }

  public static class DoubleSumReducer extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {
   
    private DoubleWritable result = new DoubleWritable();
    Text counter = new Text();
    public void reduce(IntWritable key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
      double sum = 0;
      int count = 0;
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
    job.setJarByClass(SalesTotal.class);
    job.setMapperClass(SMapper.class);
//    job.setCombinerClass(DoubleSumReducer.class);
    job.setReducerClass(DoubleSumReducer.class);
  //  job.setMapOutputKeyClass(Text.class);
  //  job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


