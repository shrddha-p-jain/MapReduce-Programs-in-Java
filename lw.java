import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class lw {


    public static class Map extends Mapper<Object, Text, Text, LongWritable> {
       // private final static IntWritable one = new IntWritable(1);
        int max = Integer.MIN_VALUE;
         private Text word = new Text();
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString(); //cumleni goturur file dan, 1 line i
            StringTokenizer tokenizer = new StringTokenizer(line); //cumleni sozlere bolur 
            while (tokenizer.hasMoreTokens()) {
                String s= tokenizer.nextToken();
                int val = s.length();
                    if(val>max) {
                        max=val;
                        word.set(s);

                    context.write(word,new LongWritable(val)); 

          }

        }
        }

    }


  public static class IntSumReducer
       extends Reducer<Text,LongWritable,Text,LongWritable> {
    private LongWritable result = new LongWritable();
    long max=-100;
    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

     // int sum = -1;

        for (LongWritable val : values) {
           if(val.get()>max) {
               max=val.get();

           }
          }
        result.set(max);

    }

    public void cleanup(Context context) throws IOException, InterruptedException {    
        context.write(new Text("longest"),result );   
    }
  }



  public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(Map.class);
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
      // job.setCombinerClass(IntSumReducer.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }


}