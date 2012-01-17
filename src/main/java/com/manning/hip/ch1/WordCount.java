//<start id="ch01-01"/>
package com.manning.hip.ch1;

import java.io.IOException;
import com.google.common.io.Files;
import java.io.File;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

  private static IntWritable ONE = new IntWritable(1); //<co id="ch01_comment1" />

  @Override
  public int run(String[] args) throws Exception {
    String testData = "test-data/ch1/moby-dick.txt";
    String outputPath = "test-output/ch1";
    Files.deleteRecursively(new File(outputPath)); //<co id="ch01_comment2" />
    Job job = new Job(getConf());
    job.setJarByClass(WordCount.class); //<co id="ch01_comment3" />
    job.setJobName("WordCount");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(testData));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new WordCount(), args);
    System.exit(ret);
  }

  public static class Map
          extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      String line = value.toString();
      String[] words = line.split("\\W"); //<co id="ch01_comment4" />
      for (String word : words) {
        if (word.trim().length() > 0) {
          Text text = new Text();
          text.set(word);
          context.write(text, ONE); //<co id="ch01_comment5" />
        }
      }
    }
  }

  public static class Reduce
          extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) { //<co id="ch01_comment6" />
        sum += val.get();
      }
      context.write(key, new IntWritable(sum)); //<co id="ch01_comment7" />
    }
  }
}
//<end id="ch01-01"/>