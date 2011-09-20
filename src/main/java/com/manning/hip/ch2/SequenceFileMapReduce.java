package com.manning.hip.ch2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;

public class SequenceFileMapReduce {
  public static void main(String... args) throws Exception {
    runJob(args[0], args[1]);
  }

  public static class Map extends
      Mapper<Text, IntWritable,  //<co id="ch02_comment_seqfile_mr1"/>
          Text, IntWritable> {
    @Override
    protected void map(Text key, IntWritable value, Context context)
        throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public static class Reduce extends
      Reducer<Text, IntWritable,  //<co id="ch02_comment_seqfile_mr2"/>
          Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      for(IntWritable value: values) {
        context.write(key, value);
      }
    }
  }

  public static void runJob(String input,
                            String output)
      throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf);
    job.setJarByClass(SequenceFileMapReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(
        SequenceFileInputFormat.class); //<co id="ch02_comment_seqfile_mr3"/>
    job.setNumReduceTasks(
        1);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);  //<co id="ch02_comment_seqfile_mr4"/>
    SequenceFileOutputFormat.setCompressOutput(job, true);  //<co id="ch02_comment_seqfile_mr5"/>
    SequenceFileOutputFormat.setOutputCompressionType(job,  //<co id="ch02_comment_seqfile_mr6"/>
        SequenceFile.CompressionType.BLOCK);
    SequenceFileOutputFormat.setOutputCompressorClass(job,  //<co id="ch02_comment_seqfile_mr7"/>
        DefaultCodec.class);

    FileInputFormat.setInputPaths(job, new Path(input));
    Path outPath = new Path(output);
    FileOutputFormat.setOutputPath(job, outPath);
    HadoopUtil.delete(conf, outPath);

    job.waitForCompletion(true);
  }
}
