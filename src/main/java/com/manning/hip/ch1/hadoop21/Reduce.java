package com.manning.hip.ch1.hadoop21;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce
        extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(
          Text key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) { //<co id="ch01_comment6" />
      sum += val.get();
    }
    context.write(key, new IntWritable(sum)); //<co id="ch01_comment7" />
  }
}