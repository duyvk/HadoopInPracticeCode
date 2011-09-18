package com.manning.hip.ch1.hadoop20;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author mark
 */
public class Map
        extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

  private static IntWritable ONE = new IntWritable(1); //<co id="ch01_comment1" />
  private Text word = new Text();

  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
    String line = value.toString();
    String[] tokens = line.split("\\W"); //<co id="ch01_comment4" />
    for (String token : tokens) {
      if (token.trim().length() > 0) {
        word.set(token);
        output.collect(word, ONE); //<co id="ch01_comment5" />
      }
    }
  }
}