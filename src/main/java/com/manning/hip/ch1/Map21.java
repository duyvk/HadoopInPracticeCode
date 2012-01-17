//<start id="ch01-04"/>
package com.manning.hip.ch1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map21
        extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static IntWritable ONE = new IntWritable(1); 

  @Override
  public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {
    String line = value.toString();    
    String[] words = line.split("\\W"); 
    for (String word : words) {
      if (word.trim().length() > 0) {
        Text text = new Text();
        text.set(word);
        context.write(text, ONE); 
      }
    }
  }
}//<end id="ch01-04"/>