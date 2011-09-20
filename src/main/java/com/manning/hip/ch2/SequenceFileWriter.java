package com.manning.hip.ch2;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;

import java.io.*;

public class SequenceFileWriter {

  public static void main(String... args) throws IOException {
    write(new File(args[0]), new Path(args[1]));
  }

  public static void write(File inputFile, Path outputPath)
      throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Writer writer =   //<co id="ch02_comment_seqfile_write1"/>
        SequenceFile.createWriter(fs, conf, outputPath, Text.class,
            IntWritable.class, SequenceFile.CompressionType.BLOCK,
            new DefaultCodec());
    try {
      Text key = new Text();
      IntWritable value = new IntWritable();

      for (String line : FileUtils.readLines(inputFile)) {   //<co id="ch02_comment_seqfile_write2"/>
        String[] parts = line.split("\t");
        key.set(parts[0]);
        value.set(Integer.valueOf(parts[1]));
        writer.append(key, value);        //<co id="ch02_comment_seqfile_write3"/>
      }
    } finally {
      writer.close();
    }
  }
}
