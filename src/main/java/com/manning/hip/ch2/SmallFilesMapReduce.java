package com.manning.hip.ch2;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SmallFilesMapReduce {

  /**
   * Uses default mapper with no reduces for a map-only identity job.
   */
  @SuppressWarnings("deprecation")
  public static void main(String... args) throws Exception {
    JobConf job = new JobConf();
    job.setJarByClass(SmallFilesMapReduce.class);
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);

    output.getFileSystem(job).delete(output);

    AvroJob.setInputSchema(job, SmallFilesWrite.SCHEMA);

    job.setOutputFormat(TextOutputFormat.class);

    AvroJob.setMapperClass(job, Mapper.class);
    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    job.setNumReduceTasks(0);                     // map-only

    JobClient.runJob(job);
  }

  public static class Mapper
      extends AvroMapper<GenericRecord, Pair<Void, Void>> {
    @Override
    public void map(GenericRecord r,
                    AvroCollector<Pair<Void, Void>> collector,
                    Reporter reporter) throws IOException {
      System.out.println(
          r.get(SmallFilesWrite.FIELD_FILENAME) +
              ": " +
              DigestUtils.md5Hex(
                  ((ByteBuffer) r.get(SmallFilesWrite.FIELD_CONTENTS))
                      .array()));
    }
  }


}
