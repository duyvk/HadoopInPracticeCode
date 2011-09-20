package com.manning.hip.ch2;

import ch02.avro.gen.Artist;
import org.apache.avro.Schema;
import org.apache.avro.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

import static org.apache.avro.file.DataFileConstants.SNAPPY_CODEC;

public class AvroMapReduce {

  /**
   * Uses default mapper with no reduces for a map-only identity job.
   */

  public static void main(String... args) throws Exception {
    JobConf job = new JobConf();
    job.setJarByClass(SmallFilesMapReduce.class);
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);

    output.getFileSystem(job).delete(output);

    AvroJob.setInputSchema(job,
        Artist.SCHEMA$);  //<co id="ch02_smallfilemr_comment1"/>
    AvroJob.setMapOutputSchema(job, Pair.getPairSchema(Artist.SCHEMA$,
        Schema.create(Schema.Type.NULL)));
    AvroJob.setOutputSchema(job,
        Artist.SCHEMA$);  //<co id="ch02_smallfilemr_comment1"/>

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    AvroJob.setMapperClass(job,
        Mapper.class);    //<co id="ch02_smallfilemr_comment2"/>
    AvroJob.setReducerClass(job,
        Reducer.class);    //<co id="ch02_smallfilemr_comment2"/>

    FileOutputFormat.setCompressOutput(job, true);
    AvroJob.setOutputCodec(job, SNAPPY_CODEC);

    JobClient.runJob(job);
  }

  public static class Mapper
      extends
      AvroMapper<Artist, Pair<Artist, Void>> {   //<co id="ch02_smallfilemr_comment3"/>

    @Override
    public void map(Artist artist,
                    AvroCollector<Pair<Artist, Void>> collector,
                    Reporter reporter) throws IOException {
      collector.collect(new Pair<Artist, Void>(artist, (Void) null));
    }
  }

  public static class Reducer
      extends AvroReducer<Artist, Void, Artist> {
    @Override
    public void reduce(Artist w, Iterable<Void> ignore,
                       AvroCollector<Artist> collector,
                       Reporter reporter) throws IOException {
      collector.collect(w);
    }
  }
}
