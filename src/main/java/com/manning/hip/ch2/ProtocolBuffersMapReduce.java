package com.manning.hip.ch2;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.mapreduce.input.LzoProtobufBlockInputFormat;
import com.twitter.elephantbird.mapreduce.io.*;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

import static ch02.proto.ArtistCatalogProtos.Artist;

public class ProtocolBuffersMapReduce {

  public static class ProtobufArtistWritable   //<co id="ch02_comment_protobuf_mr1"/>
      extends ProtobufWritable<Artist> {
    public ProtobufArtistWritable() {
      super(new TypeRef<Artist>() {
      });
    }

    public ProtobufArtistWritable(Artist m) {
      super(m, new TypeRef<Artist>() {
      });
    }
  }

  /**
   * Uses default mapper with no reduces for a map-only identity job.
   */
  public static void main(String... args) throws Exception {
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);

    Configuration conf = new Configuration();

    output.getFileSystem(conf).delete(output, true);

    generateInput(conf, input);

    Job job = new Job(conf);
    job.setJobName(ProtocolBuffersMapReduce.class.getName());

    job.setJarByClass(ProtocolBuffersMapReduce.class);
    job.setMapperClass(PBMapper.class);
    job.setReducerClass(PBReducer.class);

    job.setMapOutputValueClass(ProtobufArtistWritable.class);  //<co id="ch02_comment_protobuf_mr2"/>

    job.setInputFormatClass(
        LzoProtobufBlockInputFormat   //<co id="ch02_comment_protobuf_mr3"/>
            .getInputFormatClass(Artist.class,
                job.getConfiguration()));

    job.setOutputFormatClass(         //<co id="ch02_comment_protobuf_mr4"/>
        LzoProtobufBlockOutputFormat.getOutputFormatClass(
            Artist.class, job.getConfiguration()));

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    job.waitForCompletion(true);
  }

  private static void generateInput(Configuration config,
                                    Path input) throws IOException {
    FileSystem hdfs = FileSystem.get(config);
    OutputStream os = hdfs.create(input);

    LzopCodec codec =
        new LzopCodec();       //<co id="ch02_comment_protobuf_mr5"/>
    codec.setConf(config);
    OutputStream lzopOutputStream = codec.createOutputStream(
        os);   //<co id="ch02_comment_protobuf_mr6"/>

    ProtobufBlockWriter<Artist> writer =
        new    //<co id="ch02_comment_protobuf_mr7"/>
            ProtobufBlockWriter<Artist>(
            lzopOutputStream, Artist.class);
    for (Artist artist : ProtocolBuffersWriter
        .createArtists()) {      //<co id="ch02_comment_protobuf_mr8"/>

      writer.write(artist);
    }
    writer.finish();
    writer.close();
    IOUtils.cleanup(null, os);
  }

  public static class PBMapper extends
      Mapper<LongWritable,  //<co id="ch02_comment_protobuf_mr9"/>
          ProtobufWritable<Artist>, LongWritable, ProtobufArtistWritable> {
    @Override
    protected void map(LongWritable key,
                       ProtobufWritable<Artist> value,
                       Context context) throws IOException,
        InterruptedException {
      context.write(key, new ProtobufArtistWritable(value.get()));
    }
  }

  public static class PBReducer extends
      Reducer<LongWritable,  //<co id="ch02_comment_protobuf_mr10"/>
          ProtobufArtistWritable,
          Artist,
          ProtobufArtistWritable> {
    @Override
    protected void reduce(LongWritable key,
                          Iterable<ProtobufArtistWritable> values,
                          Context context) throws IOException,
        InterruptedException {
      for (ProtobufArtistWritable artist : values) {
        context.write(artist.get(), artist);
      }
    }
  }
}
