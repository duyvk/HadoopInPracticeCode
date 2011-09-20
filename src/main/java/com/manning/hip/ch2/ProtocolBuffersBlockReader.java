package com.manning.hip.ch2;

import com.twitter.elephantbird.mapreduce.io.ProtobufBlockReader;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import static ch02.proto.ArtistCatalogProtos.Artist;
public class ProtocolBuffersBlockReader {

  public static void readFromProtoBuf(InputStream inputStream)
      throws IOException {

    ProtobufBlockReader<Artist> reader =
        new ProtobufBlockReader<Artist>(
            inputStream, new TypeRef<Artist>() {});

    Artist artist;
    while((artist = reader.readNext()) != null) {
      System.out.println(ToStringBuilder.reflectionToString(artist)); //<co id="ch02_comment_protobuf_read2"/>
    }

    IOUtils.cleanup(null, inputStream);
  }

  public static void main(String... args) throws Exception {
    Configuration config = new Configuration();
    FileSystem hdfs = FileSystem.get(config);

    Path destFile = new Path(args[0]);

    InputStream is = hdfs.open(destFile);
    readFromProtoBuf(is);
  }
}
