package com.manning.hip.ch2;

import ch02.proto.ArtistCatalogProtos;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

public class ProtocolBuffersReader {

  public static void readFromProtoBuf(InputStream inputStream)
      throws IOException {

    ArtistCatalogProtos.ArtistCatalog catalog =    //<co id="ch02_comment_protobuf_read1"/>
        ArtistCatalogProtos.ArtistCatalog.parseFrom(inputStream);

    System.out.println(ToStringBuilder.reflectionToString(catalog)); //<co id="ch02_comment_protobuf_read2"/>

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
