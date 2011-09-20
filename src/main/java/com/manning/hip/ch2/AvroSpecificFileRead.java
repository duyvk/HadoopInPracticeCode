package com.manning.hip.ch2;

import ch02.avro.gen.Artist;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

public class AvroSpecificFileRead {

  public static void readFromAvro(InputStream is) throws IOException {
    DataFileStream<Artist> reader =     //<co id="ch02_smallfileread_comment1"/>
        new DataFileStream<Artist>(
            is, new SpecificDatumReader<Artist>());
    for (Artist a : reader) {          //<co id="ch02_smallfileread_comment2"/>
      System.out.println(ToStringBuilder.reflectionToString(a));
    }
    IOUtils.cleanup(null, is);
    IOUtils.cleanup(null, reader);
  }

  public static void main(String... args) throws Exception {
    Configuration config = new Configuration();
    FileSystem hdfs = FileSystem.get(config);

    Path destFile = new Path(args[0]);

    InputStream is = hdfs.open(destFile);
    readFromAvro(is);
  }
}
