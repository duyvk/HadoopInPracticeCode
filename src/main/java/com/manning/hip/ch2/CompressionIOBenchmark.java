package com.manning.hip.ch2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class CompressionIOBenchmark {

  public static void main(String... args) throws Exception {
    Configuration config = new Configuration();

    Class<?> codecClass = Class.forName(args[0]);
    CompressionCodec codec = (CompressionCodec)
        ReflectionUtils.newInstance(codecClass, config);

    Path srcFile = new Path(args[1]);
    Path destFile = new Path(args[2]);
    Path uncompressedDestFile = new Path(args[3]);

    FileSystem hdfs = FileSystem.get(config);

    long start = System.currentTimeMillis();
    compress(srcFile, destFile, codec, config);
    long end = System.currentTimeMillis();
    System.out.println("Compression time = " + (end - start) +
        "ms, compressed file size = " +
        hdfs.getFileStatus(destFile).getLen());

    start = System.currentTimeMillis();
    decompress(srcFile, destFile, codec, config);
    end = System.currentTimeMillis();
    System.out.println("Decompression time = " + (end - start) + "ms");
  }

  public static void compress(Path src, Path dest,
                              CompressionCodec codec,
                              Configuration config) throws IOException {
    FileSystem hdfs = FileSystem.get(config);
    InputStream is = null;
    OutputStream os = null;
    try {
      is = hdfs.open(src);
      os = codec.createOutputStream(hdfs.create(dest));

      IOUtils.copyBytes(is, os, config);
    } finally {
      IOUtils.cleanup(null, os);
      IOUtils.cleanup(null, is);
    }
  }

  public static void decompress(Path src, Path dest,
                                CompressionCodec codec,
                                Configuration config)
      throws IOException {
    FileSystem hdfs = FileSystem.get(config);
    InputStream is = null;
    OutputStream os = null;
    try {
      is = codec.createInputStream(hdfs.open(src));
      os = hdfs.create(dest);

      IOUtils.copyBytes(is, os, config);
    } finally {
      IOUtils.cleanup(null, os);
      IOUtils.cleanup(null, is);
    }
  }
}
