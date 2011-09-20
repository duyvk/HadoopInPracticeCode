package com.manning.hip.ch2;

import com.manning.hip.ch2.proto.ArtistCatalogProtos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.*;

public class ProtocolBuffersWriter {

  public static void writeToProtoBuf(OutputStream outputStream)
          throws IOException {

    ArtistCatalogProtos.ArtistCatalog.Builder catalog = //<co id="ch02_comment_protobuf_write1"/>
            ArtistCatalogProtos.ArtistCatalog.newBuilder();

    for (ArtistCatalogProtos.Artist artist : createArtists()) {
      catalog.addArtist(artist);
    }

    catalog.build().writeTo(outputStream);    //<co id="ch02_comment_protobuf_write2"/>

    IOUtils.cleanup(null, outputStream);
  }

  public static List<ArtistCatalogProtos.Artist> createArtists() {
    ArtistCatalogProtos.Artist artist1 =
            ArtistCatalogProtos.Artist.newBuilder().setName("Miles Davis").setPlayCount(43532).setId(1).addAlbum(
            ArtistCatalogProtos.Artist.Albums.newBuilder().setName("In a Silent Way").setType(
            ArtistCatalogProtos.Artist.GenreType.JAZZ)).build();

    ArtistCatalogProtos.Artist artist2 =
            ArtistCatalogProtos.Artist.newBuilder().setName("Billie Holiday").setPlayCount(26443).setId(2).addAlbum(
            ArtistCatalogProtos.Artist.Albums.newBuilder().setName("All or Nothing at All").setType(
            ArtistCatalogProtos.Artist.GenreType.BLUES)).addAlbum(
            ArtistCatalogProtos.Artist.Albums.newBuilder().setName("Songs For Distingue Lovers").setType(
            ArtistCatalogProtos.Artist.GenreType.BLUES)).build();
    return Arrays.asList(artist1, artist2);
  }

  public static void main(String... args) throws Exception {
    Configuration config = new Configuration();
    FileSystem hdfs = FileSystem.get(config);

    Path destFile = new Path(args[0]);

    OutputStream os = hdfs.create(destFile);
    writeToProtoBuf(os);
  }
}
