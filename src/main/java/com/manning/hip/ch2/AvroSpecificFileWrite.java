package com.manning.hip.ch2;

import ch02.avro.gen.*;
import org.apache.avro.file.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.*;

public class AvroSpecificFileWrite {

  public static Artist[] generateSampleData() {
    Artist[] artists = new Artist[2];

    artists[0] = createArtist("Miles Davis", 43532, 1,
        Arrays.asList(createAlbum("In a Silent Way", GenreType.JAZZ)));
    artists[1] = createArtist("Billie Holiday", 26443, 2,
        Arrays.asList(
            createAlbum("All or Nothing at All", GenreType.BLUES),
            createAlbum("Songs For Distingue Lovers",
                GenreType.BLUES)));
    return artists;
  }

  public static Artist createArtist(String name, int playCount, int id,
                                    List<ch02.avro.gen.Album> albums) {
    Artist artist = new Artist();
    artist.name = name;
    artist.playCount = playCount;
    artist.id = id;
    artist.albums = albums;
    return artist;
  }

  public static Album createAlbum(String name, GenreType type) {
    Album album = new Album();
    album.name = name;
    album.type = type;
    return album;
  }

  public static void writeToAvro(OutputStream outputStream)
      throws IOException {
    DataFileWriter<Artist> writer = //<co id="ch02_smallfilewrite_comment2"/>
        new DataFileWriter<Artist>(new SpecificDatumWriter<Artist>())
            .setSyncInterval(100);
    writer.setCodec(CodecFactory.snappyCodec());   //<co id="ch02_smallfilewrite_comment3"/>
    writer.create(Artist.SCHEMA$, outputStream);

    for(Artist artist: generateSampleData()) {
      writer.append(artist);
    }

    IOUtils.cleanup(null, writer);
    IOUtils.cleanup(null, outputStream);
  }

  public static void main(String... args) throws Exception {
    Configuration config = new Configuration();
    FileSystem hdfs = FileSystem.get(config);

    Path destFile = new Path(args[0]);

    OutputStream os = hdfs.create(destFile);
    writeToAvro(os);
  }
}
