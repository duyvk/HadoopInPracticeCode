package com.manning.hip.ch2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;

/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for CSV
 * plain text files.  Files are broken into lines, and each token
 * in each line can be transformed in a custom fashion by supplying
 * a {@link CSVLineTokenTransformer}.  Keys are byte offsets in
 * the file, and values are {@link ArrayWritable}'s with tokenized
 * values.
 */
public class CSVInputFormat extends
    FileInputFormat<LongWritable, ArrayWritable> {

  public static String CSV_TOKEN_SEPARATOR_CONFIG =
      "csvinputformat.token.delimiter";

  @Override
  public RecordReader<LongWritable, ArrayWritable>
  createRecordReader(InputSplit split,
                     TaskAttemptContext context) {
    String csvDelimiter = context.getConfiguration().get( //<co id="ch02_comment_csv_inputformat1"/>
        CSV_TOKEN_SEPARATOR_CONFIG);

    String transformerClass = context.getConfiguration().get(
        "csvinputformat.tranformer.class");
    CSVLineTokenTransformer transformer = null;
    if (null != transformerClass) {
      try {
        transformer =                      //<co id="ch02_comment_csv_inputformat2"/>
            (CSVLineTokenTransformer) Class.forName(transformerClass)
                .newInstance();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return new CSVRecordReader(csvDelimiter, transformer);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    CompressionCodec codec =
        new CompressionCodecFactory(context.getConfiguration())
            .getCodec(file);
    return codec == null;    //<co id="ch02_comment_csv_inputformat3"/>
  }

  public static class CSVRecordReader              //<co id="ch02_comment_csv_inputformat4"/>
      extends RecordReader<LongWritable, ArrayWritable> {
    public final static String DEFAULT_CSV_DELIMITER = ",";
    private CSVLineTokenTransformer transformer;
    private LineRecordReader reader;
    private ArrayWritable value;
    private String csvDelimiter = DEFAULT_CSV_DELIMITER;

    public CSVRecordReader(String csvDelimiter,
                           CSVLineTokenTransformer transformer) {
      this.reader = new LineRecordReader();
      this.transformer = transformer;
      if (csvDelimiter != null) {
        this.csvDelimiter = csvDelimiter;
      }
    }

    @Override
    public void initialize(InputSplit split,
                           TaskAttemptContext context)
        throws IOException, InterruptedException {
      reader.initialize(split, context);     //<co id="ch02_comment_csv_inputformat5"/>
    }

    @Override
    public boolean nextKeyValue()
        throws IOException, InterruptedException {
      if (reader.nextKeyValue()) {       //<co id="ch02_comment_csv_inputformat6"/>
        loadCSV();                        //<co id="ch02_comment_csv_inputformat7"/>
        return true;
      } else {
        value = null;
        return false;
      }
    }

    private void loadCSV() {            //<co id="ch02_comment_csv_inputformat8"/>
      String line = reader.getCurrentValue().toString();
      String[] tokens =
          StringUtils.splitPreserveAllTokens(line, csvDelimiter);   //<co id="ch02_comment_csv_inputformat9"/>
      if (transformer != null) {
        for (int i = 0; i < tokens.length; i++) {
          tokens[i] = transformer.transform(line, i, tokens[i]);
        }
      }
      value = new ArrayWritable(tokens);
    }

    @Override
    public LongWritable getCurrentKey()      //<co id="ch02_comment_csv_inputformat10"/>
        throws IOException, InterruptedException {
      return reader.getCurrentKey();
    }

    @Override
    public ArrayWritable getCurrentValue()    //<co id="ch02_comment_csv_inputformat11"/>
        throws IOException, InterruptedException {
      return value;
    }

    @Override
    public float getProgress()
        throws IOException, InterruptedException {
      return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }

  public static interface CSVLineTokenTransformer {
    String transform(String line, int tokenIdx, String token);
  }

}
