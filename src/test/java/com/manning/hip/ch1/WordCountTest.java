package com.manning.hip.ch1;

import com.google.common.io.Files;
import java.io.File;
import java.nio.charset.Charset;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 *
 * @author mark
 */
public class WordCountTest {
	static private String outputPath = "test-output/ch1";
  
  @BeforeClass
	public static void setupBase() throws Exception {
    System.out.println("hadoop20.setupBase");
    
    if (new File(outputPath).exists()) //<co id="ch01_comment2" />
    {
      Files.deleteRecursively(new File(outputPath));
    }		
	}
  
  @Test
  public void testMain() throws Exception {    
    System.out.println("hadoop20.testMain");
    String testData = "test-data/ch1/moby-dick.txt";
    String[] args = new String[2];
    args[0] = testData;
    args[1] = outputPath;
    WordCount.main(args);
    assert(new File("test-output/ch1/_SUCCESS").exists());
    assert(Files.readLines(new File("test-output/ch1/part-00000"), 
            Charset.defaultCharset()).
            size() == 19545);
  }
}
