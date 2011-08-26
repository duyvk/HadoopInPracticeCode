package com.manning.hip.ch1;

import java.io.File;
import junit.framework.TestCase;

/**
 *
 * @author mark
 */
public class WordCountTest extends TestCase {
    
    public WordCountTest(String testName) {
        super(testName);
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }


    /**
     * Test of main method, of class WordCount.
     */
    public void testMain() throws Exception {
        System.out.println("main");
        String[] args = null;
        WordCount.main(args);
        assertTrue(new File("test-output/_SUCCESS").exists());
    }
}
