/**
 * Copyright (c) 2012, salesforce.com, inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 *    Redistributions of source code must retain the above copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 *    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 *    the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 *    Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or
 *    promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.salesforce.scoot;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.salesforce.scoot.Scoot;

/**
 * Utilities to make testing easier for scoot.
 *
 */
public class ScootTestUtils {

  // built-in parsers
  public final static String SCOOT_FILE_PARSER = "com.salesforce.scoot.parser.HBaseScootXMLParser"; 
  public final static String CLUSTER_PARSER = "com.salesforce.scoot.parser.HBaseClusterParser"; 
  public final static String PHOENIX_FILE_PARSER = "com.salesforce.scoot.parser.HBasePhoenixXMLParser";

  static final String hbaseConfDir = System.getProperty("localHBaseConfDir");
  static final String hbaseShellPath = System.getProperty("localHBaseShellPath");

  /**
   * Clear out all schema in the given cluster
   */
  public static void clearAllTables(String zkQuorum) throws Exception {
    // drop all tables by doing a diff with an empty schema as the "to"
    ScootTestUtils.generateAndRunDiffTest(
        zkQuorum, 
        CLUSTER_PARSER,
        "src/test/resources/EmptySchema.xml", 
        SCOOT_FILE_PARSER);
    
    // verify that the cluster now doesn't have these tables
    ScootTestUtils.generateAndCompareDiffTest(
        zkQuorum, 
        CLUSTER_PARSER,
        "src/test/resources/EmptySchema.xml",
        SCOOT_FILE_PARSER,
        "src/test/resources/EmptySchemaScript.rb");

  }

  /**
   * Parse the supplied schemas and generate a diff script, and then compare it to a gold file.
   */
  public static void generateAndCompareDiffTest(String fromSchemaFile, String fromSchemaType, String toSchemaFile, String toSchemaType, String goldDiffFile) throws Exception {
    String testOutputName = "generateAndCompareDiffTest_" + String.valueOf(System.currentTimeMillis()) + ".rb";
    try {
      runScootAndWriteFile(fromSchemaFile, fromSchemaType, toSchemaFile, toSchemaType, testOutputName);
      compareFiles(testOutputName, goldDiffFile);
    } finally {
      deleteFile(testOutputName);
    }
  }
  /**
   * Parse the supplied schemas and generate a diff script, and then run it on the cluster
   */
  public static void generateAndRunDiffTest(String fromSchemaFile, String fromSchemaType, String toSchemaFile, String toSchemaType) throws Exception {
    String testOutputName = "generateAndRunDiffTest_" + String.valueOf(System.currentTimeMillis()) + ".rb";
    try {
      runScootAndWriteFile(fromSchemaFile, fromSchemaType, toSchemaFile, toSchemaType, testOutputName);
      runShellCommand("HBASE_CONF_DIR=" + hbaseConfDir + " " + hbaseShellPath + "hbase shell " + testOutputName);
    } finally {
      deleteFile(testOutputName);
    }
  }

  /**
   * run scoot, and save the output file to the given location
   */
  private static void runScootAndWriteFile(String fromSchemaFile, String fromSchemaType, String toSchemaFile, String toSchemaType, String testOutputFileName) throws Exception {
    // add in args for where to save the temporary output file
    List<String> fullArgs = new ArrayList<String>(Arrays.asList(fromSchemaFile, toSchemaFile));
    fullArgs.add("-output");
    fullArgs.add(testOutputFileName);
    // if we've provided explicit file type args, add them
    if (fromSchemaType != null){
      fullArgs.add("-from-parser");
      fullArgs.add(fromSchemaType);
    }
    if (toSchemaType != null){
      fullArgs.add("-to-parser");
      fullArgs.add(toSchemaType);
    }
    new Scoot(fullArgs.toArray(new String[fullArgs.size()])).run();
  }

  private static void deleteFile(String testOutputName) throws Exception {
    try {
      File f = new File(testOutputName);
      f.delete();
    } catch (Exception e){
      // oh well. couldn't delete the test file.
    }
  }

  /**
   * Compare two files.
   */
  private static void compareFiles(String actual, String expected) throws Exception {
    Assert.assertEquals("Files did not match; actual result is in " + actual, read(expected), read(actual));
  }
  
  /**
   * Read the contents of the named file into a string
   */
  private static String read(String fileName) throws Exception{
    FileInputStream f =  new FileInputStream(fileName);
    BufferedReader r = new BufferedReader (new InputStreamReader(f));
    StringBuilder sb = new StringBuilder();
    String thisLine = null;
    while ((thisLine = r.readLine()) != null) {  
      sb.append(thisLine + "\n");
    }
    return sb.toString();
  }
  
  /**
   * Convert the given input stream to a string, using UTF-8 encoding
   */
  private static String inputStreamToString(InputStream inputStream) throws Exception {
    StringWriter writer = new StringWriter();
    IOUtils.copy(inputStream, writer, "UTF-8");
    return writer.toString();  
  }
  
  /**
   * Run an arbitrary command on a bash shell
   */
  private static void runShellCommand(String cmd) throws Exception {
    ProcessBuilder pb = new ProcessBuilder("bash", "-c", cmd); // create a process for the shell
    pb.redirectErrorStream(true); // use this to capture messages sent to stderr
    Process shell = pb.start();
    InputStream shellIn = shell.getInputStream(); // this captures the output from the command
    try {
      int shellExitStatus = shell.waitFor(); // wait for the shell to finish and get the return code
      if (shellExitStatus != 0) {
        // error? report on it and fail
        throw new Exception("Shell exited with non-zero status: " + shellExitStatus + "; " + inputStreamToString(shellIn));
      }
    } finally {
      shellIn.close();
    }
  }
  
  static HBaseAdmin getHBaseAdmin(String zkQuorum) throws Exception {
    Configuration config = HBaseConfiguration.create();
    config.set("hbase.zookeeper.quorum", zkQuorum);
	return new HBaseAdmin(config);
  }
}
