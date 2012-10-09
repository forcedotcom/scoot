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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * Tests to generate diff scripts based on various inputs. This set of tests
 * refers to xml files in the /src/test/resources directory.
 */
public class DiffScriptGenerationTest {
  
  /**
   * Test that you get the expected upgrade script from a mixed set of
   * table definitions including create, alter, drop and ignore 
   */
  @Test
  public void testDiffTwoScootXMLSchemas() throws Exception {
    ScootTestUtils.generateAndCompareDiffTest(
        "src/test/resources/DiffScriptGenerationTestA.xml", 
        ScootTestUtils.SCOOT_FILE_PARSER,
        "src/test/resources/DiffScriptGenerationTestB.xml",
        null, //ScootTestUtils.SCOOT_FILE_PARSER,
        "src/test/resources/DiffScriptGenerationTestResultAB.rb");
  }

  /**
   * Test that if you tell it to presplit a table, it scripts it correctly  
   */
  @Test
  public void testScriptPresplit() throws Exception {
    ScootTestUtils.clearAllTables("localhost:2181");
    
    // generate a single table with presplit
    ScootTestUtils.generateAndCompareDiffTest(
            "src/test/resources/EmptySchema.xml",
            ScootTestUtils.SCOOT_FILE_PARSER,
            "src/test/resources/DiffScriptGenerationTestC.xml", 
            ScootTestUtils.SCOOT_FILE_PARSER,
            "src/test/resources/DiffScriptGenerationTestResultC.rb");

    // run it
    ScootTestUtils.generateAndRunDiffTest(
            "localhost:2181",
            ScootTestUtils.CLUSTER_PARSER,
            "src/test/resources/DiffScriptGenerationTestC.xml",
            ScootTestUtils.SCOOT_FILE_PARSER);
    
    // verify that the cluster now has the "createMe" table with the right number of regions
    assertEquals("Invalid number of regions", 12, ScootTestUtils.getHBaseAdmin("localhost:2181").getTableRegions(Bytes.toBytes("createMe")).size());
    
  }

  /**
   * Test that if you don't supply a parser, it uses the scoot parser
   * for files ending in ".xml", and the cluster parser for anything else  
   */
  @Test
  public void testDefaultParser() throws Exception {
    ScootTestUtils.clearAllTables("localhost:2181");
    ScootTestUtils.generateAndCompareDiffTest(
        "localhost:2181", 
        null, // should assume it's a cluster name
        "src/test/resources/EmptySchema.xml",
        null, // should assume it's a scoot xml file
        "src/test/resources/EmptySchemaScript.rb");
  }

  /**
   * Test that you can clean out all tables, script a single table, and 
   * see that it was created. 
   */
  @Test
  public void testDiffClusterAndXMLSchema() throws Exception {
    ScootTestUtils.clearAllTables("localhost:2181");
    
    // now put on a single table
    ScootTestUtils.generateAndRunDiffTest(
        "localhost:2181",
        ScootTestUtils.CLUSTER_PARSER,
        "src/test/resources/DiffScriptGenerationTestD.xml",
        ScootTestUtils.SCOOT_FILE_PARSER);
    
    // verify that the cluster now has the "createMe" table
    ScootTestUtils.generateAndCompareDiffTest(
        "localhost:2181", 
        ScootTestUtils.CLUSTER_PARSER,
        "src/test/resources/EmptySchema.xml",
        ScootTestUtils.SCOOT_FILE_PARSER,
        "src/test/resources/DiffScriptGenerationTestResultD.rb");
    
  }
  
  /**
   * Test that the minimal amount of schema (a single table with a single CF, with
   * no explicitly set properties) can be created on the cluster, and then diffing
   * the cluster with that creation file shows no changes needed.
   */
  @Test 
  public void testMinimalSchemaCreation() throws Exception {
    
    ScootTestUtils.clearAllTables("localhost:2181");
    
    // put on a single table with a minimal definition
    ScootTestUtils.generateAndRunDiffTest(
        "localhost:2181",
        ScootTestUtils.CLUSTER_PARSER,
        "src/test/resources/DiffScriptGenerationTestE.xml",
        ScootTestUtils.SCOOT_FILE_PARSER);

    // verify that the cluster's version of the table matches another schema file with it explicitly defined
    ScootTestUtils.generateAndCompareDiffTest(
        "localhost:2181", 
        ScootTestUtils.CLUSTER_PARSER,
        "src/test/resources/DiffScriptGenerationTestF.xml",
        ScootTestUtils.SCOOT_FILE_PARSER,
        "src/test/resources/DiffScriptGenerationTestResultEF.rb");

  }

  /**
   * Do the opposite of testMinimalSchemaCreation: put one with explicit schema on the cluster, then compare it 
   * to one with minimal schema
   */
  @Test
  public void testMinimalSchemaComparison() throws Exception {

    ScootTestUtils.clearAllTables("localhost:2181");
    
    // put on a single table with a minimal definition
    ScootTestUtils.generateAndRunDiffTest(
        "localhost:2181",
        ScootTestUtils.CLUSTER_PARSER,
        "src/test/resources/DiffScriptGenerationTestF.xml",
        ScootTestUtils.SCOOT_FILE_PARSER);

    // verify that the cluster's version of the table matches another schema file with it explicitly defined
    ScootTestUtils.generateAndCompareDiffTest(
        "localhost:2181", 
        ScootTestUtils.CLUSTER_PARSER,
        "src/test/resources/DiffScriptGenerationTestE.xml",
        ScootTestUtils.SCOOT_FILE_PARSER,
        "src/test/resources/DiffScriptGenerationTestResultFE.rb");


  }
}
