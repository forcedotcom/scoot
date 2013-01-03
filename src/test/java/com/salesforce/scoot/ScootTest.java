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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.io.Resources;
import com.salesforce.scoot.parser.HBaseScootXMLParser;

/**
 * General tests for scoot functionality
 *
 */
public class ScootTest extends TestCase {

  /**
   * Make sure the help string is right. Temporarily redirect standard out to see it.
   */
  public void testHelp() throws Exception {
    PrintStream originalStdOut = System.out;
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos);
      System.setOut(ps);
      new Scoot(null).run();
      String output = Bytes.toString(baos.toByteArray());
      assertEquals("usage: scoot\n" + 
        " -f,--from <arg>           The schema you want to start with.\n" +
        " -fp,--from-parser <arg>   The parser to use for the 'from' schema. If not\n" +
        "                           supplied, the tool will attempt to auto-detect\n" +
        "                           it.\n" +
        " -h,--help <arg>           Get help on using this utility.\n" +
        " -o,--output <arg>         The name of the file to output.\n" +
        " -t,--to <arg>             The schema you want to end up with.\n" +
        " -tp,--to-parser <arg>     The parser to use for the 'to' schema. If not\n" +
        "                           supplied, the tool will attempt to auto-detect\n" +
        "                           it.\n", 
        output);
    } finally {
      System.setOut(originalStdOut);
    }
    
  }
  
  public void testParse() throws Exception {
	URL xmlFile = Resources.getResource("ScootXMLParserTest.xml");
	String fileName = xmlFile.getFile();
	HBaseScootXMLParser parser = new HBaseScootXMLParser();
	parser.setResourceToParse(fileName);
	HBaseSchema schema = parser.parse();
	  
	List<HTableDescriptor> tables = schema.getTables();
	assertEquals(1, tables.size());
	
	// Test table attributes
	HTableDescriptor table = tables.get(0);
	assertEquals("testMe", table.getNameAsString());
	assertEquals("536870912", table.getValue(HBaseSchemaAttribute.MAX_FILESIZE.name()));
	assertEquals("true", table.getValue(HBaseSchemaAttribute.READONLY.name()));
	assertEquals("128974848", table.getValue(HBaseSchemaAttribute.MEMSTORE_FLUSHSIZE.name()));
	assertEquals("true", table.getValue(HBaseSchemaAttribute.DEFERRED_LOG_FLUSH.name()));
	assertEquals("ivarley", table.getOwnerString());
	
	HColumnDescriptor[] columnFamilies = table.getColumnFamilies();
	assertNotNull(columnFamilies);
	assertEquals(1, columnFamilies.length);
	
	// Test column family attributes
	HColumnDescriptor columnFamily = columnFamilies[0];
	assertEquals("testMeColumnFamily1", columnFamily.getNameAsString());
	assertEquals("10", columnFamily.getValue(HBaseSchemaAttribute.VERSIONS.name()));
	assertEquals("32768", columnFamily.getValue(HBaseSchemaAttribute.BLOCKSIZE.name()));
	assertEquals("false", columnFamily.getValue(HBaseSchemaAttribute.BLOCKCACHE.name()));
	assertEquals("123456789", columnFamily.getValue(HBaseSchemaAttribute.TTL.name()));
	assertEquals("true", columnFamily.getValue(HBaseSchemaAttribute.IN_MEMORY.name()));
	assertEquals("ROW", columnFamily.getValue(HBaseSchemaAttribute.BLOOMFILTER.name()));
	assertEquals("true", columnFamily.getValue(HBaseSchemaAttribute.KEEP_DELETED_CELLS.name()));
	assertEquals("5", columnFamily.getValue(HBaseSchemaAttribute.MIN_VERSIONS.name()));
	assertEquals("1", columnFamily.getValue(HBaseSchemaAttribute.REPLICATION_SCOPE.name()));
	
  }
}
