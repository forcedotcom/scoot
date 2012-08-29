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
package com.salesforce.scoot.parser;

import java.io.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.w3c.dom.*;

import com.google.common.base.Preconditions;
import com.salesforce.scoot.HBaseSchema;
import com.salesforce.scoot.ScootException;

/**
 * Parses a phoenix-style xml schema definition file into an in-memory 
 * representation of the schema. Only works with the "hard schema" portions
 * of the definitions (tables and column families) and ignores everything else.
 */
public class HBasePhoenixXMLParser extends HBaseSchemaParser {

  private File schemaFile;

  private static final String TABLE = "table";
  private static final String NAME = "name";
  private static final String COLUMN_FAMILIES = "columnFamilies";
  private static final String COLUMN_FAMILY = "columnFamily";

  public void setResourceToParse(String schemaFileName){
    this.schemaFile = new File(schemaFileName);
  }
    
    @Override
    public HBaseSchema parse() {
      Preconditions.checkNotNull(schemaFile, "Schema file reference must be set before parsing.");
      try {
        FileInputStream fis = new FileInputStream(schemaFile.getAbsoluteFile());
        try {
            return parseSchemaInputStream(fis);
        } finally {
            fis.close();
        }
      } catch (Exception ex){
        throw new ScootException("Error occurred with file input stream: " + ex.getMessage(), ex);
      }
    }
    
  private HBaseSchema parseSchemaInputStream(InputStream xmlFileInputStream) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(xmlFileInputStream);
    
    NodeList tables = doc.getElementsByTagName(TABLE);
    HBaseSchema s = new HBaseSchema();
    for (int i = 0; i < tables.getLength(); i++) {
        Node table = tables.item(i);
        s.addTable(getTable(table));
    }
    return s;
  }
    
  /**
   * Parse a table descriptor from the table node in the XML
   * TODO: get the other table attributes
   */
  private HTableDescriptor getTable(Node node) {
    NamedNodeMap tableAttr = node.getAttributes();
    String tableName = tableAttr.getNamedItem(NAME).getNodeValue();
    NodeList tableChildren = node.getChildNodes();
    HTableDescriptor t = new HTableDescriptor(tableName);
    for (int i = 0; i < tableChildren.getLength(); i++) {
      Node tableChild = tableChildren.item(i);
      if (tableChild.getNodeName().equals(COLUMN_FAMILIES)){
        Node columnFamilies = tableChild;
        NodeList columnFamiliesChildNodes = columnFamilies.getChildNodes();
        for (int x = 0; x < columnFamiliesChildNodes.getLength(); x++) {
          Node columnFamiliesChildNode = columnFamiliesChildNodes.item(x);
          if (columnFamiliesChildNode.getNodeName().equals(COLUMN_FAMILY)){
            t.addFamily(getColumnFamily(columnFamiliesChildNode));
          }
        }
      }
    }
    applyMissingTableDefaults(t);
    return t;
  }

  /**
   * Parse a column family descriptor from the cf node in the XML
   * TODO: get the other cf attributes
   */
  private HColumnDescriptor getColumnFamily(Node columnFamilyNode) {
    NamedNodeMap familyAttr = columnFamilyNode.getAttributes();
    String familyName = familyAttr.getNamedItem(NAME).getNodeValue();
    HColumnDescriptor cf = new HColumnDescriptor(familyName);
    applyMissingColumnFamilyDefaults(cf);
    return cf;
  }

}
