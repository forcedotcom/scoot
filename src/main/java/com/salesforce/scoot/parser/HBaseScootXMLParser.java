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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.common.base.Preconditions;
import com.salesforce.scoot.HBaseSchema;
import com.salesforce.scoot.HBaseSchemaAttribute;
import com.salesforce.scoot.ScootException;

/**
 * Parse the schema from a "scoot" style XML file
 */
public class HBaseScootXMLParser extends HBaseSchemaParser {
  
  private File schemaFile;

  /**
   * A mapping from the property names used in the scoot format XML file to the actual attribute
   * definitions used by HBase (see HBaseSchemaAttribute for the full list).
   */
  private static final Map<String, String> propertyNames = new HashMap<String,String>();
  static {
    // table
    propertyNames.put("useDeferredLogFlush", HBaseSchemaAttribute.DEFERRED_LOG_FLUSH.name());
    propertyNames.put("isReadOnly", HBaseSchemaAttribute.READONLY.name());
    propertyNames.put("maxFileSizeMB", HBaseSchemaAttribute.MAX_FILESIZE.name());
    propertyNames.put("memStoreFlushSizeMB", HBaseSchemaAttribute.MEMSTORE_FLUSHSIZE.name());
    propertyNames.put("owner", HBaseSchemaAttribute.OWNER.name());
    // column family
    propertyNames.put("blockCache", HBaseSchemaAttribute.BLOCKCACHE.name());
    propertyNames.put("blockSizeKB", HBaseSchemaAttribute.BLOCKSIZE.name());
    propertyNames.put("bloomFilter", HBaseSchemaAttribute.BLOOMFILTER.name());
    propertyNames.put("inMemory", HBaseSchemaAttribute.IN_MEMORY.name());
    propertyNames.put("keepDeletedCells", HBaseSchemaAttribute.KEEP_DELETED_CELLS.name());
    propertyNames.put("minVersions", HBaseSchemaAttribute.MIN_VERSIONS.name());
    propertyNames.put("maxVersions", HBaseSchemaAttribute.VERSIONS.name());
    propertyNames.put("replicationScope", HBaseSchemaAttribute.REPLICATION_SCOPE.name());
    propertyNames.put("timeToLiveMS", HBaseSchemaAttribute.TTL.name());
  }
  /**
   * Scoot's xml format uses more human-readable representations for big numbers, in MB or KB
   * as appropriate. This matches the property name with the multiplier that should be applied
   * to get back to bytes (which is what HBase needs).
   */
  static final Map<String, Integer> multiplier = new HashMap<String, Integer>();
  static final Integer MEGABYTE_TO_BYTE_CONVERSION = 1024 * 1024;
  static final Integer KILOBYTE_TO_BYTE_CONVERSION = 1024;
  static {
    // table
    multiplier.put("maxFileSizeMB", MEGABYTE_TO_BYTE_CONVERSION);
    multiplier.put("memStoreFlushSizeMB", MEGABYTE_TO_BYTE_CONVERSION);
    // column family
    multiplier.put("blockSizeKB", KILOBYTE_TO_BYTE_CONVERSION);
  }
  
  private static final String TABLE_ELEMENT = "table";
  private static final String TABLE_NAME_ATTRIBUTE = "name";
  private static final String COLUMN_FAMILIES_ELEMENT = "columnFamilies";
  private static final String FULL_SCHEMA_PROPERTY = "fullSchema";
  private static final String COLUMN_FAMILY_ELEMENT = "columnFamily";
  private static final String COLUMN_FAMILY_NAME_ATTRIBUTE = "name"; 

  
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
    } catch (IOException ex){
      throw new ScootException("Error occurred with file input stream: " + ex.getMessage(), ex);
    }
  }

  /**
   * Extract an object representation of the schema from an xml file
   */
  private HBaseSchema parseSchemaInputStream(FileInputStream inputStream) {
    
    HBaseSchema s = new HBaseSchema();
    
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    try {
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.parse(inputStream);
      NodeList tables = doc.getElementsByTagName(TABLE_ELEMENT);
      for (int x = 0; x < tables.getLength(); x++){
        Node table = tables.item(x);
        HTableDescriptor t = getTable(table);
        s.addTable(t);
      }
    } catch (Exception x) {
      throw new ScootException("Unable to parse schema file: " + x.getMessage(), x);
    }
    
    return s;
  }

  /**
   * Convert a <table> node from the xml into an HTableDescriptor (with its column families)
   */
  private HTableDescriptor getTable(Node tableNode) {
    NamedNodeMap tableAttributes = tableNode.getAttributes();
    String tableName = tableAttributes.getNamedItem(TABLE_NAME_ATTRIBUTE).getNodeValue();
    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
    for (int x = 0; x < tableAttributes.getLength(); x++){
      Node attr = tableAttributes.item(x);
      if (!attr.getNodeName().equalsIgnoreCase(TABLE_NAME_ATTRIBUTE)) { // skip name, already got it
        setAttributeValue(tableDescriptor, attr.getNodeName(), attr.getNodeValue());
      }
    }

    applyMissingTableDefaults(tableDescriptor);

    // parse the column families
    NodeList tableChildren = tableNode.getChildNodes();
    for (int x = 0; x < tableChildren.getLength(); x++) {
      Node tableChild = tableChildren.item(x);
      if (tableChild.getNodeName().equals(COLUMN_FAMILIES_ELEMENT)){
        for (HColumnDescriptor family : getColumnFamilies(tableChild)){
          tableDescriptor.addFamily(family);
        }
      }
    }
    
    // push this entire subtree of the xml file into the table metadata as the table's schema
    tableDescriptor.setValue(FULL_SCHEMA_PROPERTY, getFullXML(tableNode));

    validateTableDefinition(tableDescriptor);

    return tableDescriptor;
  }

  /**
   * Return a string of the full XML node, including all children, without linebreaks or indentation
   */
  private String getFullXML(Node tableNode) {
    try {
      StringWriter writer = new StringWriter();
      Transformer t = TransformerFactory.newInstance().newTransformer();
      t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
      t.setOutputProperty(OutputKeys.INDENT, "no");
      t.transform(new DOMSource(tableNode), new StreamResult(writer));
      return writer.toString().replaceAll(">[\\t\\s\\n\\r]+<", "><"); // remove whitespace and linebreaks between tags
    } catch (Exception e) {
      throw new ScootException("Error while serializing table xml.", e);
    }
  }

  /**
   * Use the propertyNames and multipliers map to attempt to cast the value to a strong type (if appropriate)
   * and apply a multiplier (if there is one), and then push the resulting name/value pair into this schema 
   * object's property collection.
   */
  private void setAttributeValue(Object schemaObject, String originalName, String originalValue) {
    String attributeName = propertyNames.containsKey(originalName) ? propertyNames.get(originalName) : originalName;
    String attributeValue = originalValue;

    HBaseSchemaAttribute a = HBaseSchemaAttribute.getFromName(attributeName);
    if (a != null) {
      try {
        a.tryParse(originalValue);
        // if this has a multiplier, and the parse passed, update the value using the multiplier
        if (a.type.equals(Integer.class) && multiplier.containsKey(originalName)) {
          attributeValue = String.valueOf(Integer.parseInt(originalValue) * multiplier.get(originalName));
        } else if ((a.type.equals(Long.class) && multiplier.containsKey(originalName))) {
          attributeValue = String.valueOf(Long.parseLong(originalValue) * multiplier.get(originalName));
        }
      } catch (Exception e) {
        throw new ScootException("Unable to parse value of '" + attributeValue + "' for attribute '" + attributeName + ".", e);
      }
    }
    
    // stupid runtime type inspection needed b/c tables & columns don't implement a common "value settable" interface
    if (schemaObject instanceof HTableDescriptor) {
      ((HTableDescriptor)schemaObject).setValue(attributeName, attributeValue);
    } else if (schemaObject instanceof HColumnDescriptor) {
      ((HColumnDescriptor)schemaObject).setValue(attributeName, attributeValue);
    } else {
      throw new ScootException("Wrong object type provided to set schema object value on:" + schemaObject.getClass());
    }
  }

  /**
   * Convert a <columnFamilies> node to a set of HColumnDescriptors
   */
  private Set<HColumnDescriptor> getColumnFamilies(Node columnFamiliesNode) {
    final Set<HColumnDescriptor> result = new HashSet<HColumnDescriptor>();
    
    NodeList columnFamilies = columnFamiliesNode.getChildNodes();
    for (int x = 0; x < columnFamilies.getLength(); x++){
      Node columnFamily = columnFamilies.item(x);
      if (columnFamily.getNodeName().equals(COLUMN_FAMILY_ELEMENT)) {
        NamedNodeMap columnFamilyAttributes = columnFamily.getAttributes();
        String familyName = columnFamilyAttributes.getNamedItem(COLUMN_FAMILY_NAME_ATTRIBUTE).getNodeValue();
        HColumnDescriptor cf = new HColumnDescriptor(familyName);
        for (int y = 0; y < columnFamilyAttributes.getLength(); y++){
          Node attr = columnFamilyAttributes.item(y);
          if (!attr.getNodeName().equalsIgnoreCase(COLUMN_FAMILY_NAME_ATTRIBUTE)) { // skip name, already got it
            setAttributeValue(cf, attr.getNodeName(), attr.getNodeValue());
          }
        }
        applyMissingColumnFamilyDefaults(cf);
        validateColumnFamily(cf);
        result.add(cf);
      }
    }
    return result;
  }
  

}
