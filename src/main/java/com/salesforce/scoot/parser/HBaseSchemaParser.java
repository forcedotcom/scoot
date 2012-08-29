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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import com.salesforce.scoot.HBaseSchema;
import com.salesforce.scoot.HBaseSchemaAttribute;
import com.salesforce.scoot.ScootException;

/**
 * Parsers take schema that is expressed somewhere (e.g. a file or a live cluster) and parse out the meaningful
 * schema information into an HBaseSchema object.
 * 
 * Implementors are expected to create a single-arg constructor that takes a String, used to identify
 * the resource to be parsed. In the case of a file, it would normally be the file name, but other uses
 * (such as scripting a live cluster) can also take a pointer to something else (like a zookeeper quorum).
 *
 */
public abstract class HBaseSchemaParser {
  
  public abstract void setResourceToParse(String resourceName);

  /**
   * Parses the schema from the representation defined by the subclass, and returns a schema object
   */
  public abstract HBaseSchema parse();


  /** 
   * Interrogate the table object and give any un-set attributes their default values explicitly. 
   * This is required because they'll get them anyway when the table is applied, and we need to compare 
   * them with other objects.
   */
   protected void applyMissingTableDefaults(HTableDescriptor t) {
     for (HBaseSchemaAttribute a : HBaseSchemaAttribute.values()){
       if (HTableDescriptor.class.equals(a.appliesToObjectType) 
           && t.getValue(a.name) == null 
           && a.defaultValue != null) {
         t.setValue(a.name, a.defaultValue);
       }
     }
   }

  /** 
  * Interrogate the column family object and give any un-set attributes their default values explicitly. 
  * This is required because they'll get them anyway when the table is applied, and we need to compare 
  * them with other objects.
  */
   protected void applyMissingColumnFamilyDefaults(HColumnDescriptor cf) {
    for (HBaseSchemaAttribute a : HBaseSchemaAttribute.values()){
      if (HColumnDescriptor.class.equals(a.appliesToObjectType) 
          && cf.getValue(a.name) == null 
          && a.defaultValue != null) {
        cf.setValue(a.name, a.defaultValue);
      }
    }
  }
   
   /**
    * Sanity check for table definition
    */
   protected void validateTableDefinition(HTableDescriptor table) {
     StringBuilder errors = new StringBuilder();
     if (table != null) {
       if (table.getFamilies().size() <= 0) {
         errors.append("Table definition should have at least one correctly defined column family \n");
       }
       if (table.getMaxFileSize() <= 0) {
         errors.append("Table definition has incorrect max file size. Input value is, "
             + table.getMaxFileSize() + ", this should be greater than 0 \n");
       }
       if (table.getMemStoreFlushSize() <= 0) {
         errors.append("Table definition has incorrect memstore flush size. Input value is, "
             + table.getMemStoreFlushSize()
             + ", this should be greater than 0 \n");
       }
     }
     if (errors.length() > 0) {
       throw new ScootException("Errors in defining table: " + errors.toString());
     }
   }
   
   /**
    * Sanity check for column family definition
    */
   protected void validateColumnFamily(HColumnDescriptor columnFamily) {
     StringBuilder errors = new StringBuilder();
     if (columnFamily != null) {
       if (columnFamily.getName() == null) {
         errors.append("Invalid column family name. Name should not be null \n");
       }
       if (columnFamily.getBlocksize() <= 0) {
         errors.append("Invalid block size in column family. Input value is, "
             + columnFamily.getBlocksize()
             + ", this should be greater than 0 \n");
       }
       if (columnFamily.getScope() < 0 || columnFamily.getScope() > 1) {
         errors.append("Invalid replication scope in column family. Input value is, "
             + columnFamily.getScope() + ", this should 0 or 1\n");
       }
       if (columnFamily.getMaxVersions() <= 0) {
         errors.append("Invalid Max versions to keep in column family. Input value is, "
             + columnFamily.getMaxVersions()
             + ", this should be greater than 0 \n");
       }
       if (columnFamily.getTimeToLive() <= 0) {
         errors.append("Invalid time to live in column family. Input value is, "
             + columnFamily.getTimeToLive()
             + ", this should be greater than 0 \n");
       }
     }
     if (errors.length() > 0) {
       throw new ScootException("Errors in configuring column family: " + errors.toString());
     } 
   }

}