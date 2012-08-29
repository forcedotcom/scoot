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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;

/**
 * Represents an officially supported attribute that a schema element in HBase can have. Scoot supports loading
 * other attributes, but it gives special treatment to these in making sure they can be correctly cast to the real
 * type, and allowing the loader to equate missing values and default values.
 */
public enum HBaseSchemaAttribute {

  /* Tables */
  DEFERRED_LOG_FLUSH(HTableDescriptor.DEFERRED_LOG_FLUSH, HTableDescriptor.class, Boolean.class, 
      /* HTableDescriptor.DEFAULT_DEFERRED_LOG_FLUSH */ String.valueOf(false), null),
  IS_META(HTableDescriptor.IS_META, HTableDescriptor.class, Boolean.class, 
      String.valueOf(false), null),
  IS_ROOT(HTableDescriptor.IS_ROOT, HTableDescriptor.class, Boolean.class, 
      String.valueOf(false), null),
  MAX_FILESIZE(HTableDescriptor.MAX_FILESIZE, HTableDescriptor.class, Long.class, 
      String.valueOf(HConstants.DEFAULT_MAX_FILE_SIZE), null),
  MEMSTORE_FLUSHSIZE(HTableDescriptor.MEMSTORE_FLUSHSIZE, HTableDescriptor.class, Long.class, 
      String.valueOf(HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE), null),
  OWNER(HTableDescriptor.OWNER, HTableDescriptor.class, String.class, 
      null, null),
  READONLY(HTableDescriptor.READONLY, HTableDescriptor.class, Boolean.class, 
      String.valueOf(HTableDescriptor.DEFAULT_READONLY), null),
  
  /* Column families */
  BLOCKCACHE(HColumnDescriptor.BLOCKCACHE, HColumnDescriptor.class, Boolean.class, 
      String.valueOf(HColumnDescriptor.DEFAULT_BLOCKCACHE), null),
  BLOCKSIZE(HColumnDescriptor.BLOCKSIZE, HColumnDescriptor.class, Integer.class, 
      String.valueOf(HColumnDescriptor.DEFAULT_BLOCKSIZE), 7),
  BLOOMFILTER(HColumnDescriptor.BLOOMFILTER, HColumnDescriptor.class, StoreFile.BloomType.class, 
      HColumnDescriptor.DEFAULT_BLOOMFILTER, 8),
  COMPRESSION(HColumnDescriptor.COMPRESSION, HColumnDescriptor.class, Compression.Algorithm.class, 
      HColumnDescriptor.DEFAULT_COMPRESSION, 7),
  DATA_BLOCK_ENCODING(HColumnDescriptor.DATA_BLOCK_ENCODING, HColumnDescriptor.class, DataBlockEncoding.class, 
      HColumnDescriptor.DEFAULT_DATA_BLOCK_ENCODING, 9),
  ENCODE_ON_DISK(HColumnDescriptor.ENCODE_ON_DISK, HColumnDescriptor.class, Boolean.class, 
      String.valueOf(HColumnDescriptor.DEFAULT_ENCODE_ON_DISK), 9),
  IN_MEMORY(HConstants.IN_MEMORY, HColumnDescriptor.class, Boolean.class, 
      String.valueOf(HColumnDescriptor.DEFAULT_IN_MEMORY), 9),
  KEEP_DELETED_CELLS(HColumnDescriptor.KEEP_DELETED_CELLS, HColumnDescriptor.class, Boolean.class, 
      String.valueOf(HColumnDescriptor.DEFAULT_KEEP_DELETED), 9),
  MIN_VERSIONS(HColumnDescriptor.MIN_VERSIONS, HColumnDescriptor.class, Integer.class, 
      String.valueOf(HColumnDescriptor.DEFAULT_MIN_VERSIONS), 8),
  REPLICATION_SCOPE(HColumnDescriptor.REPLICATION_SCOPE, HColumnDescriptor.class, Integer.class, 
      String.valueOf(HColumnDescriptor.DEFAULT_REPLICATION_SCOPE), null),
  TTL(HColumnDescriptor.TTL, HColumnDescriptor.class, Integer.class, 
      String.valueOf(HColumnDescriptor.DEFAULT_TTL), null),
  VERSIONS(HConstants.VERSIONS, HColumnDescriptor.class, Integer.class, 
      String.valueOf(HColumnDescriptor.DEFAULT_VERSIONS), null),

  ;
  
  /** The string name of the attribute, usually shown in all caps */
  public final String name;
  /** Which type of schema element supports this attribute: tables or column families */
  public final Class<?> appliesToObjectType;
  /** What's the underlying type of the attribute? Supported are string, integer, boolean, and enums. */
  public final Class<?> type;
  /** What's the default value that this attribute gets if not set by the user? Not all attributes have defaults. */
  public final String defaultValue;
  /** What's the earliest integer version of the schema element that supports this attribute? As defined in the HTableDescriptor and HColumnDescriptor source. 
   *  Versions older than 7 aren't tracked, as this tool doesn't purport to work with anything older than 7. */
  public final Integer minVersion;

  private HBaseSchemaAttribute(String name, Class<?> appliesToObjectType, Class<?> type, String defaultValue, Integer minVersion){
    this.name = name;
    this.appliesToObjectType = appliesToObjectType;
    this.type = type;
    this.defaultValue = defaultValue;
    this.minVersion = minVersion;
  }
  
  public static HBaseSchemaAttribute getFromName(String name){
    for (HBaseSchemaAttribute a : values()){
      if (name.equalsIgnoreCase(a.name)){
        return a;
      }
    }
    return null;
  }
  
  /**
   * See if the supplied string can be successfully parsed as this type, and throw if not
   */
  public void tryParse(String attributeValue) throws SecurityException, NoSuchFieldException, NumberFormatException, IllegalArgumentException {
    if (this.type.equals(String.class)) return;
    if (this.type.equals(Integer.class)) {
      Integer.parseInt(attributeValue);
      return;
    }
    if (this.type.equals(Long.class)) {
      Long.parseLong(attributeValue);
      return;
    }
    if (this.type.equals(Boolean.class)) {
      if (! (attributeValue.equalsIgnoreCase("true") || attributeValue.equalsIgnoreCase("false")))
        throw new IllegalArgumentException("Invalid boolean value: " + attributeValue);
      return;
    }
    else if (this.type.isEnum()) {
      this.type.getField(attributeValue);
      return;
    }
    throw new ScootException("Attempted to parse unknown type: " + this.type);
  }
  
}