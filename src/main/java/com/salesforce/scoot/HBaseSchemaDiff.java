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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Allows iterating over the changes between any two schemas. Specifically, the first schema 
 * is the "from" one (i.e. the one you started with) and the second is the "to" schema (the 
 * one you want to end up with after you're done).
 * 
 * Typically, you'd get the "to schema" from a declarative file you wrote by hand, or from 
 * an existing cluster whose schema you want to replicate somewhere else; and you'd get the
 * "from schema" from pointing this utility at a live cluster you want to modify, or pass in 
 * an empty schema to create something from scratch.
 */
public class HBaseSchemaDiff {

  private final HBaseSchema fromSchema;
  private final HBaseSchema toSchema;

  /**
   * Construct the class with a from and to schema.
   */
  public HBaseSchemaDiff(HBaseSchema fromSchema, HBaseSchema toSchema){
    this.fromSchema = fromSchema;
    this.toSchema = toSchema;
    analyze();
  }

  /**
   * Tables in the schema diff are classified in the manner in which they change.
   * IGNORE isn't really a change, but it's one of the things that a diff can report.
   */
  public enum ChangeType {
    ALTER,
    CREATE,
    DROP,
    IGNORE,
  }
  
  /**
   * Simple struct representing a change to a single property of an object in a schema.  
   */
  public static class PropertyChange{
    public String schemaObjectName;
    public String key;
    public String oldValue;
    public String newValue;
    PropertyChange(){}
    PropertyChange(String schemaObjectName, String key) { this.schemaObjectName = schemaObjectName; this.key = key;}
    @Override public String toString(){ return schemaObjectName + ":" + key + ((oldValue != null || newValue != null) ? ":" + oldValue + "->" + newValue : "") + ";"; }
  }

  /**
   * Representation of a schema object that is changing in this diff, with reference to the old and new version
   * of the object and the nature of the change, as well as a list of specific property changes if applicable.
   */
  public class HBaseSchemaChange {
    public String tableName;
    public HTableDescriptor oldTable;
    public HTableDescriptor newTable;
    public ChangeType type;
    public List<PropertyChange> propertyChanges = new ArrayList<PropertyChange>();
  }
  
  /**
   * Helper class to collect a set of changes using a simple method call for each addition.
   */
  public class HBaseSchemaChangeList {
    private final List<HBaseSchemaChange> changes = new ArrayList<HBaseSchemaChange>();
    public void create(HTableDescriptor newTable){
      HBaseSchemaChange change = new HBaseSchemaChange();
      change.tableName = newTable.getNameAsString();
      change.type = ChangeType.CREATE;
      change.newTable = newTable;
      change.oldTable = null;
      changes.add(change);
    }
    public void drop(HTableDescriptor oldTable){
      HBaseSchemaChange change = new HBaseSchemaChange();
      change.tableName = oldTable.getNameAsString();
      change.type = ChangeType.DROP;
      change.newTable = null;
      change.oldTable = oldTable;
      changes.add(change);
    }
    public void alter(HTableDescriptor oldTable, HTableDescriptor newTable, List<PropertyChange> propertyChanges){
      HBaseSchemaChange change = new HBaseSchemaChange();
      change.tableName = oldTable.getNameAsString();
      change.type = ChangeType.ALTER;
      change.newTable = newTable;
      change.oldTable = oldTable;
      change.propertyChanges.addAll(propertyChanges);
      changes.add(change);
    }
    public void ignore(HTableDescriptor sameTable){
      HBaseSchemaChange change = new HBaseSchemaChange();
      change.tableName = sameTable.getNameAsString();
      change.type = ChangeType.IGNORE;
      change.newTable = sameTable;
      change.oldTable = sameTable;
      changes.add(change);
    }
  }
  
  private final HBaseSchemaChangeList changeList = new HBaseSchemaChangeList();
  private final Map<ChangeType, List<HBaseSchemaChange>> changesByType = new HashMap<ChangeType, List<HBaseSchemaChange>>();

  /**
   * Make a single pass through the input schemas to detect and organize the changes by type
   */
  private void analyze() {
    
    // reorganize the tables in the two schemas by name
    Map<String, HTableDescriptor> oldTablesByName = getTableMap(fromSchema);
    Map<String, HTableDescriptor> newTablesByName = getTableMap(toSchema);
    Set<String> allTableNames = new HashSet<String>();
    allTableNames.addAll(oldTablesByName.keySet());
    allTableNames.addAll(newTablesByName.keySet());
    
    // Diff the objects
    for (String tableName : allTableNames){
      HTableDescriptor oldTable = oldTablesByName.get(tableName);
      HTableDescriptor newTable = newTablesByName.get(tableName);
      
      // If the object isn't found in old, but is in new, CREATE
      if (oldTable == null && newTable != null){
        changeList.create(newTable);
      }
      // if the object isn't found in new, but is in old, DROP
      else if (oldTable != null && newTable == null){
        changeList.drop(oldTable);
      }
      // otherwise, it's ALTER or IGNORE
      else if (oldTable != null && newTable != null) {
        List<PropertyChange> propertyChanges = getTableModifications(oldTable, newTable);
        if (! propertyChanges.isEmpty()){
          // if it was modified, it's ALTER
          changeList.alter(oldTable, newTable, propertyChanges);
        } else {
          // if it was not modified, it's IGNORE
          changeList.ignore(oldTable);
        }
      }
    }
    
    // Organize the resulting changes into a map by type, for convenience
    for (ChangeType c : ChangeType.values()) {
      changesByType.put(c, new ArrayList<HBaseSchemaChange>());
    }
    for (HBaseSchemaChange c : changeList.changes){
      changesByType.get(c.type).add(c);
    }
    
  }
  
  /**
   * Create the modification data structure from two tables that both exist. Property changes include
   * changes to the properties of the table's attributes, addition or removal of column families, and
   * changes to the properties of column families.
   */
  private List<PropertyChange> getTableModifications(HTableDescriptor oldTable, HTableDescriptor newTable) {
    List<PropertyChange> propertyChanges = new ArrayList<PropertyChange>();
    
    // check the table properties
    propertyChanges.addAll(getPropertyChanges(newTable.getNameAsString(), oldTable.getValues(), newTable.getValues()));
    
    // check the column families and their properties.
    Map<String,HColumnDescriptor> oldColumnFamilies = getColumnFamilyMap(oldTable.getFamilies());
    Map<String,HColumnDescriptor> newColumnFamilies = getColumnFamilyMap(newTable.getFamilies());
    
    // some are added (new name that didn't previously exist)
    Set<String> addedColumnFamilies = new HashSet<String>(newColumnFamilies.keySet());
    addedColumnFamilies.removeAll(oldColumnFamilies.keySet());
    for (String addedColumnFamily : addedColumnFamilies) {
      propertyChanges.add(new PropertyChange(newTable.getNameAsString(), "Added column family " + addedColumnFamily));
    }

    // some are removed (old name no longer exists)
    Set<String> removedColumnFamilies = new HashSet<String>(oldColumnFamilies.keySet());
    removedColumnFamilies.removeAll(newColumnFamilies.keySet());
    for (String removedColumnFamily : removedColumnFamilies) {
      propertyChanges.add(new PropertyChange(oldTable.getNameAsString(), "Removed column family " + removedColumnFamily));
    }

    // some are altered
    for (Entry<String, HColumnDescriptor> e : oldColumnFamilies.entrySet()) {
      HColumnDescriptor oldColumnFamily = e.getValue();
      HColumnDescriptor newColumnFamily = newColumnFamilies.get(e.getKey());
      if (newColumnFamily != null && !oldColumnFamily.equals(newColumnFamily)) {
        // get the individual property changes, so we can show them as well
        propertyChanges.addAll(getPropertyChanges(newTable.getNameAsString() + ":" + newColumnFamily.getNameAsString(), oldColumnFamily.getValues(), newColumnFamily.getValues()));
      }
    }

    return propertyChanges;
  }


  /**
   * Compares two maps and returns a flat list of changes (key, old value, new value), accounting for adds & removes
   */
  private List<PropertyChange> getPropertyChanges(String schemaObjectName, Map<ImmutableBytesWritable,ImmutableBytesWritable> oldValues, Map<ImmutableBytesWritable,ImmutableBytesWritable> newValues){
    List<PropertyChange> propertyChanges = new ArrayList<PropertyChange>();
    // this finds properties that have been modified or removed
    for (Entry<ImmutableBytesWritable,ImmutableBytesWritable> e : oldValues.entrySet()){
      if (! e.getValue().equals(newValues.get(e.getKey()))){
        PropertyChange p = new PropertyChange(schemaObjectName, Bytes.toString(e.getKey().get()));
        p.oldValue = Bytes.toString(e.getValue().get());
        p.newValue = newValues.containsKey(e.getKey()) ? Bytes.toString(newValues.get(e.getKey()).get()) : null;
        propertyChanges.add(p);
      }
    }
    // this finds properties that have been added
    for (Entry<ImmutableBytesWritable,ImmutableBytesWritable> e : newValues.entrySet()){
      if (! oldValues.containsKey(e.getKey())){
        PropertyChange p = new PropertyChange(schemaObjectName, Bytes.toString(e.getKey().get()));
        p.newValue = newValues.containsKey(e.getKey()) ? Bytes.toString(newValues.get(e.getKey()).get()) : null;
        propertyChanges.add(p);
      }
    }
    return propertyChanges; 
  }

  /**
   * Convert the given collection of tables into a name/object map, throwing an exception if there are duplicates
   * @param the schema out of which you want to pull tables into the map
   */
  private Map<String, HTableDescriptor> getTableMap(HBaseSchema schema) {
    Map<String, HTableDescriptor> result = new HashMap<String, HTableDescriptor>();
    for (HTableDescriptor t : schema.getTables()) {
      if (result.containsKey(t.getNameAsString())) {
        throw new ScootException("Schema contains duplicate tables:" + t.getNameAsString());
      }
      result.put(t.getNameAsString(), t);
    }
    return result;
  }

  /**
   * Convert a collection of column families to a name/object map, throwing an exception if there are duplicates
   */
  private Map<String, HColumnDescriptor> getColumnFamilyMap(Collection<HColumnDescriptor> families) {
    Map<String, HColumnDescriptor> result = new HashMap<String, HColumnDescriptor>();
    for (HColumnDescriptor cf : families) {
      if (result.containsKey(cf.getNameAsString())) {
        throw new ScootException("Schema contains duplicate column families:" + cf.getNameAsString());
      }
      result.put(cf.getNameAsString(), cf);
    }
    return result;
  }
  
  /**
   * Get the list of tables that are part of this diff
   * @return an umodifiable representation of the list
   */
  public List<HBaseSchemaChange> getTableChanges(){
    return Collections.unmodifiableList(this.changeList.changes);
  }
  
  /**
   * Get the list of tables that are part of this diff, but only for a 
   * specific change type.
   * @return an umodifiable representation of the list
   */
  public List<HBaseSchemaChange> getTableChangesByType(ChangeType type){
    return Collections.unmodifiableList(changesByType.get(type));
  }

  /**
   * Get the list of changed tables, arranged into a map by change type.
   * @return an umodifiable representation of the map
   */

  public Map<ChangeType, List<HBaseSchemaChange>> getTableChangesByType(){
    return Collections.unmodifiableMap(changesByType);
  }

}
