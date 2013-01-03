###############################################################################
# HBase Schema Update Script
#
# Summary:
#
#  * Create 0 tables.
#
#  * Alter 1 table:
#       minimal
#       property change: minimal:fullSchema:<table isReadOnly="false" maxFileSizeMB="10240" memStoreFlushSizeMB="128" name="minimal" useDeferredLogFlush="false"><columnFamilies><columnFamily blockCache="true" blockSizeKB="64" bloomFilter="NONE" inMemory="false" maxVersions="3" name="minimalColumnFamily1" replicationScope="0" timeToLiveMS="2147483647"></columnFamily></columnFamilies></table>-><table name="minimal"><columnFamilies><columnFamily name="minimalColumnFamily1"></columnFamily></columnFamilies></table>;
#
#  * Drop 0 tables.
#
#  * Ignore 0 tables.
###############################################################################

###############################################################################
# Initialization
###############################################################################
include Java
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes

conf = HBaseConfiguration.new
admin = HBaseAdmin.new(conf)
preErrors = Array.new
preWarnings = Array.new
postErrors = Array.new

###############################################################################
# Utility methods
###############################################################################

def compare(errs, obj, action, attr, val)
    if (obj.getValue(attr).to_s != val)
        errs << "Object '#{obj.getNameAsString()}', which is targeted for #{action} by this script, should have had a value of \"#{val}\" for #{attr}, but it was \"#{obj.getValue(attr)}\" instead.\n"
    end
end

###############################################################################
# Pre Validation
#
# This step makes sure that the existing schema on the cluster matches what you
# think should be there. It will emit warnings for problems that won't make the
# script fail; it will emit errors and exit if it encounters any problems that
# will make the script fail.
###############################################################################
# Table 'minimal' should exist
tablename = "minimal"
if !admin.tableExists(tablename)
    preErrors << "Table '#{tablename}' should exist, but it does not.\n"
end

# Table 'minimal' will error if it doesn't match the expected definition.
if admin.tableExists(tablename)
    table = admin.getTableDescriptor(tablename.bytes.to_a)
    compare(preErrors, table, "alter", "DEFERRED_LOG_FLUSH", "false")
    compare(preErrors, table, "alter", "IS_META", "false")
    compare(preErrors, table, "alter", "IS_ROOT", "false")
    compare(preErrors, table, "alter", "MAX_FILESIZE", "10737418240")
    compare(preErrors, table, "alter", "MEMSTORE_FLUSHSIZE", "134217728")
    compare(preErrors, table, "alter", "READONLY", "false")
    compare(preErrors, table, "alter", "fullSchema", "<table isReadOnly=\"false\" maxFileSizeMB=\"10240\" memStoreFlushSizeMB=\"128\" name=\"minimal\" useDeferredLogFlush=\"false\"><columnFamilies><columnFamily blockCache=\"true\" blockSizeKB=\"64\" bloomFilter=\"NONE\" inMemory=\"false\" maxVersions=\"3\" name=\"minimalColumnFamily1\" replicationScope=\"0\" timeToLiveMS=\"2147483647\"></columnFamily></columnFamilies></table>")
    # Column family: minimalColumnFamily1
    cfname = "minimalColumnFamily1"
    cf = table.getFamily(cfname.bytes.to_a)
    compare(preErrors, cf, "alter", "BLOCKCACHE", "true")
    compare(preErrors, cf, "alter", "BLOCKSIZE", "65536")
    compare(preErrors, cf, "alter", "BLOOMFILTER", "NONE")
    compare(preErrors, cf, "alter", "COMPRESSION", "NONE")
    compare(preErrors, cf, "alter", "DATA_BLOCK_ENCODING", "NONE")
    compare(preErrors, cf, "alter", "ENCODE_ON_DISK", "true")
    compare(preErrors, cf, "alter", "IN_MEMORY", "false")
    compare(preErrors, cf, "alter", "KEEP_DELETED_CELLS", "false")
    compare(preErrors, cf, "alter", "MIN_VERSIONS", "0")
    compare(preErrors, cf, "alter", "REPLICATION_SCOPE", "0")
    compare(preErrors, cf, "alter", "TTL", "2147483647")
    compare(preErrors, cf, "alter", "VERSIONS", "3")
end


# If any pre-validations had errors, report them and exit the script.
if (preErrors.length > 0)
    puts "There were #{preErrors.length} error(s) and #{preWarnings.length} warning(s) during table pre-validation:"
    print "#{preErrors.collect{|msg| "Error: " + msg}}"
    print "#{preWarnings.collect{|msg| "Warning: " + msg}}"
    raise
    exit
elsif (preWarnings.length > 0)
    puts "Pre-validations successful with #{preWarnings.length} warnings:"
    print "#{preWarnings.collect{|msg| "Warning: " + msg}}"
else
    puts "Pre-validations successful."
end

###############################################################################
# Modifications
#
# This step actually modifies the schema on the cluster.
###############################################################################

# Modify table: minimal
tablename = "minimal"
table = admin.getTableDescriptor(tablename.bytes.to_a)
table.setValue("DEFERRED_LOG_FLUSH", "false")
table.setValue("IS_META", "false")
table.setValue("IS_ROOT", "false")
table.setValue("MAX_FILESIZE", "10737418240")
table.setValue("MEMSTORE_FLUSHSIZE", "134217728")
table.setValue("READONLY", "false")
table.setValue("fullSchema", "<table name=\"minimal\"><columnFamilies><columnFamily name=\"minimalColumnFamily1\"></columnFamily></columnFamilies></table>")
cf = HColumnDescriptor.new("minimalColumnFamily1")
cf.setValue("BLOCKCACHE", "true")
cf.setValue("BLOCKSIZE", "65536")
cf.setValue("BLOOMFILTER", "NONE")
cf.setValue("COMPRESSION", "NONE")
cf.setValue("DATA_BLOCK_ENCODING", "NONE")
cf.setValue("ENCODE_ON_DISK", "true")
cf.setValue("IN_MEMORY", "false")
cf.setValue("KEEP_DELETED_CELLS", "false")
cf.setValue("MIN_VERSIONS", "0")
cf.setValue("REPLICATION_SCOPE", "0")
cf.setValue("TTL", "2147483647")
cf.setValue("VERSIONS", "3")
table.addFamily(cf)
puts "Disabling table '#{tablename}' prior to modification ..."
admin.disableTable(tablename)
puts "Modifying table '#{tablename}' ..."
admin.modifyTable(tablename.bytes.to_a, table)
puts "Enabling table '#{tablename}' after modification ..."
admin.enableTable(tablename)
puts "Modified table '#{tablename}"

puts "Table creations & modifications successful."

###############################################################################
# Post Validation
#
# This step ensures that changes were successful, and that the resulting schema
# on the cluster matches what you want to be there.
###############################################################################
# Table 'minimal' should exist
tablename = "minimal"
if !admin.tableExists(tablename)
    preErrors << "Table '#{tablename}' should exist, but it does not.\n"
end

# Table 'minimal' will error if it doesn't match the expected definition.
if admin.tableExists(tablename)
    table = admin.getTableDescriptor(tablename.bytes.to_a)
    compare(preErrors, table, "alter", "DEFERRED_LOG_FLUSH", "false")
    compare(preErrors, table, "alter", "IS_META", "false")
    compare(preErrors, table, "alter", "IS_ROOT", "false")
    compare(preErrors, table, "alter", "MAX_FILESIZE", "10737418240")
    compare(preErrors, table, "alter", "MEMSTORE_FLUSHSIZE", "134217728")
    compare(preErrors, table, "alter", "READONLY", "false")
    compare(preErrors, table, "alter", "fullSchema", "<table name=\"minimal\"><columnFamilies><columnFamily name=\"minimalColumnFamily1\"></columnFamily></columnFamilies></table>")
    # Column family: minimalColumnFamily1
    cfname = "minimalColumnFamily1"
    cf = table.getFamily(cfname.bytes.to_a)
    compare(preErrors, cf, "alter", "BLOCKCACHE", "true")
    compare(preErrors, cf, "alter", "BLOCKSIZE", "65536")
    compare(preErrors, cf, "alter", "BLOOMFILTER", "NONE")
    compare(preErrors, cf, "alter", "COMPRESSION", "NONE")
    compare(preErrors, cf, "alter", "DATA_BLOCK_ENCODING", "NONE")
    compare(preErrors, cf, "alter", "ENCODE_ON_DISK", "true")
    compare(preErrors, cf, "alter", "IN_MEMORY", "false")
    compare(preErrors, cf, "alter", "KEEP_DELETED_CELLS", "false")
    compare(preErrors, cf, "alter", "MIN_VERSIONS", "0")
    compare(preErrors, cf, "alter", "REPLICATION_SCOPE", "0")
    compare(preErrors, cf, "alter", "TTL", "2147483647")
    compare(preErrors, cf, "alter", "VERSIONS", "3")
end

puts "Post-validation successful."

puts "Script complete. Share and enjoy."
exit
