###############################################################################
# HBase Schema Update Script
#
# Summary:
#
#  * Create 1 table:
#       createMe
#
#  * Alter 0 tables.
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
# Table 'createMe' should not exist
tablename = "createMe"
if admin.tableExists(tablename)
    preErrors << "Table '#{tablename}' should not already exist, but it does.\n"
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

# Create Table: createMe
tablename = "createMe"
table = HTableDescriptor.new(tablename)
#set table properties
table.setValue("DEFERRED_LOG_FLUSH", "false")
table.setValue("IS_META", "false")
table.setValue("IS_ROOT", "false")
table.setValue("MAX_FILESIZE", "268435456")
table.setValue("MEMSTORE_FLUSHSIZE", "67108864")
table.setValue("NUMREGIONS", "12")
table.setValue("OWNER", "ivarley")
table.setValue("READONLY", "false")
table.setValue("fullSchema", "<table isReadOnly=\"false\" maxFileSizeMB=\"256\" memStoreFlushSizeMB=\"64\" name=\"createMe\" numRegionsToPreSplitOnCreation=\"12\" owner=\"ivarley\" useDeferredLogFlush=\"false\"><key><keyPart inverted=\"false\" length=\"15\" name=\"createMeKeyPart1\" type=\"String\"/><keyPart inverted=\"true\" length=\"15\" name=\"createMeKeyPart2\" type=\"Timestamp\"/></key><columnFamilies><columnFamily blockCache=\"true\" blockSizeKB=\"64\" bloomFilter=\"NONE\" inMemory=\"false\" maxVersions=\"3\" name=\"createMeColumnFamily1\" replicationScope=\"0\" timeToLiveMS=\"2147483647\"><column name=\"createMeColumn1\" type=\"String\"/><column name=\"createMeColumn2\" type=\"Timestamp\"/><column name=\"createMeColumn3\" type=\"Byte\"/></columnFamily></columnFamilies></table>")
cf = HColumnDescriptor.new("createMeColumnFamily1")
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
puts "Creating table '#{tablename}' ... "
admin.createTable(table, Bytes.toBytes("\x00"), Bytes.toBytes("\xFF"), 12)
puts "Created table '#{tablename}'"

puts "Table creations & modifications successful."

###############################################################################
# Post Validation
#
# This step ensures that changes were successful, and that the resulting schema
# on the cluster matches what you want to be there.
###############################################################################
# Table 'createMe' should exist
tablename = "createMe"
if !admin.tableExists(tablename)
    preErrors << "Table '#{tablename}' should exist, but it does not.\n"
end

# Table 'createMe' will error if it doesn't match the expected definition.
if admin.tableExists(tablename)
    table = admin.getTableDescriptor(tablename.bytes.to_a)
    compare(preErrors, table, "create", "DEFERRED_LOG_FLUSH", "false")
    compare(preErrors, table, "create", "IS_META", "false")
    compare(preErrors, table, "create", "IS_ROOT", "false")
    compare(preErrors, table, "create", "MAX_FILESIZE", "268435456")
    compare(preErrors, table, "create", "MEMSTORE_FLUSHSIZE", "67108864")
    compare(preErrors, table, "create", "NUMREGIONS", "12")
    compare(preErrors, table, "create", "OWNER", "ivarley")
    compare(preErrors, table, "create", "READONLY", "false")
    compare(preErrors, table, "create", "fullSchema", "<table isReadOnly=\"false\" maxFileSizeMB=\"256\" memStoreFlushSizeMB=\"64\" name=\"createMe\" numRegionsToPreSplitOnCreation=\"12\" owner=\"ivarley\" useDeferredLogFlush=\"false\"><key><keyPart inverted=\"false\" length=\"15\" name=\"createMeKeyPart1\" type=\"String\"/><keyPart inverted=\"true\" length=\"15\" name=\"createMeKeyPart2\" type=\"Timestamp\"/></key><columnFamilies><columnFamily blockCache=\"true\" blockSizeKB=\"64\" bloomFilter=\"NONE\" inMemory=\"false\" maxVersions=\"3\" name=\"createMeColumnFamily1\" replicationScope=\"0\" timeToLiveMS=\"2147483647\"><column name=\"createMeColumn1\" type=\"String\"/><column name=\"createMeColumn2\" type=\"Timestamp\"/><column name=\"createMeColumn3\" type=\"Byte\"/></columnFamily></columnFamilies></table>")
    # Column family: createMeColumnFamily1
    cf = HColumnDescriptor.new("createMeColumnFamily1")
    compare(preErrors, cf, "create", "BLOCKCACHE", "true")
    compare(preErrors, cf, "create", "BLOCKSIZE", "65536")
    compare(preErrors, cf, "create", "BLOOMFILTER", "NONE")
    compare(preErrors, cf, "create", "COMPRESSION", "NONE")
    compare(preErrors, cf, "create", "DATA_BLOCK_ENCODING", "NONE")
    compare(preErrors, cf, "create", "ENCODE_ON_DISK", "true")
    compare(preErrors, cf, "create", "IN_MEMORY", "false")
    compare(preErrors, cf, "create", "KEEP_DELETED_CELLS", "false")
    compare(preErrors, cf, "create", "MIN_VERSIONS", "0")
    compare(preErrors, cf, "create", "REPLICATION_SCOPE", "0")
    compare(preErrors, cf, "create", "TTL", "2147483647")
    compare(preErrors, cf, "create", "VERSIONS", "3")
end

puts "Post-validation successful."

puts "Script complete. Share and enjoy."
exit
