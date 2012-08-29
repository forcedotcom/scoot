###############################################################################
# HBase Schema Update Script
#
# Summary:
#
#  * Create 0 tables.
#
#  * Alter 0 tables.
#
#  * Drop 1 table:
#       createMe
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
# Table 'createMe' should exist
tablename = "createMe"
if !admin.tableExists(tablename)
    preErrors << "Table '#{tablename}' should exist, but it does not.\n"
end

# Table 'createMe' will warn if it doesn't match the expected definition.
if admin.tableExists(tablename)
    table = admin.getTableDescriptor(tablename.bytes.to_a)
    compare(preWarnings, table, "drop", "DEFERRED_LOG_FLUSH", "false")
    compare(preWarnings, table, "drop", "IS_META", "false")
    compare(preWarnings, table, "drop", "IS_ROOT", "false")
    compare(preWarnings, table, "drop", "MAX_FILESIZE", "268435456")
    compare(preWarnings, table, "drop", "MEMSTORE_FLUSHSIZE", "67108864")
    compare(preWarnings, table, "drop", "OWNER", "ivarley")
    compare(preWarnings, table, "drop", "READONLY", "false")
    compare(preWarnings, table, "drop", "fullSchema", "<table isReadOnly=\"false\" maxFileSizeMB=\"256\" memStoreFlushSizeMB=\"64\" name=\"createMe\" owner=\"ivarley\" useDeferredLogFlush=\"false\"><key><keyPart inverted=\"false\" length=\"15\" name=\"createMeKeyPart1\" type=\"String\"/><keyPart inverted=\"true\" length=\"15\" name=\"createMeKeyPart2\" type=\"Timestamp\"/></key><columnFamilies><columnFamily blockCache=\"true\" blockSizeKB=\"64\" bloomFilter=\"NONE\" inMemory=\"false\" maxVersions=\"3\" name=\"createMeColumnFamily1\" replicationScope=\"0\" timeToLiveMS=\"2147483647\"><column name=\"createMeColumn1\" type=\"String\"/><column name=\"createMeColumn2\" type=\"Timestamp\"/><column name=\"createMeColumn3\" type=\"Byte\"/></columnFamily></columnFamilies></table>")
    # Column family: createMeColumnFamily1
    cf = HColumnDescriptor.new("createMeColumnFamily1")
    compare(preWarnings, cf, "drop", "BLOCKCACHE", "true")
    compare(preWarnings, cf, "drop", "BLOCKSIZE", "65536")
    compare(preWarnings, cf, "drop", "BLOOMFILTER", "NONE")
    compare(preWarnings, cf, "drop", "COMPRESSION", "NONE")
    compare(preWarnings, cf, "drop", "DATA_BLOCK_ENCODING", "NONE")
    compare(preWarnings, cf, "drop", "ENCODE_ON_DISK", "true")
    compare(preWarnings, cf, "drop", "IN_MEMORY", "false")
    compare(preWarnings, cf, "drop", "KEEP_DELETED_CELLS", "false")
    compare(preWarnings, cf, "drop", "MIN_VERSIONS", "0")
    compare(preWarnings, cf, "drop", "REPLICATION_SCOPE", "0")
    compare(preWarnings, cf, "drop", "TTL", "2147483647")
    compare(preWarnings, cf, "drop", "VERSIONS", "3")
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

# Drop Table: createMe
tablename = "createMe"
table = HTableDescriptor.new(tablename)
if admin.tableExists(tablename)
  if admin.isTableEnabled(tablename)
    puts "Disabling table '#{tablename}' prior to dropping it ..."
    admin.disableTable(tablename)
  end
    puts "Dropping table '#{tablename}' ..."
  admin.deleteTable(tablename)
end
puts "Dropped table '#{tablename}'"

puts "Table creations & modifications successful."

###############################################################################
# Post Validation
#
# This step ensures that changes were successful, and that the resulting schema
# on the cluster matches what you want to be there.
###############################################################################
# Table 'createMe' should not exist
tablename = "createMe"
if admin.tableExists(tablename)
    preErrors << "Table '#{tablename}' should not already exist, but it does.\n"
end

puts "Post-validation successful."

puts "Script complete. Share and enjoy."
exit
