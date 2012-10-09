###############################################################################
# HBase Schema Update Script
#
# Summary:
#
#  * Create 1 table:
#       createMe
#
#  * Alter 1 table:
#       alterMe
#       property change: alterMe:MAX_FILESIZE:268435456->269484032;
#       property change: alterMe:MEMSTORE_FLUSHSIZE:67108864->68157440;
#       property change: alterMe:OWNER:ivarley->ivarley2;
#       property change: alterMe:alterMeColumnFamily1:BLOCKSIZE:65536->66560;
#       property change: alterMe:fullSchema:<table isReadOnly="false" maxFileSizeMB="256" memStoreFlushSizeMB="64" name="alterMe" owner="ivarley" useDeferredLogFlush="false"><key><keyPart inverted="false" length="15" name="alterMeKeyPart1" type="String"/><keyPart inverted="true" length="15" name="alterMeKeyPart2" type="Timestamp"/></key><columnFamilies><columnFamily blockCache="true" blockSizeKB="64" bloomFilter="NONE" inMemory="false" maxVersions="3" name="alterMeColumnFamily1" replicationScope="0" timeToLiveMS="2147483647"><column name="alterMeColumn1" type="String"/><column name="alterMeColumn2" type="Timestamp"/><column name="alterMeColumn3" type="Byte"/></columnFamily></columnFamilies></table>-><table isReadOnly="false" maxFileSizeMB="257" memStoreFlushSizeMB="65" name="alterMe" owner="ivarley2" useDeferredLogFlush="false"><key><keyPart inverted="false" length="15" name="alterMeKeyPart1" type="String"/><keyPart inverted="true" length="15" name="alterMeKeyPart2" type="Timestamp"/></key><columnFamilies><columnFamily blockCache="true" blockSizeKB="65" bloomFilter="NONE" inMemory="false" maxVersions="3" name="alterMeColumnFamily1" replicationScope="0" timeToLiveMS="2147483647"><column name="alterMeColumn1" type="String"/><column name="alterMeColumn2" type="Timestamp"/><column name="alterMeColumn3" type="Byte"/></columnFamily></columnFamilies></table>;
#
#  * Drop 1 table:
#       dropMe
#
#  * Ignore 1 table:
#       ignoreMe
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
# Table 'dropMe' should exist
tablename = "dropMe"
if !admin.tableExists(tablename)
    preErrors << "Table '#{tablename}' should exist, but it does not.\n"
end

# Table 'dropMe' will warn if it doesn't match the expected definition.
if admin.tableExists(tablename)
    table = admin.getTableDescriptor(tablename.bytes.to_a)
    compare(preWarnings, table, "drop", "DEFERRED_LOG_FLUSH", "false")
    compare(preWarnings, table, "drop", "IS_META", "false")
    compare(preWarnings, table, "drop", "IS_ROOT", "false")
    compare(preWarnings, table, "drop", "MAX_FILESIZE", "268435456")
    compare(preWarnings, table, "drop", "MEMSTORE_FLUSHSIZE", "67108864")
    compare(preWarnings, table, "drop", "OWNER", "ivarley")
    compare(preWarnings, table, "drop", "READONLY", "false")
    compare(preWarnings, table, "drop", "fullSchema", "<table isReadOnly=\"false\" maxFileSizeMB=\"256\" memStoreFlushSizeMB=\"64\" name=\"dropMe\" owner=\"ivarley\" useDeferredLogFlush=\"false\"><key><keyPart inverted=\"false\" length=\"15\" name=\"dropMeKeyPart1\" type=\"String\"/><keyPart inverted=\"true\" length=\"15\" name=\"dropMeKeyPart2\" type=\"Timestamp\"/></key><columnFamilies><columnFamily blockCache=\"true\" blockSizeKB=\"64\" bloomFilter=\"NONE\" inMemory=\"false\" maxVersions=\"3\" name=\"dropMeColumnFamily1\" replicationScope=\"0\" timeToLiveMS=\"2147483647\"><column name=\"dropMeColumn1\" type=\"String\"/><column name=\"dropMeColumn2\" type=\"Timestamp\"/><column name=\"dropMeColumn3\" type=\"Byte\"/></columnFamily></columnFamilies></table>")
    # Column family: dropMeColumnFamily1
    cf = HColumnDescriptor.new("dropMeColumnFamily1")
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

# Table 'createMe' should not exist
tablename = "createMe"
if admin.tableExists(tablename)
    preErrors << "Table '#{tablename}' should not already exist, but it does.\n"
end

# Table 'alterMe' should exist
tablename = "alterMe"
if !admin.tableExists(tablename)
    preErrors << "Table '#{tablename}' should exist, but it does not.\n"
end

# Table 'alterMe' will error if it doesn't match the expected definition.
if admin.tableExists(tablename)
    table = admin.getTableDescriptor(tablename.bytes.to_a)
    compare(preErrors, table, "alter", "DEFERRED_LOG_FLUSH", "false")
    compare(preErrors, table, "alter", "IS_META", "false")
    compare(preErrors, table, "alter", "IS_ROOT", "false")
    compare(preErrors, table, "alter", "MAX_FILESIZE", "268435456")
    compare(preErrors, table, "alter", "MEMSTORE_FLUSHSIZE", "67108864")
    compare(preErrors, table, "alter", "OWNER", "ivarley")
    compare(preErrors, table, "alter", "READONLY", "false")
    compare(preErrors, table, "alter", "fullSchema", "<table isReadOnly=\"false\" maxFileSizeMB=\"256\" memStoreFlushSizeMB=\"64\" name=\"alterMe\" owner=\"ivarley\" useDeferredLogFlush=\"false\"><key><keyPart inverted=\"false\" length=\"15\" name=\"alterMeKeyPart1\" type=\"String\"/><keyPart inverted=\"true\" length=\"15\" name=\"alterMeKeyPart2\" type=\"Timestamp\"/></key><columnFamilies><columnFamily blockCache=\"true\" blockSizeKB=\"64\" bloomFilter=\"NONE\" inMemory=\"false\" maxVersions=\"3\" name=\"alterMeColumnFamily1\" replicationScope=\"0\" timeToLiveMS=\"2147483647\"><column name=\"alterMeColumn1\" type=\"String\"/><column name=\"alterMeColumn2\" type=\"Timestamp\"/><column name=\"alterMeColumn3\" type=\"Byte\"/></columnFamily></columnFamilies></table>")
    # Column family: alterMeColumnFamily1
    cf = HColumnDescriptor.new("alterMeColumnFamily1")
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

# Drop Table: dropMe
tablename = "dropMe"
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

# Create Table: createMe
tablename = "createMe"
table = HTableDescriptor.new(tablename)
#set table properties
table.setValue("DEFERRED_LOG_FLUSH", "false")
table.setValue("IS_META", "false")
table.setValue("IS_ROOT", "false")
table.setValue("MAX_FILESIZE", "268435456")
table.setValue("MEMSTORE_FLUSHSIZE", "67108864")
table.setValue("OWNER", "ivarley")
table.setValue("READONLY", "false")
table.setValue("fullSchema", "<table isReadOnly=\"false\" maxFileSizeMB=\"256\" memStoreFlushSizeMB=\"64\" name=\"createMe\" owner=\"ivarley\" useDeferredLogFlush=\"false\"><key><keyPart inverted=\"false\" length=\"15\" name=\"createMeKeyPart1\" type=\"String\"/><keyPart inverted=\"true\" length=\"15\" name=\"createMeKeyPart2\" type=\"Timestamp\"/></key><columnFamilies><columnFamily blockCache=\"true\" blockSizeKB=\"64\" bloomFilter=\"NONE\" inMemory=\"false\" maxVersions=\"3\" name=\"createMeColumnFamily1\" replicationScope=\"0\" timeToLiveMS=\"2147483647\"><column name=\"createMeColumn1\" type=\"String\"/><column name=\"createMeColumn2\" type=\"Timestamp\"/><column name=\"createMeColumn3\" type=\"Byte\"/></columnFamily></columnFamilies></table>")
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
admin.createTable(table)
puts "Created table '#{tablename}'"

# Modify table: alterMe
tablename = "alterMe"
table = admin.getTableDescriptor(tablename.bytes.to_a)
table.setValue("DEFERRED_LOG_FLUSH", "false")
table.setValue("IS_META", "false")
table.setValue("IS_ROOT", "false")
table.setValue("MAX_FILESIZE", "269484032")
table.setValue("MEMSTORE_FLUSHSIZE", "68157440")
table.setValue("OWNER", "ivarley2")
table.setValue("READONLY", "false")
table.setValue("fullSchema", "<table isReadOnly=\"false\" maxFileSizeMB=\"257\" memStoreFlushSizeMB=\"65\" name=\"alterMe\" owner=\"ivarley2\" useDeferredLogFlush=\"false\"><key><keyPart inverted=\"false\" length=\"15\" name=\"alterMeKeyPart1\" type=\"String\"/><keyPart inverted=\"true\" length=\"15\" name=\"alterMeKeyPart2\" type=\"Timestamp\"/></key><columnFamilies><columnFamily blockCache=\"true\" blockSizeKB=\"65\" bloomFilter=\"NONE\" inMemory=\"false\" maxVersions=\"3\" name=\"alterMeColumnFamily1\" replicationScope=\"0\" timeToLiveMS=\"2147483647\"><column name=\"alterMeColumn1\" type=\"String\"/><column name=\"alterMeColumn2\" type=\"Timestamp\"/><column name=\"alterMeColumn3\" type=\"Byte\"/></columnFamily></columnFamilies></table>")
cf = HColumnDescriptor.new("alterMeColumnFamily1")
cf.setValue("BLOCKCACHE", "true")
cf.setValue("BLOCKSIZE", "66560")
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
# Table 'dropMe' should not exist
tablename = "dropMe"
if admin.tableExists(tablename)
    preErrors << "Table '#{tablename}' should not already exist, but it does.\n"
end

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
    compare(preErrors, table, "create", "OWNER", "ivarley")
    compare(preErrors, table, "create", "READONLY", "false")
    compare(preErrors, table, "create", "fullSchema", "<table isReadOnly=\"false\" maxFileSizeMB=\"256\" memStoreFlushSizeMB=\"64\" name=\"createMe\" owner=\"ivarley\" useDeferredLogFlush=\"false\"><key><keyPart inverted=\"false\" length=\"15\" name=\"createMeKeyPart1\" type=\"String\"/><keyPart inverted=\"true\" length=\"15\" name=\"createMeKeyPart2\" type=\"Timestamp\"/></key><columnFamilies><columnFamily blockCache=\"true\" blockSizeKB=\"64\" bloomFilter=\"NONE\" inMemory=\"false\" maxVersions=\"3\" name=\"createMeColumnFamily1\" replicationScope=\"0\" timeToLiveMS=\"2147483647\"><column name=\"createMeColumn1\" type=\"String\"/><column name=\"createMeColumn2\" type=\"Timestamp\"/><column name=\"createMeColumn3\" type=\"Byte\"/></columnFamily></columnFamilies></table>")
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

# Table 'alterMe' should exist
tablename = "alterMe"
if !admin.tableExists(tablename)
    preErrors << "Table '#{tablename}' should exist, but it does not.\n"
end

# Table 'alterMe' will error if it doesn't match the expected definition.
if admin.tableExists(tablename)
    table = admin.getTableDescriptor(tablename.bytes.to_a)
    compare(preErrors, table, "alter", "DEFERRED_LOG_FLUSH", "false")
    compare(preErrors, table, "alter", "IS_META", "false")
    compare(preErrors, table, "alter", "IS_ROOT", "false")
    compare(preErrors, table, "alter", "MAX_FILESIZE", "269484032")
    compare(preErrors, table, "alter", "MEMSTORE_FLUSHSIZE", "68157440")
    compare(preErrors, table, "alter", "OWNER", "ivarley2")
    compare(preErrors, table, "alter", "READONLY", "false")
    compare(preErrors, table, "alter", "fullSchema", "<table isReadOnly=\"false\" maxFileSizeMB=\"257\" memStoreFlushSizeMB=\"65\" name=\"alterMe\" owner=\"ivarley2\" useDeferredLogFlush=\"false\"><key><keyPart inverted=\"false\" length=\"15\" name=\"alterMeKeyPart1\" type=\"String\"/><keyPart inverted=\"true\" length=\"15\" name=\"alterMeKeyPart2\" type=\"Timestamp\"/></key><columnFamilies><columnFamily blockCache=\"true\" blockSizeKB=\"65\" bloomFilter=\"NONE\" inMemory=\"false\" maxVersions=\"3\" name=\"alterMeColumnFamily1\" replicationScope=\"0\" timeToLiveMS=\"2147483647\"><column name=\"alterMeColumn1\" type=\"String\"/><column name=\"alterMeColumn2\" type=\"Timestamp\"/><column name=\"alterMeColumn3\" type=\"Byte\"/></columnFamily></columnFamilies></table>")
    # Column family: alterMeColumnFamily1
    cf = HColumnDescriptor.new("alterMeColumnFamily1")
    compare(preErrors, cf, "alter", "BLOCKCACHE", "true")
    compare(preErrors, cf, "alter", "BLOCKSIZE", "66560")
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
