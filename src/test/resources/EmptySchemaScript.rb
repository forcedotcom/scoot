###############################################################################
# HBase Schema Update Script
#
# Summary:
#
#  * Create 0 tables.
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

puts "Table creations & modifications successful."

###############################################################################
# Post Validation
#
# This step ensures that changes were successful, and that the resulting schema
# on the cluster matches what you want to be there.
###############################################################################
puts "Post-validation successful."

puts "Script complete. Share and enjoy."
exit
