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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.google.common.base.Preconditions;
import com.salesforce.scoot.HBaseSchema;
import com.salesforce.scoot.ScootException;

/**
 * "Parses" a schema representation out of a running cluster
 *
 */
public class HBaseClusterParser extends HBaseSchemaParser {

  private Configuration config;

  public void setResourceToParse(String zookeeperQuorum){
    this.config = createConfig(zookeeperQuorum);
  }


  private static Configuration createConfig(String zookeeperQuorum) {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
    return configuration;
  }

  /**
   * Connect to the cluster supplied in the constructor and extract a representation
   * of its current schema
   */
  @Override
  public HBaseSchema parse() {
    Preconditions.checkNotNull(config, "Configuration with zookeeper quorum must be set before parsing.");
    HBaseSchema s = new HBaseSchema();
    try {
      HBaseAdmin admin = new HBaseAdmin(config);
      for (HTableDescriptor t : admin.listTables()){
        s.addTable(t);
      }
    } catch (Exception x) {
      throw new ScootException("Unable to connect and get current HBase schema information: " + x.getMessage(), x);
    }
    return s;
  }

}
