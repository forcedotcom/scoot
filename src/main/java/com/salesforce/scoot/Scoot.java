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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;

import com.google.common.base.Preconditions;
import com.salesforce.scoot.parser.HBaseClusterParser;
import com.salesforce.scoot.parser.HBaseSchemaParser;
import com.salesforce.scoot.parser.HBaseScootXMLParser;
import com.salesforce.scoot.scripter.HBaseRubySchemaPatchScripter;

/**
 * Loads, diffs and scripts HBase schemas.
 *
 */
public class Scoot {
  
  private static final Options options = new Options();
  static {
    options.addOption("f", "from", true, "The schema you want to start with.");
    options.addOption("fp", "from-parser", true, "The parser to use for the 'from' schema. If not supplied, the tool will attempt to auto-detect it.");
    options.addOption("t", "to", true, "The schema you want to end up with.");
    options.addOption("tp", "to-parser", true, "The parser to use for the 'to' schema. If not supplied, the tool will attempt to auto-detect it.");
    options.addOption("o", "output", true, "The name of the file to output.");
    options.addOption("h", "help", true, "Get help on using this utility.");
  }
  
  private final String fromSchemaName;
  private final String fromSchemaParser;
  private final String toSchemaName;
  private final String toSchemaParser;
  private final String outputFileName;
  private final boolean helpMode;
  
  /**
   * Create an instance of scoot with the supplied args
   * @param command line args; see options or use the "-h" option for details.
   */
  public Scoot(String[] args){
    try {
      CommandLineParser parser = new PosixParser();
      CommandLine command = parser.parse(options, args);
      
      // are they asking for help?
      if (command.hasOption("h") || (args == null || args.length == 0)) {
        helpMode = true;
      } else {
        helpMode = false;
      }

      // figure out the "from" schema & type
      if (command.hasOption("f")) {
        fromSchemaName = command.getOptionValue("f");
      } else if (command.getArgs().length > 0) {
        fromSchemaName = command.getArgs()[0];
      } else {
        fromSchemaName = null;
      }
      if (command.hasOption("fp")) {
        fromSchemaParser = command.getOptionValue("fp");
      } else {
        fromSchemaParser = null;
      }
      
      // figure out the "to" schema & type
      if (command.hasOption("t")) {
        toSchemaName = command.getOptionValue("t");
      } else if (command.getArgs().length > 1) {
        toSchemaName = command.getArgs()[1];
      } else {
        toSchemaName = null;
      }
      if (command.hasOption("tp")) {
        toSchemaParser = command.getOptionValue("tp");
      } else {
        toSchemaParser = null;
      }
      
      // TODO: if you supplied only one schema pointer as an unflagged arg, it should be considered a "to" and the from should be empty
      
      if (command.hasOption("o")){
        outputFileName = command.getOptionValue("o");
      } else {
        outputFileName = null;
      }

    } catch (ParseException e) {
      throw new ScootException("Error during initialization: ", e);
    }

  }
  
  /**
   * Can be run from a command line
   */
  public static void main(String[] args) {
    new Scoot(args).run();
  }

  /**
   * Using the options supplied at construction time, do any required work.
   */
  public void run() {

    if (helpMode){
      HelpFormatter hf = new HelpFormatter();
      hf.printHelp("scoot", options);
      return;
    }

    Preconditions.checkNotNull(fromSchemaName, "Missing 'from' schema argument.");
    
    HBaseSchema fromSchema = parseSchema(fromSchemaName, fromSchemaParser == null ? getDefaultParser(fromSchemaName) : fromSchemaParser);
    HBaseSchema toSchema = parseSchema(toSchemaName, toSchemaParser == null ? getDefaultParser(toSchemaName) : toSchemaParser);
    
    // if there's no "to" schema, use an empty one (i.e. script this as a create operation)
    if (toSchema == null) toSchema = new HBaseSchema();
    HBaseSchemaDiff diff = new HBaseSchemaDiff(fromSchema, toSchema);
    String script = new HBaseRubySchemaPatchScripter(diff).generateScript();
    writeFile(outputFileName, script);
  }
  
  /**
   * Detect the schema type based on the name and / or supplied parser, and return a parsed schema object.
   * @param schemaName The name of the resource you're pulling schema from (the format of which depends on which parser you're using)
   * @param schemaParser Fully qualified class name of the parser to use
   */
  private HBaseSchema parseSchema(String schemaName, String schemaParser) {
    if (schemaName == null || schemaParser == null) return null;
    try {
      HBaseSchemaParser parser = (HBaseSchemaParser)Class.forName(schemaParser).newInstance();
      parser.setResourceToParse(schemaName);
      return parser.parse();
    } catch (Exception e) {
      throw new ScootException("Unable to instantiate supplied parser: " + schemaParser);
    } 
  }

  /**
   * For xml files, default is the scoot xml parser; for anything else, assume it's a live cluster.
   * TODO: this should probably be pluggable using an implementation supplied by injected parser classes.
   */
  private String getDefaultParser(String schemaName) {
    if (schemaName.endsWith(".xml")) {
      return HBaseScootXMLParser.class.getName();
    } else {
      return HBaseClusterParser.class.getName();
    }
  }

  /**
   * First write the file to a temp location, then move it to the desired location
   */
  private void writeFile(String outputFileName, String script) {
    File destination = new File(outputFileName);
    try{
      File tmp = File.createTempFile("scoot_output_file_" + System.currentTimeMillis(), null);
      FileWriter fstream = new FileWriter(tmp, false);
      BufferedWriter out = new BufferedWriter(fstream);
      out.write(script.toString());
      out.close();

      // if successful, move to real location
      try {
        FileUtils.moveFile(tmp, destination);
      } catch (IOException e) {
          throw new ScootException("Could not move temporary file " + tmp.getAbsolutePath() + " to " + destination + ": " + e.getMessage());
      }
    }catch (Exception x){ //Catch exception if any
      throw new ScootException("Error writing output script file: " + x.getMessage(), x);
    }
  }

  
}
