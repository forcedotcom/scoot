# HBase Schema Loader #

While HBase is technically "NoSQL" it still requires a schema in the form of HColumnDescriptors. However, defining, storing and updating schemas in HBase is a huge pain and requires a lot of internal knowledge of how HBase works. scoot (short for "Schema Boot") makes it easy and painless to define, deploy and work within schemas in HBase.

In scoot, schemas are defined as XML - everything from the column names to compression type. The XML files can then be loaded and compared with existing clusters (or other XML files) to produce "upgrade scripts"--ruby scripts you can run against a cluster in one state to bring it up to another state.

## Building ##

Scoot is a fully mavenized project. That means you can build simply by doing:

```
 $ mvn package
```

which will build, test and package scoot and put the resulting jar in the generated target/ directory.

You can then create a simple shell executor for running the loader using app assembler:

```
 $ mvn appassembler:assemble
```

This allows you to run easily scoot, like so:

```
 $ ./target/appassembler/bin/scoot {args}
```

The test package relies on a running local HBase; if you don't have that, you might need to build with the following:

```
 $ mvn package -DskipTests
```


For more on using maven, see: <a href="http://maven.apache.org">Apache Maven</a>

## Requirements ##

* Java 1.6.0_21
* Maven 2.0.x

## Roadmap ##

 * support more kinds of schema files (such as classic apache configuration xml style files)
 * allow generating XML as output (so the tool can also be used to document existing clusters' schema easily) 
 * more examples
