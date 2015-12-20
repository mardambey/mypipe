# mypipe
mypipe latches onto a MySQL server with binary log replication enabled and
allows for the creation of pipes that can consume the replication stream and
act on the data (primarily integrated with Apache Kafka).

# Features
* streams binary logs remotely, emulating a slave
* writes binlog events into Kafka using a generic or specific Avro schema
* supports saving / loading binary log positions in a modular fashion (files, MySQL, or custom Java/Scala code to do so) 
* handles `ALTER TABLE` events and can refresh Avro schema being used
* built in a modular way allowing binlog events to be published into any system, not just Kafka
* can preload an entire MySQL table into Kafka, then resume from binary logs (useful with Kafka compaction, infinite retention, can be used to bootstrap downstream systems with the entire data for a table)
* configurable Kafka topic names based on the database and table
* whitelist / blacklist support for what to process or what not to process in a binary log (based on database and table)
* configurable error handling with the ability to specify custom handlers written in Java or Scala
* Kafka generic console consumer that interfaces with an in memory Avro schema repo for quick and easy data exploration in Kafka

# API
mypipe tries to provide enough information that usually is not part of the
MySQL binary log stream so that the data is meaningful. mypipe requires a
row based binary log format and provides `Insert`, `Update`, and `Delete`
mutations representing changed rows. Each change is related back to it's
table and the API provides metadata like column types, primary key
information (composite, key order), and other such useful information.

Look at `ColumnType.scala` and `Mutation.scala` for more details.

# Producers
Producers receive MySQL binary log events and act on them. They can funnel
down to another data store, send them to some service, or just print them
out to the screen in the case of the stdout producer.

# Pipes
Pipes tie one or more MySQL binary log consumers to a producer. Pipes can be
used to create a system of fan-in data flow from several MySQL servers to
other data sources such as Hadoop, Cassandra, Kafka, or other MySQL servers.
They can also be used to update or flush caches.

# Kafka integration
mypipe's main goal is to replicate a MySQL binlog stream into Apache Kafka.
mypipe supports Avro encoding and can use a schema repository to figure out
how to encode data. Data can either be encoded generically and stored in
Avro maps by type (integers, strings, longs, etc.) or it can encode data
more specifically if the schema repository can return specific schemas. The
latter will allow the table's structure to be reflected in the Avro structure.

## Kafka message format
Binary log events, specifically mutation events (insert, update, delete) are
pushed into Kafka and are binary encoded. Every message has the following 
format:

     -----------------
    | MAGIC | 1 byte  |
    |-----------------|
    | MTYPE | 1 byte  |
    |-----------------|
    | SCMID | N bytes |
    |-----------------|
    | DATA  | N bytes |
     -----------------

The above fields are:

* `MAGIC`: magic byte, used to figure out protocol version
* `MTYPE`: mutation type, a single byte indicating insert (`0x1`), update (`0x2`), or delete (`0x3`)
* `SCMID`: Avro schema ID, variable number of bytes
* `DATA`: the actual mutation data as bytes, variable size

## MySQL to "generic" Kafka topics
If you do not have an Avro schema repository running that contains schemas 
for each of your tables you can use generic Avro encoding. This will take 
binary log mutations (insert, update, or delete) and encode them into the 
following structure in the case of an insert (`InsertMutation.avsc`):

    {
      "namespace": "mypipe.avro",
      "type": "record",
      "name": "InsertMutation",
      "fields": [
    	    {
    			"name": "database",
    			"type": "string"
    		},
    		{
    			"name": "table",
    			"type": "string"
    		},
    		{
    			"name": "tableId",
    			"type": "long"
    		},
    		{
    			"name": "integers",
    			"type": {"type": "map", "values": "int"}
    		},
    		{
    			"name": "strings",
    			"type": {"type": "map", "values": "string"}
    		},
    		{
    			"name": "longs",
    			"type": {"type": "map", "values": "long"}
    		}
    	]
    }

Updates will contain both the old row values and the new ones (see 
`UpdateMutation.avsc`) and deletes are similar to inserts (`DeleteMutation.avsc`). 
Once transformed into Avro data the mutations are pushed into Kafka topics 
based on the following convention (this is configurable):

    topicName = s"$db_$table_generic"

This ensures that all mutations destined to a specific database / table tuple are
all added to a single topic with mutation ordering guarantees.

## MySQL to "specific" Kafka topics
If you are running an Avro schema repository you can encode binary log events based on 
the structures specified in that repository for the incoming database / table streams. 

In order to configure a specific producer you should add a `pipe` using a `mypipe.producer.KafkaMutationSpecificAvroProducer` 
as it's producer. The producer needs some configuration values in order to find the Kafka brokers, 
ZooKeeper ensemble, and the Avro schema repository. Here is a sample configuration:

    kafka-specific {
	  enabled = true
	  consumers = ["localhost"]
	  producer {
	    kafka-specific {
	      schema-repo-client = "mypipe.avro.schema.SchemaRepo"
          metadata-brokers = "localhost:9092"
          zk-connect = "localhost:2181"
	    }
	  }
	}

Note that if you use `mypipe.avro.schema.SchemaRepo` as the schema repository client, you have to 
provide the running JVM with the system property `avro.repo.server-url` in order for the client to 
know where to reach the repository.

## Configuring Kafka topic names
mypipe will push events into Kafka based on the `topic-format` configuration value. `reference.conf` 
has the default values under `mypipe.kafka`, `specific-producer` and `generic-producer`.

    specific-producer {
      topic-format = "${db}_${table}_specific"
    }
     
    generic-producer {
      topic-format = "${db}_${table}_generic"
    }

## `ALTER` queries and "generic" Kafka topics
mypipe handles `ALTER` table queries (as described below) allowing it to add 
new columns or stop including removed ones into "generic" Avro records published 
into Kafka. Since the "generic" Avro beans consist of typed maps (ints, strings, etc.) 
mypipe can easily include or remove columns based on `ALTER` queries. Once a table's 
metadata is refreshed (blocking operation) all subsequent mutations to the 
table will use the new structure and publish that into Kafka.

## Consuming from "generic" Kafka topics
In order to consume from generic Kafka topics the `KafkaGenericMutationAvroConsumer` 
can be used. This consumer will allow you react to insert, update, and delete mutations. 
The consumer needs an Avro schema repository as well as some helpers to be defined. A quick 
(and incomplete) example follows:

    val kafkaConsumer = new KafkaGenericMutationAvroConsumer[Short](
      topic = KafkaUtil.genericTopic("databaseName", "tableName"),
      zkConnect = "localhost:2181",
      groupId = "someGroupId",
      schemaIdSizeInBytes = 2)(
      
      insertCallback = { insertMutation ⇒ ??? }
      updateCallback = { updateMutation ⇒ ??? }
      deleteCallback = { deleteMutation ⇒ ??? } 
    ) {
      protected val schemaRepoClient: GenericSchemaRepository[Short, Schema] = GenericInMemorySchemaRepo
    }

For a more complete example take a look at `KafkaGenericSpec.scala`.

Alternatively you can implement your own Kafka consumer given the binary structure 
of the messages as shown above if the `KafkaGenericMutationAvroConsumer` does not 
satisfy your needs.

# MySQL Event Processing Internals
mypipe uses the [mysql-binlog-connector-java](https://github.com/shyiko/mysql-binlog-connector-java) to tap 
into the MySQL server's binary log stream and handles several types of events.

## `TABLE_MAP`
This event causes mypipe to look up a table's metadata (primary key, column names and types, etc.).
This is done by issuing the following query to the MySQL server to determine column information:

    select COLUMN_NAME, DATA_TYPE, COLUMN_KEY 
    from COLUMNS 
    where TABLE_SCHEMA="$db" and TABLE_NAME = "$table" 
    order by ORDINAL_POSITION

The following query is issued to the server also to determine the primary key:

    select COLUMN_NAME
    from KEY_COLUMN_USAGE 
    where TABLE_SCHEMA='${db}' and TABLE_NAME='${table}' and CONSTRAINT_NAME='PRIMARY' 
    order by ORDINAL_POSITION

While a `TABLE_MAP` event is being handled no other events will be handled concurrently.

## `QUERY`
mypipe handles a few different types of raw queries (besides mutations) like:

### `BEGIN`
If transaction event grouping is enabled mypipe will queue up all events that
arrive after a `BEGIN` query has been encountered. While queuing is occuring 
mypipe will not save it's binary log position as it receives events and will 
only do so once the transaction is committed.

### `COMMIT`
If transaction event grouping is enabled mypipe will hold wait for a `COMMIT` 
query to arrive before "flushing" all queued events (mutations) at and then 
saves it's binary log position to mark the processing of the entire transaction.

### `ROLLBACK`
If transaction event grouping is enabled mypipe will clear and not flush the 
queued up events (mutations) upon receiving a `ROLBACK`.

### `ALTER`
Upon receiving an `ALTER` query mypipe will attempt to reload the affected table's 
metadata. This allows mypipe to detect dropped / added columns, changed keys, or 
anything else that could affect the table. mypipe will perform a look up similar 
to the one done when handling a `TABLE_MAP` event.

# Getting Started
This section aims to guide you through setting up MySQL so that it can be used
with mypipe. It then goes into setting up mypipe itself to push binary log events
into Kafka. Finally, it explains how to consume these events from Kafka.

## Enabling MySQL binary logging

The following snippet of configuration will enable MySQL to generate binary logs 
the mypipe will be able to understand. Taken from `my.cnf`:

    server-id         = 112233
    log_bin           = mysql-bin
    expire_logs_days  = 1
    binlog_format     = row

The binary log format is required to be set to `row` for mypipe to work since 
we need to track individual changes to rows (insert, update, delete).

## Configuring mypipe to ingest MySQL binary logs

Currently, mypipe does not offset a binary distribution that can be installed.
In order use mypipe, start by cloning the git repository:

    git clone https://github.com/mardambey/mypipe.git

Compile and package the code:

    ./sbt package

mypipe needs some configuration in order to point it to a MySQL server.
The following configuration goes into `application.conf` in the `mypipe-runner`
sub-project. Some other configuration entries can be added to other files
as well; when indicated.

A MySQL server can be defined in the configuration under the `consumers` 
section. For example:

    # consumers represent sources for mysql binary logs
    consumers {
  
    database1  {
        # database "host:port:user:pass" 
        source = "localhost:3306:mypipe:mypipe"
      }
    }

Multiple consumers (or sources of data) can be defined. Once mypipe
latches onto a MySQL server, it will attempt to pick up where it last
left of if an offset was previously saved for the given database host
and database name combination. mypipe saves it's offsets in files for
now. These files are stored in the location indicated by the config
entry `data-dir` in the `mypipe-api` project's `reference.conf`.

    data-dir = "/tmp/mypipe"

You can either override that config in `application.conf` of `mypipe-runner`
or edit it in `mypipe-api` directly.

The simplest way to observe the replication stream that mypipe is 
ingesting is to configure the `stdout` producer. This producer will
simply print to standard out the mutation events that mypipe is 
consuming.

The following snippet goes into the configuration file in order to
do so:

    producers {
      stdout {
         class = "mypipe.producer.stdout.StdoutProducer"
      }
    }

In order to instruct mypipe to connect the consumer called `database1`
and the `stdout` producer they must be joined by a `pipe`.

The following configuration entry creates such a pipe:

    pipes {
  
      # prints queries to standard out
      stdout {
        consumers = ["database1"]
        producer {
          stdout {}
        }
      }
    }

At this point, this configuration can be tested out. The
`mypipe-runner` project can run mypipe and use these configuration
entries.

    ./sbt "project runner" "runMain mypipe.runner.PipeRunner"

As soon as mypipe starts, it will latch onto the binary log
stream and the `stdout` producer will print out mutations to the
console.

## Configuring mypipe to produce events into Kafka
mypipe can produce mutations into Kafka either generically or more
specifically. See the above sections for more explanation on what
this means.

### Generic Kafka topics
In order to set up mypipe to produce events to Kafka generically, a
producer must be added to the `producers` section of the configuration.
This is similar to the `stdout` producer added earlier.

    producers {
        kafka-generic {
            class = "mypipe.producer.KafkaMutationGenericAvroProducer"
        }
    }

This producer must then be joined with a data consumer using a pipe.
The configuration of the Kafka brokers will also be passed to the
producer.

    pipes {
        kafka-generic {
            enabled = true
            consumers = ["database1"]
            producer {
              kafka-generic {
                 metadata-brokers = "localhost:9092"
              }
            }
        }
    }

This pipe connects `database1` with the Kafka cluster defined by
the values in the `meta-brokers` key, brokers must be comma separated.
As mentioned before, once this pipe is active it will produce events
into Kafka topics based on the database and table the mutations are
coming from. Topics are named as such:

    $database_$table_generic

Each of these topics contain ordered mutations for the database / table
tuple.

For a more complete configuration file, take a look at the sample
configuration shown later in this document.

### Consuming mutations pushed into Kafka via console (generically)
In order to generically consume and display the mutations pushed into
Kafka, the provided generic console consumer will help. It can be
used by invoking the following command:

    ./sbt "project runner" \
    "runMain mypipe.runner.KafkaGenericConsoleConsumer $database_$table_generic zkHost:2181 groupId"

This will consume messages for the given `$database` / `$table` tuple from
the Kafka cluster specified by the given ZooKeeper ensemble connection
string and the consumer group ID to use.

# Error Handling
If the default configuration based error handler is used:

    mypipe.error-handler { default { class = "mypipe.mysql.ConfigBasedErrorHandler" } }

then user can decide to abort and stop processing using the following flags:

* `quit-on-event-handler-failure`: an event handler failed (commit)
* `quit-on-event-decode-failure`: mypipe could not decode the event
* `quit-on-listener-failure`: a specified listener could not process the event
* `quit-on-empty-mutation-commit-failure`: quit upon encountering an empty transaction

Errors are handled as such:

* The first handler deals with event decoding errors (ie: mypipe can not determine the event type and decode it).
* The second layer of error handlers deals with specific event errors, for example: mutation, alter, table map, commit.
* The third and final layer is the global error handler.

Error handler invocation:

* If the first layer or second layer are invoked and they return true, the next event will be consumed and the global error handler is not called.
* If the first layer or second layer are invoked and they return false, then the third layer (global error handler) is invoked, otherwise, processing of the next event continues

A custom error handler can also be provided if available in the classpath.

# Event Filtering
If not all events are to be processed, they can be filtered by setting `include-event-condition`.
This allows for controlling what dbs and tables will be consumed. Effectively, this is treated as Scala code and is compiled at runtime. Setting this value to blank ignores it.
Example:

    include-event-condition = """ db == "mypipe" && table =="user" """

The above will only process events originating from the database named "mypipe" and the table named "user".

# Tests
In order to run the tests you need to configure `test.conf` with proper MySQL
values. You should also make sure there you have a database called `mypipe` with
the following credentials:

    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mypipe'@'%' IDENTIFIED BY 'mypipe'
    GRANT ALL PRIVILEGES ON `mypipe`.* TO 'mypipe'@'%'

The database must also have binary logging enabled in `row` format.

# Sample application.conf

    mypipe {
    
      # Avro schema repository client class name
      schema-repo-client = "mypipe.avro.schema.SchemaRepo"
    
      # consumers represent sources for mysql binary logs
      consumers {
    
        localhost {
          # database "host:port:user:pass" array
          source = "localhost:3306:mypipe:mypipe"
        }
      }
    
      # data producers export data out (stdout, other stores, external services, etc.)
      producers {
    
        stdout {
          class = "mypipe.producer.stdout.StdoutProducer"
        }
    
        kafka-generic {
          class = "mypipe.producer.KafkaMutationGenericAvroProducer"
        }
      }
    
      # pipes join consumers and producers
      pipes {
    
        stdout {
          consumers = ["localhost"]
          producer {
            stdout {}
          }
          # how to save and load binary log positions
          binlog-position-repo {
            # saved to a file, this is the default if unspecified
            class = "mypipe.api.repo.ConfigurableFileBasedBinaryLogPositionRepository"
            config {
              file-prefix = "stdout-00"     # required if binlog-position-repo is specifiec
              data-dir = "/tmp/mypipe/data" # defaults to mypipe.data-dir if not present
            }
          }
        }
    
        kafka-generic {
          enabled = true
          consumers = ["localhost"]
          producer {
            kafka-generic {
              metadata-brokers = "localhost:9092"
            }
          }
          binlog-position-repo {
            # saves to a MySQL database, make sure you use the following as well to prevent reacting on
            # inserts / updates made in the same DB being listenened on for changes
            # mypipe {
            #   include-event-condition = """ table != "binlogpos" """
            #   error {
            #     quit-on-empty-mutation-commit-failure = false
            #   }
            # }
            class = "mypipe.api.repo.ConfigurableMySQLBasedBinaryLogPositionRepository"
            config {
              # database "host:port:user:pass" array
              source = "localhost:3306:mypipe:mypipe"
              database = "mypipe"
              table = "binlogpos"
              id = "kafka-generic" # used to find the row in the table for this pipe
            }
          }
        }
      }
    }

# mailing list
https://groups.google.com/d/forum/mypipe
