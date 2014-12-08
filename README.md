# mypipe
mypipe latches onto a MySQL server with binary log replication enabled and
allows for the creation of pipes that can consume the replication stream and
act on the data.

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
latter will allow the table's structure to be reflected in the Avro beans.

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
based on the following convention:

    topicName = s"$db_$table_generic"

This ensures that all mutations destined to a specific database / table tuple are
all added to a single topic with mutation ordering guarantees.

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
    
      database1  {
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
    
        # prints queries to standard out
        stdout {
          consumers = ["database1"]
          producer {
            stdout {}
          }
        }
    
        # push events into kafka topics
        # where each database-table tuple
        # get their own topic
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
    }

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

## Binary log offset handling

