# mypipe

mypipe latches onto a MySQL server with binary log replication enabled and
allows for the creation of pipes that can consume the replication stream and
act on the data.

# kafka integration
mypipe's main goal is to replicate a MySQL binlog stream into Apache Kafka.
mypipe supports Avro encoding and can use a schema repository to figure out
how to encode data. Data can either be encoded generically and stored in
Avro maps by type (integers, strings, longs, etc.) or it can encode data
more specifically if the schema repository can return specific schemas. The
latter will allow the table's structure to be reflected in the Avro beans.

# api
mypipe tries to provide enough information that usually is not part of the
MySQL binary log stream so that the data is meaningful. mypipe requires a
row based binary log format and provides `Insert`, `Update`, and `Delete`
mutations representing changed rows. Each change is related back to it's
table and the API provides metadata like column types, primary key
information (composite, key order), and other such useful information.

## producers

Producers receive MySQL binary log events and act on them. They can funnel
down to another data store, send them to some service, or just print them
out to the screen in the case of the stdout producer.

Producers can also make use of mappings. Mappings will map the data from one
form to another (providing minimal data transformations) before the producer 
flushes the data out.

## pipes

Pipes tie one or more MySQL binary log consumers to a producer. Pipes can be
used to create a system of fan-in data flow from several MySQL servers to
other data sources such as Hadoop, Cassandra, Kafka, or other MySQL servers.
They can also be used to update or flush caches.

## usage

Look at `mypipe-runner/src/main/resources/application.conf` for a quick example
on how to configure mypipe to slurp a binlog from a MySQL server then uses the
stdout producer to display it to standard out and the Cassandra producer with
some mappings (found in `mypipe-samples/src/main/scala/mypipe/samples/mappings/CassandraMappings.scala`)
to transform the MySQL row data into Cassandra batch mutations affecting several
column families.

## tests

In order to run the tests you need to configure `test.conf` with proper MySQL
values. You should also make sure there you have a database called `mypipe` with
the following credentials:

    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mypipe'@'%' IDENTIFIED BY 'mypipe'
    GRANT ALL PRIVILEGES ON `mypipe`.* TO 'mypipe'@'%'

The database must also have binary logging enabled in `row` format.

## sample application.conf

    mypipe {
    
      # consumers represent sources for mysql binary logs
      consumers {
    
        consumer1 {
          # database "host:port:user:pass" array
          source = "localhost:3306:root:root"
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
          consumers = ["consumer1"]
          producer {
            stdout {}
          }
        }
    
      	kafka-generic {
      		enabled = true
      	  consumers = ["consumer1"]
      	  producer {
      	    kafka-generic {
      	      metadata-brokers = "localhost:9092"
      	    }
      	  }
      	 }
      }
    }
    

