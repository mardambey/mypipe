# mypipe

mypipe latches onto a MySQL server with binary log replication enabled and
allows for the creation of pipes that can consume the replication stream and
act on the data.

mypipe tries to provide enough information that usually is not part of the
MySQL binary log stream so that the data is meaningful. mypipe requires a
row based binary log format and provides `Insert`, `Update`, and `Delete`
mutations representing changed rows. Each change is related back to it's
table and the API provides metadata like column types, primary key
information (composite, key order), and other such useful information.

## Producers

Producers receive MySQL binary log events and act on them. They can funnel
down to another data store, send them to some service, or just print them
out to the screen in the case of the stdout producer.

Producers can also make use of mappings. Mappings will map the data from one
form to another (providing minimal data transformations) before the producer 
flushes the data out.

## Pipes

Pipes tie one or more MySQL binary log consumers to a producer. Pipes can be
used to create a system of fan-in data flow from several MySQL servers to
other data sources such as Hadoop, Cassandra, Kafka, or other MySQL servers.
They can also be used to update or flush caches.

## Usage

Look at `mypipe-runner/src/main/resources/application.conf` for a quick example
on how to configure mypipe to slurp a binlog from a MySQL server then uses the
stdout producer to display it to standard out and the Cassandra producer with
some mappings (found in `mypipe-samples/src/main/scala/mypipe/samples/mappings/CassandraMappings.scala`)
to transform the MySQL row data into Cassandra batch mutations affecting several
column families.

## Tests

In order to run the tests you need to configure `test.conf` with proper MySQL
values. You should also make sure there you have a database called `mypipe` with
the following credentials:

    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mypipe'@'%' IDENTIFIED BY 'mypipe'
    GRANT ALL PRIVILEGES ON `mypipe`.* TO 'mypipe'@'%'

The database must also have binary logging enabled in `row` format.
